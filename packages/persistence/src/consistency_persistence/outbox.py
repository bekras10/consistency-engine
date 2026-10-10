"""Commit-safe read of ``notification_outbox``.

``id`` is taken from ``notification_outbox_id_seq`` before the detection
transaction commits, so assignment order is not commit order and a rollback
leaves a hole. Readers do not look at ``pg_locks``. Lock state is live and can
change after the snapshot: a transaction can commit in that gap, and a reader
that treats "no lock" as "aborted" skips an id that actually committed.

Protocol:

1. The detection transaction reads ``pg_current_xact_id()``.
2. A second connection, holding ``pg_advisory_xact_lock``, takes ``nextval``
   and inserts ``outbox_claims(id, xid)``, then commits. The lock is released
   only after that commit, so the next id is not reserved until this claim is
   durable. The detection transaction then inserts the outbox row with that id.
3. A reader loads the outbox rows visible to its snapshot, then asks
   ``pg_xact_status`` for the xid of each missing id. Status is terminal or
   still open; it cannot flip from aborted back to committed.

Delivery:

- A visible id is delivered.
- ``aborted`` is skipped. A missing id with no claim, when a later claim
  exists, is skipped too: the reservation rolled back after ``nextval`` and
  the row was never inserted. ``nextval`` does not roll back; the claim
  transaction is the only place it runs for protocol inserts.
- ``in progress`` stops the cursor. A higher committed id waits.
- ``committed`` while the row is absent from this snapshot stops the cursor.
  The id committed after the snapshot. It is not an aborted hole. The next
  read sees the row. This is the snapshot/status race.
- ``unknown`` (no claim, and no later claim to prove the id was abandoned)
  also stops the cursor. An unrelated transaction does not insert a claim, so
  it does not stall a contiguous committed prefix.

``created_at`` is the insert time, not the commit time, and is not a cursor.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

# Serializes id reservation only. Released when the claim transaction commits,
# before the detection transaction commits, so two detections can still commit
# out of order. It does not stay held for the life of the detection write.
_ALLOC_LOCK = 884422331

HoleState = Literal["aborted", "in progress", "committed", "unknown"]
BeforeStatus = Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class OutboxNote:
    id: int
    topic: str
    session_id: str | None
    payload: dict[str, object]


def publication_prefix(
    visible: Sequence[int], after_id: int, holes: Mapping[int, str]
) -> tuple[bool, list[int]]:
    """Ids strictly above ``after_id`` that are safe to deliver, in id order.

    ``holes`` describes ids missing from ``visible``. Only ``aborted`` may be
    skipped. ``committed`` means the assigning transaction has committed but
    the row is not in this snapshot: stop, do not skip. ``in progress`` and
    ``unknown`` stop as well. The boolean is true when a hole blocked the cursor.
    """
    chosen: list[int] = []
    remaining = [item for item in visible if item > after_id]
    index = 0
    expected = after_id + 1
    while True:
        if index < len(remaining) and remaining[index] < expected:
            index += 1
            continue
        if index < len(remaining) and remaining[index] == expected:
            chosen.append(expected)
            expected += 1
            index += 1
            continue
        later = index < len(remaining)
        state = holes.get(expected)
        if state is None and not later:
            return False, chosen
        if state == "aborted":
            expected += 1
            continue
        return True, chosen


def _payload(value: object) -> dict[str, object]:
    if isinstance(value, str):
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise TypeError("outbox payload must be an object")
        return {str(key): item for key, item in parsed.items()}
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    raise TypeError("outbox payload must be an object")


def _gap_ids(visible: Sequence[int], after_id: int, highest_claim: int) -> list[int]:
    gaps: list[int] = []
    expected = after_id + 1
    upper = highest_claim
    for item in visible:
        if item <= after_id:
            continue
        if item > upper:
            upper = item
        while expected < item:
            gaps.append(expected)
            expected += 1
        expected = item + 1
    while expected <= highest_claim:
        gaps.append(expected)
        expected += 1
    return gaps


def _hole_state(status: object) -> HoleState:
    if status == "aborted":
        return "aborted"
    if status == "in progress":
        return "in progress"
    if status == "committed":
        return "committed"
    return "unknown"


async def _reserve_on(holder: AsyncConnection | AsyncSession, engine: AsyncEngine) -> int:
    """Commit ``(id, xid)`` before the caller's outbox insert.

    ``holder`` is the detection transaction. Its xid is stored on the claim.
    The claim commits on ``engine`` before this function returns, while the
    caller's transaction stays open.
    """
    xid = await holder.scalar(text("SELECT pg_current_xact_id()::text"))
    if not isinstance(xid, str) or not xid:
        raise TypeError("pg_current_xact_id did not return a transaction id")
    async with engine.connect() as claim:
        await claim.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": _ALLOC_LOCK})
        allocated = await claim.scalar(text("SELECT nextval('notification_outbox_id_seq')"))
        if not isinstance(allocated, int):
            raise TypeError("outbox id sequence did not return an integer")
        await claim.execute(
            text("INSERT INTO outbox_claims (id, xid) VALUES (:id, :xid)"),
            {"id": allocated, "xid": xid},
        )
        await claim.commit()
    return allocated


async def reserve_outbox_id(session: AsyncSession) -> int:
    """Reserve an outbox id for a row the caller will insert on ``session``."""
    bind = session.bind
    if not isinstance(bind, AsyncEngine):
        raise RuntimeError("outbox id reservation requires an async engine")
    connection = await session.connection()
    return await _reserve_on(connection, bind)


async def stage_notification(
    connection: AsyncConnection,
    engine: AsyncEngine,
    payload: str,
    *,
    topic: str = "detection",
    session_id: str | None = None,
) -> int:
    """Insert one outbox row on an open ``connection`` after its claim commits."""
    new_id = await _reserve_on(connection, engine)
    await connection.execute(
        text(
            """
            INSERT INTO notification_outbox (id, topic, session_id, payload, created_at)
            VALUES (
                :id, :topic, :session_id, CAST(:payload AS jsonb), CURRENT_TIMESTAMP
            )
            """
        ),
        {"id": new_id, "topic": topic, "session_id": session_id, "payload": payload},
    )
    return new_id


async def _visible_ids(session: AsyncSession, after_id: int, limit: int | None) -> list[int]:
    sql = "SELECT id FROM notification_outbox WHERE id > :after_id ORDER BY id"
    params: dict[str, int] = {"after_id": after_id}
    if limit is not None:
        sql += " LIMIT :limit"
        params["limit"] = limit
    result = await session.execute(text(sql), params)
    return [int(item) for item in result.scalars()]


async def _hole_states(
    session: AsyncSession,
    after_id: int,
    visible: Sequence[int],
    before_status: BeforeStatus | None,
) -> dict[int, HoleState]:
    claim_rows = (
        await session.execute(
            text("SELECT id, xid::text FROM outbox_claims WHERE id > :after_id ORDER BY id"),
            {"after_id": after_id},
        )
    ).all()
    claims = {int(row[0]): str(row[1]) for row in claim_rows}
    if before_status is not None:
        await before_status()
    highest_claim = max(claims) if claims else after_id
    gaps = _gap_ids(visible, after_id, highest_claim)
    live: dict[int, object] = {}
    if claims:
        status_rows = (
            await session.execute(
                text(
                    """
                    SELECT id, pg_xact_status(CAST(xid AS xid8))
                    FROM outbox_claims
                    WHERE id > :after_id
                    """
                ),
                {"after_id": after_id},
            )
        ).all()
        live = {int(row[0]): row[1] for row in status_rows}
    holes: dict[int, HoleState] = {}
    for gap in gaps:
        if gap in claims:
            holes[gap] = _hole_state(live.get(gap))
        elif highest_claim > gap:
            holes[gap] = "aborted"
        else:
            holes[gap] = "unknown"
    return holes


async def _load_notes(session: AsyncSession, ids: Sequence[int]) -> list[OutboxNote]:
    if not ids:
        return []
    result = await session.execute(
        text(
            """
            SELECT id, topic, session_id, payload
            FROM notification_outbox
            WHERE id > :after_id AND id <= :last_id
            ORDER BY id
            """
        ),
        {"after_id": ids[0] - 1, "last_id": ids[-1]},
    )
    wanted = set(ids)
    notes: list[OutboxNote] = []
    for row in result:
        raw_id = row[0]
        if not isinstance(raw_id, int) or raw_id not in wanted:
            continue
        session_id = row[2]
        notes.append(
            OutboxNote(
                id=raw_id,
                topic=str(row[1]),
                session_id=None if session_id is None else str(session_id),
                payload=_payload(row[3]),
            )
        )
    return notes


async def highest_outbox_id(session: AsyncSession) -> int:
    """Largest reserved or committed outbox id, or 0 when the log is empty."""
    raw = await session.scalar(
        text(
            """
            SELECT GREATEST(
                COALESCE((SELECT MAX(id) FROM notification_outbox), 0),
                COALESCE((SELECT MAX(id) FROM outbox_claims), 0)
            )
            """
        )
    )
    if not isinstance(raw, int):
        raise TypeError("outbox high-water mark was not an integer")
    return raw


async def contiguous_watermark(session: AsyncSession, after_id: int) -> int:
    """Highest id that can be acknowledged without skipping a live gap.

    The watermark and the caller's detection read share the session snapshot.
    This does not load payloads and does not stop at a fixed row count.
    """
    visible = await _visible_ids(session, after_id, None)
    holes = await _hole_states(session, after_id, visible, None)
    _blocked, chosen = publication_prefix(visible, after_id, holes)
    if not chosen:
        return after_id
    return chosen[-1]


async def committed_notifications(
    session: AsyncSession,
    after_id: int,
    *,
    limit: int = 100,
    before_status: BeforeStatus | None = None,
) -> tuple[bool, list[OutboxNote]]:
    """Return ``(blocked, notes)`` for the committed prefix above ``after_id``.

    ``before_status`` runs after the outbox snapshot is taken and before
    ``pg_xact_status`` is read. Tests use it to commit a writer in that gap.
    Production passes none. The row snapshot is not read again afterwards, so
    a commit during the gap cannot be mistaken for an abort, and it also
    cannot appear inside this snapshot.
    """
    if limit < 1:
        return False, []
    visible = await _visible_ids(session, after_id, limit)
    holes = await _hole_states(session, after_id, visible, before_status)
    blocked, chosen = publication_prefix(visible, after_id, holes)
    notes = await _load_notes(session, chosen[:limit])
    return blocked, notes

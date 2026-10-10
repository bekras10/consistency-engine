"""Commit-safe read of ``notification_outbox``.

``id`` is assigned by ``nextval`` at INSERT, before COMMIT. A later transaction can
commit a higher id while an earlier id is still open, and a rollback leaves a
permanent hole. Readers therefore do not treat the highest visible id as a cursor.

``select_contiguous`` walks committed ids upward from the cursor:

- Take the next id when it is exactly ``cursor + 1``.
- If an id is missing and a transaction still holds a write lock on
  ``notification_outbox``, stop. That transaction can still commit the missing
  id. Hold every higher committed id until the lock clears.
- If an id is missing and no such lock is held, the hole's transaction aborted
  (or never inserted). Skip it. An open transaction that has not locked this
  table cannot be given an id that was already consumed, so it must not stall
  the tail.

Outbox ids are allocated by ``INSERT`` into ``notification_outbox``. That
statement takes ``RowExclusiveLock`` before ``nextval``'s value is visible to
other sessions, and the lock lasts until commit or abort. A transaction that
has not taken it cannot fill a gap behind a higher committed id.

``created_at`` is the insert time, not the commit time, and is not a cursor.
The ``detections`` table remains the resynchronization snapshot when the tail
is uncertain. See ``docs/architecture.md``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True)
class OutboxNote:
    id: int
    topic: str
    session_id: str | None
    payload: dict[str, object]


def select_contiguous(ids: list[int], after_id: int, writers_in_progress: bool) -> list[int]:
    """Committed ids strictly above ``after_id``, already sorted ascending."""
    chosen: list[int] = []
    expected = after_id + 1
    for item in ids:
        if item == expected:
            chosen.append(item)
            expected = item + 1
            continue
        if item < expected:
            continue
        if writers_in_progress:
            break
        chosen.append(item)
        expected = item + 1
    return chosen


def _payload(value: object) -> dict[str, object]:
    if isinstance(value, str):
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise TypeError("outbox payload must be an object")
        return {str(key): item for key, item in parsed.items()}
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    raise TypeError("outbox payload must be an object")


_WRITER_LOCKS = """
    'RowExclusiveLock',
    'ShareRowExclusiveLock',
    'ExclusiveLock',
    'AccessExclusiveLock'
"""


def _writer_flag(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return isinstance(value, int) and value > 0


async def outbox_writers_in_progress(session: AsyncSession) -> bool:
    """True when some other backend holds a write lock on ``notification_outbox``."""
    result = await session.execute(
        text(
            f"""
            SELECT EXISTS (
                SELECT 1
                FROM pg_locks AS held
                WHERE held.relation = 'notification_outbox'::regclass
                  AND held.locktype = 'relation'
                  AND held.granted
                  AND held.mode IN ({_WRITER_LOCKS})
                  AND held.pid IS DISTINCT FROM pg_backend_pid()
            )
            """
        )
    )
    return _writer_flag(result.scalar_one())


async def contiguous_watermark(session: AsyncSession, after_id: int) -> int:
    """Highest committed id that can be acknowledged without skipping a live gap.

    The watermark and the caller’s detection read must share one snapshot
    (repeatable read). This does not load payloads and does not stop at a
    fixed row count.
    """
    result = await session.execute(
        text(
            f"""
            WITH writers AS (
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks AS held
                    WHERE held.relation = 'notification_outbox'::regclass
                      AND held.locktype = 'relation'
                      AND held.granted
                      AND held.mode IN ({_WRITER_LOCKS})
                      AND held.pid IS DISTINCT FROM pg_backend_pid()
                ) AS busy
            ),
            ordered AS (
                SELECT id, lag(id) OVER (ORDER BY id) AS prev
                FROM notification_outbox
                WHERE id > :after_id
            ),
            gap AS (
                SELECT prev
                FROM ordered
                WHERE id > COALESCE(prev, :after_id) + 1
                ORDER BY id
                LIMIT 1
            ),
            tail AS (
                SELECT COALESCE(MAX(id), :after_id) AS max_id
                FROM notification_outbox
                WHERE id > :after_id
            )
            SELECT CASE
                WHEN writers.busy AND EXISTS (SELECT 1 FROM gap)
                    THEN COALESCE((SELECT prev FROM gap), :after_id)
                ELSE (SELECT max_id FROM tail)
            END
            FROM writers
            """
        ),
        {"after_id": after_id},
    )
    raw = result.scalar_one()
    if not isinstance(raw, int):
        raise TypeError("outbox watermark was not an integer")
    return raw


async def committed_notifications(
    session: AsyncSession, after_id: int, *, limit: int = 100
) -> tuple[bool, list[OutboxNote]]:
    """Return ``(outbox_writers_in_progress, contiguous committed notes)``.

    The lock check and the visible rows come from one statement so they share
    a snapshot. A gap is held only while another transaction holds a write lock
    on ``notification_outbox``.
    """
    if limit < 1:
        return False, []
    result = await session.execute(
        text(
            f"""
            SELECT writers.busy AS writers, note.id, note.topic, note.session_id, note.payload
            FROM (
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks AS held
                    WHERE held.relation = 'notification_outbox'::regclass
                      AND held.locktype = 'relation'
                      AND held.granted
                      AND held.mode IN ({_WRITER_LOCKS})
                      AND held.pid IS DISTINCT FROM pg_backend_pid()
                ) AS busy
            ) AS writers
            LEFT JOIN LATERAL (
                SELECT id, topic, session_id, payload
                FROM notification_outbox
                WHERE id > :after_id
                ORDER BY id
                LIMIT :limit
            ) AS note ON true
            ORDER BY note.id NULLS FIRST
            """
        ),
        {"after_id": after_id, "limit": limit},
    )
    rows = result.all()
    if not rows:
        return False, []
    writers = _writer_flag(cast(object, rows[0][0]))
    visible: list[OutboxNote] = []
    for row in rows:
        raw_id = cast(object, row[1])
        if not isinstance(raw_id, int):
            continue
        session_id = row[3]
        visible.append(
            OutboxNote(
                id=raw_id,
                topic=str(row[2]),
                session_id=None if session_id is None else str(session_id),
                payload=_payload(row[4]),
            )
        )
    allowed = set(select_contiguous([note.id for note in visible], after_id, writers))
    return writers, [note for note in visible if note.id in allowed]

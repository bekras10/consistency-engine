"""Commit-safe read of ``notification_outbox``.

``id`` is assigned by ``nextval`` at INSERT, before COMMIT. A later transaction can
commit a higher id while an earlier id is still open, and a rollback leaves a
permanent hole. Readers therefore do not treat the highest visible id as a cursor.

``select_contiguous`` walks committed ids upward from the cursor:

- Take the next id when it is exactly ``cursor + 1``.
- If an id is missing and any other transaction is still in progress, stop. Hold
  every higher committed id until that gap commits or the in-progress work ends.
- If an id is missing and no other transaction is in progress, the hole's
  transaction aborted. Skip it and continue.

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


async def committed_notifications(
    session: AsyncSession, after_id: int, *, limit: int = 100
) -> tuple[bool, list[OutboxNote]]:
    """Return ``(writers_in_progress, contiguous committed notes)``.

    The writer count and the visible rows come from one statement so they share
    a snapshot. A gap is held while that snapshot still shows an in-progress
    transaction (``pg_snapshot_xip``).
    """
    if limit < 1:
        return False, []
    result = await session.execute(
        text(
            """
            SELECT writers.n AS writers, note.id, note.topic, note.session_id, note.payload
            FROM (
                SELECT count(*)::int AS n
                FROM pg_snapshot_xip(pg_current_snapshot())
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
    raw_writers = cast(object, rows[0][0])
    writers = isinstance(raw_writers, int) and raw_writers > 0
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

"""Retention for high-frequency book rows.

Synthetic and replay sessions keep their journal until they age out or the session cap is
hit, **except** pinned demo sessions (the bundled inconsistent dataset). Those stay complete
so a demo can be reconstructed exactly. Third-party sources persist no raw rows unless
``THIRD_PARTY_RAW_PERSISTENCE_AUTHORIZED`` is set; any raw rows that were authorized are
removed once they are older than ``RETENTION_THIRD_PARTY_HOURS`` (0 removes them on the next
run). Detections, relationships, and reference data are not deleted by this job.

The same retention pass also prunes ``notification_outbox`` and ``outbox_claims``.
``outbox_keep`` (``OUTBOX_RETENTION_KEEP``, default 10000) is how many of the newest
commit-safe ids stay. The consumer watermark is the contiguous outbox watermark.
Ids at or below ``watermark - outbox_keep`` may be deleted, and never an id above
that watermark. A ``Last-Event-ID`` still inside the retained suffix is replayed.
A cursor at or below the stored floor is an SSE full resync, not a skip. Detection
rows stay, so that resync still has the current snapshot. 10000 is larger than the
bundled inconsistent session's detection-event count, so a normal synthetic run
does not prune a live cursor, and the table cannot grow without a bound.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from consistency_persistence.schema import (
    OrderbookSnapshotRow,
    OrderbookUpdateRow,
    SessionCheckpointRow,
)


@dataclass(frozen=True)
class RetentionPolicy:
    max_age_hours: int
    max_sessions: int
    third_party_hours: int
    outbox_keep: int = 10_000


@dataclass(frozen=True)
class SessionRetentionView:
    session_id: str
    started_at: datetime
    source_kind: str
    pinned: bool
    raw_persisted: bool


def sessions_losing_raw_data(
    sessions: list[SessionRetentionView], *, now: datetime, policy: RetentionPolicy
) -> list[str]:
    """Session ids whose snapshots, updates, and checkpoints should be deleted."""
    drop: set[str] = set()
    within_age: list[SessionRetentionView] = []
    for view in sessions:
        if not view.raw_persisted:
            continue
        age = now - view.started_at
        if view.source_kind == "kalshi_authorized":
            if age >= timedelta(hours=policy.third_party_hours):
                drop.add(view.session_id)
            continue
        if view.pinned:
            continue
        if policy.max_age_hours <= 0 or age >= timedelta(hours=policy.max_age_hours):
            drop.add(view.session_id)
            continue
        within_age.append(view)
    within_age.sort(key=lambda v: (v.started_at, v.session_id))
    overflow = len(within_age) - policy.max_sessions
    if policy.max_sessions >= 0 and overflow > 0:
        drop.update(v.session_id for v in within_age[:overflow])
    return sorted(drop)


async def delete_raw_sessions(session: AsyncSession, session_ids: list[str]) -> int:
    if not session_ids:
        return 0
    for model in (OrderbookSnapshotRow, OrderbookUpdateRow, SessionCheckpointRow):
        await session.execute(delete(model).where(model.session_id.in_(session_ids)))
    return len(session_ids)

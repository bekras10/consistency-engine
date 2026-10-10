"""A commit between the snapshot and the lock check must not be treated as an abort.

The reader takes a repeatable-read snapshot that does not contain an in-flight
outbox row. That transaction then commits, releasing its lock, before the
reader looks at lock state. The id was committed. It is not an aborted hole.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.outbox import committed_notifications, stage_notification

pytestmark = pytest.mark.integration

_INSERT = text(
    """
    INSERT INTO notification_outbox (topic, session_id, payload, created_at)
    VALUES ('detection', NULL, CAST(:payload AS jsonb), CURRENT_TIMESTAMP)
    RETURNING id
    """
)


async def test_commit_between_snapshot_and_lock_check_is_not_skipped(db: str) -> None:
    """Snapshot misses the row; the writer commits before the status check.

    Delivering the higher id without the lower one is the skip. Holding both
    until the next read, or returning them together, is acceptable. Returning
    only the higher id is not.
    """
    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    low = await engine.connect()
    high = await engine.connect()
    try:
        await low.begin()
        low_id = int((await low.execute(_INSERT, {"payload": '{"side":"low"}'})).scalar_one())
        await high.begin()
        high_id = int((await high.execute(_INSERT, {"payload": '{"side":"high"}'})).scalar_one())
        await high.commit()
        assert low_id < high_id

        async with maker() as session:
            await session.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            visible = (
                (await session.execute(text("SELECT id FROM notification_outbox ORDER BY id")))
                .scalars()
                .all()
            )
            assert list(visible) == [high_id]
            await low.commit()
            _writers, notes = await committed_notifications(session, 0)

        delivered = [note.id for note in notes]
        assert delivered != [high_id]
        assert low_id in delivered or delivered == []

        async with maker() as session:
            _writers, follow = await committed_notifications(session, 0)
        assert [note.id for note in follow] == [low_id, high_id]
        assert [note.payload["side"] for note in follow] == ["low", "high"]
    finally:
        await low.close()
        await high.close()
        await engine.dispose()


async def test_committed_claim_is_not_skipped_when_the_row_misses_the_snapshot(db: str) -> None:
    """Claim is visible, the row is not, then the writer commits before status is read."""
    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    low = await engine.connect()
    high = await engine.connect()
    try:
        await low.begin()
        low_id = await stage_notification(low, engine, '{"side":"low"}')
        await high.begin()
        high_id = await stage_notification(high, engine, '{"side":"high"}')
        await high.commit()
        assert low_id < high_id

        async def commit_low() -> None:
            await low.commit()

        async with maker() as session:
            _writers, notes = await committed_notifications(session, 0, before_status=commit_low)
        delivered = [note.id for note in notes]
        assert delivered != [high_id]
        assert low_id in delivered or delivered == []

        async with maker() as session:
            _writers, follow = await committed_notifications(session, 0)
        assert [note.id for note in follow] == [low_id, high_id]
        assert [note.payload["side"] for note in follow] == ["low", "high"]
    finally:
        await low.close()
        await high.close()
        await engine.dispose()

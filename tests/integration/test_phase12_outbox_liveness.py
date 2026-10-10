"""Outbox delivery must not stall on transactions that cannot fill an id gap."""

from __future__ import annotations

import pytest
from sqlalchemy import text

from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.outbox import committed_notifications

pytestmark = pytest.mark.integration

_INSERT = text(
    """
    INSERT INTO notification_outbox (topic, session_id, payload, created_at)
    VALUES ('detection', NULL, CAST(:payload AS jsonb), CURRENT_TIMESTAMP)
    RETURNING id
    """
)


async def test_unrelated_transaction_does_not_stall_committed_notifications(db: str) -> None:
    """Contiguous commits stay visible, an in-flight outbox gap still holds, an abort skips.

    An open transaction that has not inserted into ``notification_outbox`` must not
    freeze a higher committed id. The transaction that owns the missing id must.
    """
    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    unrelated = await engine.connect()
    gap = await engine.connect()
    higher = await engine.connect()
    try:
        await unrelated.begin()
        # A write assigns an xid. A read-only SELECT does not, so it never appears in
        # pg_snapshot_xip. This row is not an outbox insert; it must not freeze the tail.
        await unrelated.execute(
            text(
                """
                INSERT INTO system_health (component, status, checked_at, detail)
                VALUES ('unrelated', 'ok', CURRENT_TIMESTAMP, '{}'::jsonb)
                """
            )
        )

        first = await engine.connect()
        try:
            await first.begin()
            low = int((await first.execute(_INSERT, {"payload": '{"n":1}'})).scalar_one())
            await first.commit()
            await first.begin()
            high = int((await first.execute(_INSERT, {"payload": '{"n":2}'})).scalar_one())
            await first.commit()
        finally:
            await first.close()
        assert high == low + 1
        async with maker() as session:
            writers, notes = await committed_notifications(session, 0)
        assert writers is False
        assert [note.id for note in notes] == [low, high]

        await gap.begin()
        missing = int((await gap.execute(_INSERT, {"payload": '{"n":"gap"}'})).scalar_one())
        await higher.begin()
        held = int((await higher.execute(_INSERT, {"payload": '{"n":"held"}'})).scalar_one())
        await higher.commit()
        assert missing < held
        async with maker() as session:
            writers, blocked = await committed_notifications(session, high)
        assert writers is True
        assert blocked == []

        await gap.rollback()
        async with maker() as session:
            writers, skipped = await committed_notifications(session, high)
        assert writers is False
        assert [note.id for note in skipped] == [held]
        assert skipped[0].payload["n"] == "held"
    finally:
        await unrelated.close()
        await gap.close()
        await higher.close()
        await engine.dispose()

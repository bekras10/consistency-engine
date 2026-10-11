"""Bounded claim reads and retention that does not skip an unconsumed id."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.outbox import (
    CLAIM_PAGE_SIZE,
    _lock_prune,
    committed_notifications,
    prune_consumed_outbox,
    pruned_through,
)

pytestmark = pytest.mark.integration

_HISTORY = CLAIM_PAGE_SIZE + 50


async def _seed(db: str, count: int) -> str:
    engine = make_engine(db)
    try:
        async with engine.begin() as conn:
            xid = await conn.scalar(text("SELECT pg_current_xact_id()::text"))
        if not isinstance(xid, str):
            raise TypeError("expected a transaction id")
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO outbox_claims (id, xid)
                    SELECT g, :xid FROM generate_series(1, :count) AS g
                    """
                ),
                {"xid": xid, "count": count},
            )
            await conn.execute(
                text(
                    """
                    INSERT INTO notification_outbox (id, topic, session_id, payload, created_at)
                    SELECT g, 'detection', NULL, CAST(:payload AS jsonb), CURRENT_TIMESTAMP
                    FROM generate_series(1, :count) AS g
                    """
                ),
                {"count": count, "payload": '{"n":1}'},
            )
            await conn.execute(
                text("SELECT setval('notification_outbox_id_seq', :count)"),
                {"count": count},
            )
        return xid
    finally:
        await engine.dispose()


async def test_large_history_is_readable_from_the_tail(db: str) -> None:
    await _seed(db, _HISTORY)
    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    try:
        async with maker() as session:
            blocked, notes = await committed_notifications(session, _HISTORY - 10, limit=20)
        assert blocked is False
        assert [note.id for note in notes] == list(range(_HISTORY - 9, _HISTORY + 1))
    finally:
        await engine.dispose()


async def test_prune_stops_at_the_watermark_and_does_not_skip(db: str) -> None:
    await _seed(db, _HISTORY)
    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    consumed = _HISTORY - 10
    try:
        async with maker() as session:
            with pytest.raises(ValueError, match="commit-safe watermark"):
                await prune_consumed_outbox(session, _HISTORY + 1)
            pruned = await prune_consumed_outbox(session, consumed)
            await session.commit()
        assert pruned == consumed
        async with maker() as session:
            remaining_claims = await session.scalar(
                text("SELECT COUNT(*) FROM outbox_claims WHERE id <= :consumed"),
                {"consumed": consumed},
            )
            kept = await session.scalar(
                text("SELECT COUNT(*) FROM outbox_claims WHERE id > :consumed"),
                {"consumed": consumed},
            )
            blocked, notes = await committed_notifications(session, 0)
            blocked_tail, tail = await committed_notifications(session, consumed, limit=20)
        assert remaining_claims == 0
        assert kept == 10
        assert blocked is True
        assert notes == []
        assert blocked_tail is False
        assert [note.id for note in tail] == list(range(consumed + 1, _HISTORY + 1))
    finally:
        await engine.dispose()


async def test_overlapping_prunes_keep_a_monotonic_floor(db: str) -> None:
    """Two prunes in flight must not move the stored floor backward.

    The higher watermark holds the retention lock until the lower prune is
    blocked on that same lock, then commits. The lower call re-reads the
    floor and must return it. Ids above the commit-safe watermark stay.
    """
    total = 40
    high = 30
    low = 10
    await _seed(db, total)
    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    high_holds = asyncio.Event()
    low_waiting = asyncio.Event()

    async def prune_to(consumed: int) -> int:
        async with maker() as session, session.begin():
            if consumed == high:
                await _lock_prune(session)
                high_holds.set()
                await low_waiting.wait()
            else:
                await high_holds.wait()
                low_waiting.set()
            return await prune_consumed_outbox(session, consumed)

    try:
        high_floor, low_floor = await asyncio.gather(prune_to(high), prune_to(low))
        async with maker() as session:
            stored = await pruned_through(session)
            kept = await session.scalar(
                text("SELECT COUNT(*) FROM outbox_claims WHERE id > :high"),
                {"high": high},
            )
            removed = await session.scalar(
                text("SELECT COUNT(*) FROM notification_outbox WHERE id <= :high"),
                {"high": high},
            )
            above = await session.scalar(
                text("SELECT COUNT(*) FROM notification_outbox WHERE id > :total"),
                {"total": total},
            )
            max_id = await session.scalar(text("SELECT MAX(id) FROM notification_outbox"))
        assert high_floor == high
        assert low_floor == high
        assert stored == high
        assert kept == total - high
        assert removed == 0
        assert above == 0
        assert max_id == total
    finally:
        await engine.dispose()

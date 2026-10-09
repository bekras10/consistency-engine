"""Outbox rows commit with the detection, and a higher id cannot skip a lower one."""

from __future__ import annotations

import pytest
from sqlalchemy import func, select, text

from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.outbox import committed_notifications
from consistency_persistence.recording import _apply_event
from consistency_persistence.schema import NotificationOutboxRow
from consistency_persistence.store import PersistenceStore
from consistency_pipeline.lifecycle import EventKind
from tests.golden.support import fee_calculator
from tests.integration.test_postgres_pipeline import _messages
from tests.unit.test_detection_pipeline import Harness

pytestmark = pytest.mark.integration

_INSERT = text(
    """
    INSERT INTO notification_outbox (topic, session_id, payload, created_at)
    VALUES ('detection', NULL, CAST(:payload AS jsonb), CURRENT_TIMESTAMP)
    RETURNING id
    """
)


async def test_outbox_row_is_in_the_detection_transaction(db: str) -> None:
    catalog, relationships, _messages_unused = _messages()
    fees = fee_calculator()
    store = PersistenceStore(db)
    opened = await store.open(
        preferred_session_id="ses-outbox",
        fingerprint="sha256:outbox",
        deterministic=True,
        source_label="unit",
        source_kind="synthetic",
        catalog=catalog,
        relationships=relationships,
        fees=fees,
        persist_raw=True,
        pinned=False,
        batch_size=10,
        config_document={"test": "outbox"},
    )
    try:
        harness = Harness()
        harness.open_books()
        harness.heartbeats(20, dt=100)
        event = next(ev for ev in harness.events if ev.kind is EventKind.OPENED)
        event = event.model_copy(
            update={"record": event.record.model_copy(update={"session_id": opened.session_id})}
        )
        opened.sink.fault = RuntimeError("boom")
        with pytest.raises(RuntimeError, match="boom"):
            await opened.sink.write([event])
        async with store.sessions() as session:
            pending = await session.scalar(select(func.count()).select_from(NotificationOutboxRow))
        assert pending == 0

        async def _insert_then_roll_back() -> None:
            async with store.sessions() as session, session.begin():
                await _apply_event(session, event)
                inside = await session.scalar(
                    select(func.count()).select_from(NotificationOutboxRow)
                )
                assert inside == 1
                async with store.sessions() as other:
                    outside = await other.scalar(
                        select(func.count()).select_from(NotificationOutboxRow)
                    )
                assert outside == 0
                raise RuntimeError("rollback")

        with pytest.raises(RuntimeError, match="rollback"):
            await _insert_then_roll_back()
        async with store.sessions() as session:
            after = await session.scalar(select(func.count()).select_from(NotificationOutboxRow))
        assert after == 0

        opened.sink.fault = None
        await opened.sink.write([event])
        async with store.sessions() as session:
            note = (await session.execute(select(NotificationOutboxRow).limit(1))).scalar_one()
        assert note.topic == "detection"
        detection = note.payload["detection"]
        assert isinstance(detection, dict)
        assert detection["detection_id"] == event.detection_id
    finally:
        await store.aclose()


async def test_higher_outbox_id_waits_for_the_lower_commit(db: str) -> None:
    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    lower = await engine.connect()
    higher = await engine.connect()
    try:
        await lower.begin()
        low_id = int((await lower.execute(_INSERT, {"payload": '{"side":"low"}'})).scalar_one())
        await higher.begin()
        high_id = int((await higher.execute(_INSERT, {"payload": '{"side":"high"}'})).scalar_one())
        await higher.commit()
        assert low_id < high_id
        async with maker() as session:
            _writers, held = await committed_notifications(session, 0)
        assert [note.id for note in held] == []
        await lower.commit()
        async with maker() as session:
            _writers, delivered = await committed_notifications(session, 0)
        assert [note.id for note in delivered] == [low_id, high_id]
        assert [note.payload["side"] for note in delivered] == ["low", "high"]
    finally:
        await lower.close()
        await higher.close()
        await engine.dispose()


async def test_aborted_outbox_hole_is_not_a_permanent_stall(db: str) -> None:
    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    abandoned = await engine.connect()
    committed = await engine.connect()
    try:
        await abandoned.begin()
        await abandoned.execute(_INSERT, {"payload": '{"side":"gone"}'})
        await abandoned.rollback()
        await committed.begin()
        kept = int((await committed.execute(_INSERT, {"payload": '{"side":"kept"}'})).scalar_one())
        await committed.commit()
        async with maker() as session:
            writers, notes = await committed_notifications(session, 0)
        assert writers is False
        assert [note.id for note in notes] == [kept]
        assert notes[0].payload["side"] == "kept"
    finally:
        await abandoned.close()
        await committed.close()
        await engine.dispose()

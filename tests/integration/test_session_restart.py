"""A completed synthetic fingerprint is not ingested again. A failed journal still resumes."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import select, update

from consistency_connectors.base import ConnectionState
from consistency_connectors.settings import Settings
from consistency_connectors.sources.recorded import RecordedStreamSource
from consistency_core.models.common import DataSourceKind
from consistency_persistence.schema import IngestionSessionRow
from consistency_worker.bootstrap import SessionInputs, fee_calculator
from consistency_worker.service import WorkerService
from consistency_worker.store import DatabaseSessionStore
from tests.conftest import FIXTURES
from tests.integration.test_postgres_pipeline import _messages

pytestmark = pytest.mark.integration


def _settings(db: str, *, reseed: bool = False) -> Settings:
    return Settings(
        DATABASE_URL=db,
        checkpoint_every=15,
        journal_batch_size=8,
        retention_interval_s=3600,
        RESEED_SESSION=reseed,
    )


def _inputs(source: RecordedStreamSource) -> SessionInputs:
    catalog, relationships, _messages_ignored = _messages()
    return SessionInputs(
        source=source,
        catalog=catalog,
        relationships=relationships,
        fees=fee_calculator(FIXTURES / "fees"),
        source_label="replay:unit",
        deterministic=True,
        fingerprint="sha256:completed-restart",
    )


def _source() -> RecordedStreamSource:
    _catalog, _relationships, messages = _messages()
    return RecordedStreamSource(DataSourceKind.REPLAY, _catalog, messages, speed=None)


async def _ids(store: DatabaseSessionStore, session_id: str) -> list[str]:
    rows = await store._inner.list_detection_rows(session_id)
    return [row.detection_id for row in rows]


async def _sessions(store: DatabaseSessionStore) -> list[IngestionSessionRow]:
    async with store._inner.sessions() as session:
        rows = (
            await session.execute(
                select(IngestionSessionRow).order_by(IngestionSessionRow.session_id)
            )
        ).scalars()
        return list(rows)


async def test_completed_fingerprint_is_not_ingested_again(db: str) -> None:
    settings = _settings(db)
    store = DatabaseSessionStore(settings)
    first = await asyncio.wait_for(
        WorkerService(settings, inputs=_inputs(_source()), store=store).run(), 60
    )
    assert first.status == "completed"
    original = await _ids(store, first.session_id)
    assert original
    second = await asyncio.wait_for(
        WorkerService(settings, inputs=_inputs(_source()), store=store).run(), 60
    )
    assert second.status == "completed"
    assert second.session_id == first.session_id
    assert second.detail == "already_completed"
    sessions = await _sessions(store)
    assert [row.session_id for row in sessions] == [first.session_id]
    assert sessions[0].status == "completed"
    assert await _ids(store, first.session_id) == original
    await store.aclose()


async def test_failed_mid_journal_resumes_without_duplicate_detection_ids(db: str) -> None:
    catalog, relationships, messages = _messages()
    settings = _settings(db)
    fingerprint = "sha256:failed-mid-journal"
    gate = asyncio.Event()

    class _Gated(RecordedStreamSource):
        def __init__(self) -> None:
            super().__init__(DataSourceKind.REPLAY, catalog, messages, speed=None)

        async def subscribe_orderbooks(self, market_ids: object = None) -> object:
            del market_ids
            self._state = ConnectionState.CONNECTED
            for index, msg in enumerate(self.messages):
                if index == 20:
                    await gate.wait()
                self.position = msg.position
                yield msg
            self._state = ConnectionState.DISCONNECTED

    def inputs(source: RecordedStreamSource) -> SessionInputs:
        return SessionInputs(
            source=source,
            catalog=catalog,
            relationships=relationships,
            fees=fee_calculator(FIXTURES / "fees"),
            source_label="replay:unit",
            deterministic=True,
            fingerprint=fingerprint,
        )

    store = DatabaseSessionStore(settings)
    first = WorkerService(settings, inputs=inputs(_Gated()), store=store)
    stop = asyncio.Event()
    task = asyncio.create_task(first.run(stop))
    await asyncio.wait_for(first.started.wait(), 5)
    while first.listener is None or first.listener.stats.checkpoints < 1:
        if task.done():
            break
        await asyncio.sleep(0.01)
    stop.set()
    gate.set()
    interrupted = await asyncio.wait_for(task, 30)
    assert interrupted.status == "interrupted"
    partial = await _ids(store, interrupted.session_id)
    async with store._inner.sessions() as session, session.begin():
        await session.execute(
            update(IngestionSessionRow)
            .where(IngestionSessionRow.session_id == interrupted.session_id)
            .values(status="failed", detail="mid-journal")
        )
    second = await asyncio.wait_for(
        WorkerService(
            settings,
            inputs=inputs(
                RecordedStreamSource(DataSourceKind.REPLAY, catalog, messages, speed=None)
            ),
            store=store,
        ).run(),
        60,
    )
    assert second.session_id == interrupted.session_id
    assert second.resumed_from is not None
    assert second.status == "completed"
    rows = await store._inner.list_detection_rows(second.session_id)
    ids = [row.detection_id for row in rows]
    assert len(ids) == len(set(ids))
    assert set(partial).issubset(set(ids))
    assert not [row for row in rows if row.status in ("OPEN", "UPDATED")]
    sessions = await _sessions(store)
    assert len(sessions) == 1
    await store.aclose()


async def test_reseed_opens_a_new_session(db: str) -> None:
    settings = _settings(db)
    store = DatabaseSessionStore(settings)
    first = await asyncio.wait_for(
        WorkerService(settings, inputs=_inputs(_source()), store=store).run(), 60
    )
    assert first.status == "completed"
    reseed = _settings(db, reseed=True)
    second = await asyncio.wait_for(
        WorkerService(reseed, inputs=_inputs(_source()), store=store).run(), 60
    )
    assert second.status == "completed"
    assert second.session_id != first.session_id
    sessions = await _sessions(store)
    assert len(sessions) == 2
    assert {row.status for row in sessions} == {"completed"}
    first_ids = await _ids(store, first.session_id)
    second_ids = await _ids(store, second.session_id)
    assert first_ids
    assert second_ids
    assert set(first_ids).isdisjoint(second_ids)
    await store.aclose()

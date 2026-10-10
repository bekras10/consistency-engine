"""A real Postgres restart must not duplicate or orphan detections."""

from __future__ import annotations

import asyncio
import subprocess
import time

import pytest
from sqlalchemy import select, text

from consistency_api.main import create_app
from consistency_connectors.base import ConnectionState
from consistency_connectors.settings import Settings
from consistency_connectors.sources.recorded import RecordedStreamSource
from consistency_core.models.common import DataSourceKind
from consistency_persistence.db import make_engine, ping
from consistency_persistence.schema import DetectionLegRow, DetectionRow
from consistency_worker.bootstrap import SessionInputs, fee_calculator
from consistency_worker.service import WorkerService
from consistency_worker.store import DatabaseSessionStore
from tests.conftest import FIXTURES
from tests.integration.test_postgres_pipeline import _messages

pytestmark = pytest.mark.integration


async def _restart_postgres(url: str) -> None:
    host_port = url.rsplit("@", 1)[-1].split("/", 1)[0]
    port = host_port.rsplit(":", 1)[-1]
    raw = subprocess.check_output(
        ["docker", "ps", "--filter", f"publish={port}", "--format", "{{.ID}} {{.Image}}"],
        text=True,
    )
    matches = [
        line.split()[0]
        for line in raw.splitlines()
        if line.strip() and "postgres" in line.split()[-1]
    ]
    if len(matches) != 1:
        raise AssertionError(f"expected one postgres container published on {port}, saw {raw!r}")
    subprocess.check_call(["docker", "restart", matches[0]])
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if await ping(url):
            return
        await asyncio.sleep(0.5)
    raise AssertionError("postgres did not accept connections after restart")


async def _orphans(url: str) -> int:
    engine = make_engine(url)
    try:
        async with engine.connect() as conn:
            value = await conn.scalar(
                text(
                    """
                    SELECT count(*)
                    FROM detection_legs AS leg
                    LEFT JOIN detections AS det ON det.detection_id = leg.detection_id
                    WHERE det.detection_id IS NULL
                    """
                )
            )
        return int(value or 0)
    finally:
        await engine.dispose()


async def test_worker_recovers_after_postgres_container_restart(db: str) -> None:
    catalog, relationships, messages = _messages()
    fees = fee_calculator(FIXTURES / "fees")
    settings = Settings(
        DATABASE_URL=db,
        checkpoint_every=15,
        journal_batch_size=8,
        retention_interval_s=3600,
    )
    fingerprint = "sha256:db-restart"
    gate = asyncio.Event()

    def inputs(source: RecordedStreamSource) -> SessionInputs:
        return SessionInputs(
            source=source,
            catalog=catalog,
            relationships=relationships,
            fees=fees,
            source_label="replay:unit",
            deterministic=True,
            fingerprint=fingerprint,
        )

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
    await store.aclose()

    await _restart_postgres(db)

    resumed = DatabaseSessionStore(settings)
    second = WorkerService(
        settings,
        inputs=inputs(RecordedStreamSource(DataSourceKind.REPLAY, catalog, messages, speed=None)),
        store=resumed,
    )
    finished = await asyncio.wait_for(second.run(), 30)
    assert finished.status == "completed"
    assert finished.resumed_from is not None
    assert finished.session_id == interrupted.session_id
    rows = await resumed._inner.list_detection_rows(finished.session_id)
    ids = [row.detection_id for row in rows]
    assert ids
    assert len(ids) == len(set(ids))
    assert await _orphans(db) == 0
    engine = make_engine(db)
    try:
        async with engine.connect() as conn:
            stored = (await conn.execute(select(DetectionRow.detection_id))).scalars().all()
            legs = (await conn.execute(select(DetectionLegRow.detection_id))).scalars().all()
        assert len(list(stored)) == len(set(stored))
        assert set(legs) <= set(stored)
    finally:
        await engine.dispose()

    app = create_app(Settings(DATABASE_URL=db))
    transport_client = httpx_client(app)
    async with transport_client as client:
        ready = await client.get("/api/v1/health/ready")
    assert ready.status_code == 200
    assert ready.json()["database"] == "ok"
    await resumed.aclose()


def httpx_client(app: object) -> object:
    import httpx

    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")

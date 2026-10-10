"""Crash and backend-kill recovery on a disposable Postgres, not the dev database.

The container name is ``consistency-engine-crash-db`` on host port 55432.
``docker kill`` and ``docker rm`` apply only to that container. The dev
database on 5433 (``consistency-engine-db-1``) and any Postgres on 5432 are
not restarted.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import subprocess
import sys
import time
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from consistency_api.main import create_app
from consistency_connectors.base import ConnectionState
from consistency_connectors.settings import Settings
from consistency_connectors.sources.recorded import RecordedStreamSource
from consistency_core.models.common import DataSourceKind
from consistency_persistence.db import make_engine, ping
from consistency_persistence.store import PersistenceStore
from consistency_worker.bootstrap import SessionInputs, fee_calculator
from consistency_worker.service import WorkerService
from consistency_worker.store import DatabaseSessionStore
from tests.conftest import FIXTURES
from tests.integration.test_phase12_db_restart import httpx_client
from tests.integration.test_postgres_pipeline import _messages

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
_NAME = "consistency-engine-crash-db"
_VOLUME = "consistency-engine-crash-data"
_PORT = 55432
_URL = f"postgresql+asyncpg://consistency:consistency@127.0.0.1:{_PORT}/consistency"


def _image() -> str:
    listed = subprocess.check_output(
        ["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"],
        text=True,
    )
    have = set(listed.splitlines())
    for name in (
        "postgres:16",
        "postgres:16.15",
        "public.ecr.aws/docker/library/postgres:16",
    ):
        if name in have:
            return name
    return "postgres:16"


def _destroy() -> None:
    subprocess.run(["docker", "rm", "-f", _NAME], check=False, capture_output=True)
    subprocess.run(["docker", "volume", "rm", "-f", _VOLUME], check=False, capture_output=True)


def _run_container() -> None:
    subprocess.check_call(
        [
            "docker",
            "run",
            "-d",
            "--name",
            _NAME,
            "--restart",
            "no",
            "-e",
            "POSTGRES_USER=consistency",
            "-e",
            "POSTGRES_PASSWORD=consistency",
            "-e",
            "POSTGRES_DB=consistency",
            "-p",
            f"{_PORT}:5432",
            "-v",
            f"{_VOLUME}:/var/lib/postgresql/data",
            _image(),
        ]
    )


def _migrate(url: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = url
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env=env,
        check=True,
    )


async def _wait_ready(url: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if await ping(url):
            return
        await asyncio.sleep(0.5)
    raise AssertionError("disposable postgres did not accept connections")


async def _disposable() -> AsyncIterator[str]:
    if _PORT in {5432, 5433}:
        raise AssertionError("disposable postgres must not use the dev database ports")
    _destroy()
    _run_container()
    try:
        await _wait_ready(_URL)
        _migrate(_URL)
        yield _URL
    finally:
        _destroy()


async def _counts(url: str) -> tuple[int, int, int]:
    engine = make_engine(url)
    try:
        async with engine.connect() as conn:
            detections = int(await conn.scalar(text("SELECT count(*) FROM detections")) or 0)
            legs = int(await conn.scalar(text("SELECT count(*) FROM detection_legs")) or 0)
            scenarios = int(
                await conn.scalar(text("SELECT count(*) FROM detection_scenarios")) or 0
            )
        return detections, legs, scenarios
    finally:
        await engine.dispose()


async def _assert_consistent(url: str) -> None:
    engine = make_engine(url)
    try:
        async with engine.connect() as conn:
            orphan_legs = int(
                await conn.scalar(
                    text(
                        """
                        SELECT count(*)
                        FROM detection_legs AS leg
                        LEFT JOIN detections AS det ON det.detection_id = leg.detection_id
                        WHERE det.detection_id IS NULL
                        """
                    )
                )
                or 0
            )
            orphan_scenarios = int(
                await conn.scalar(
                    text(
                        """
                        SELECT count(*)
                        FROM detection_scenarios AS sc
                        LEFT JOIN detections AS det ON det.detection_id = sc.detection_id
                        WHERE det.detection_id IS NULL
                        """
                    )
                )
                or 0
            )
            certificates = (
                (
                    await conn.execute(
                        text(
                            """
                        SELECT detection_id
                        FROM detections
                        WHERE certificate_json IS NOT NULL
                        """
                        )
                    )
                )
                .scalars()
                .all()
            )
            stored = (
                (await conn.execute(text("SELECT detection_id FROM detections"))).scalars().all()
            )
            legs = (
                (await conn.execute(text("SELECT detection_id FROM detection_legs")))
                .scalars()
                .all()
            )
            scenarios = (
                (await conn.execute(text("SELECT detection_id FROM detection_scenarios")))
                .scalars()
                .all()
            )
        assert orphan_legs == 0
        assert orphan_scenarios == 0
        assert set(certificates) <= set(stored)
        assert len(list(stored)) == len(set(stored))
        assert set(legs) <= set(stored)
        assert set(scenarios) <= set(stored)
    finally:
        await engine.dispose()


async def _insert_detection(conn: AsyncConnection, session_id: str, detection_id: str) -> None:
    await conn.execute(
        text(
            """
            INSERT INTO detections (
                detection_id, session_id, relationship_id, strategy_id, template,
                ordinal, status, classification, peak_classification, reason_codes,
                first_observed_at, last_observed_at, first_observed_ms, last_observed_ms,
                first_position, last_position, event_count, certificate_hash,
                certificate_json, certificate_version, record_json
            ) VALUES (
                :detection_id, :session_id, 'rel', 'strategy', 'implication',
                0, 'open', 'candidate', 'candidate', '[]'::jsonb,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0, 0,
                0, 0, 1, 'sha256:crash',
                :certificate, 'proof-certificate/2', '{}'::jsonb
            )
            """
        ),
        {
            "detection_id": detection_id,
            "session_id": session_id,
            "certificate": '{"certificate_version":"proof-certificate/2"}',
        },
    )
    await conn.execute(
        text(
            """
            INSERT INTO detection_legs (
                detection_id, leg_index, market_id, side, ratio
            ) VALUES (:detection_id, 0, 'm', 'yes', 1)
            """
        ),
        {"detection_id": detection_id},
    )
    await conn.execute(
        text(
            """
            INSERT INTO detection_scenarios (
                detection_id, scenario_index, state_json, payoff_per_unit, is_worst
            ) VALUES (:detection_id, 0, '[1]', 0, true)
            """
        ),
        {"detection_id": detection_id},
    )


async def test_terminate_backend_during_open_write_leaves_no_orphans(
    database_url: str,
) -> None:
    """``pg_terminate_backend`` aborts an open detection write on the disposable instance."""
    del database_url
    async for url in _disposable():
        catalog, relationships, _messages_unused = _messages()
        store = PersistenceStore(url)
        opened = await store.open(
            preferred_session_id="ses-terminate",
            fingerprint="sha256:terminate",
            deterministic=True,
            source_label="unit",
            source_kind="synthetic",
            catalog=catalog,
            relationships=relationships,
            fees=fee_calculator(FIXTURES / "fees"),
            persist_raw=False,
            pinned=False,
            batch_size=10,
            config_document={"test": "terminate"},
        )
        await store.aclose()
        engine = make_engine(url)
        writer = await engine.connect()
        try:
            await writer.begin()
            pid = int(await writer.scalar(text("SELECT pg_backend_pid()")) or 0)
            await _insert_detection(writer, opened.session_id, "det-terminated")
            async with engine.connect() as killer:
                terminated = await killer.scalar(
                    text("SELECT pg_terminate_backend(:pid)"),
                    {"pid": pid},
                )
            assert terminated is True
            with pytest.raises(DBAPIError):
                await writer.execute(text("SELECT 1"))
        finally:
            with contextlib.suppress(Exception):
                await writer.close()
            await engine.dispose()
        assert await _counts(url) == (0, 0, 0)
        await _assert_consistent(url)

        engine = make_engine(url)
        try:
            async with engine.begin() as conn:
                await _insert_detection(conn, opened.session_id, "det-kept")
        finally:
            await engine.dispose()
        assert await _counts(url) == (1, 1, 1)
        await _assert_consistent(url)
        return


async def test_worker_recovers_after_postgres_container_kill(database_url: str) -> None:
    """Kill the disposable Postgres while the worker is writing, then resume."""
    del database_url
    catalog, relationships, messages = _messages()
    fees = fee_calculator(FIXTURES / "fees")
    fingerprint = "sha256:db-kill"
    _destroy()
    _run_container()
    try:
        await _wait_ready(_URL)
        _migrate(_URL)
        settings = Settings(
            DATABASE_URL=_URL,
            checkpoint_every=15,
            journal_batch_size=8,
            retention_interval_s=3600,
        )

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

        class _Live(RecordedStreamSource):
            def __init__(self) -> None:
                super().__init__(DataSourceKind.REPLAY, catalog, messages, speed=None)

            async def subscribe_orderbooks(self, market_ids: object = None) -> object:
                del market_ids
                self._state = ConnectionState.CONNECTED
                for msg in self.messages:
                    self.position = msg.position
                    yield msg
                self._state = ConnectionState.DISCONNECTED

        store = DatabaseSessionStore(settings)
        first = WorkerService(settings, inputs=inputs(_Live()), store=store)
        task = asyncio.create_task(first.run())
        await asyncio.wait_for(first.started.wait(), 10)
        while first.listener is None or first.listener.stats.checkpoints < 1:
            if task.done():
                break
            await asyncio.sleep(0.01)
        assert not task.done()
        subprocess.check_call(["docker", "kill", _NAME])
        _done, pending = await asyncio.wait({task}, timeout=20)
        if task in pending:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        await store.aclose()

        subprocess.check_call(["docker", "rm", _NAME])
        _run_container()
        await _wait_ready(_URL)
        _migrate(_URL)

        resumed = DatabaseSessionStore(settings)
        second = WorkerService(
            settings,
            inputs=inputs(
                RecordedStreamSource(DataSourceKind.REPLAY, catalog, messages, speed=None)
            ),
            store=resumed,
        )
        finished = await asyncio.wait_for(second.run(), 60)
        assert finished.status == "completed"
        assert finished.resumed_from is not None
        assert finished.session_id
        rows = await resumed._inner.list_detection_rows(finished.session_id)
        ids = [row.detection_id for row in rows]
        assert ids
        assert len(ids) == len(set(ids))
        await _assert_consistent(_URL)
        engine = make_engine(_URL)
        try:
            async with engine.connect() as conn:
                stored = (await conn.execute(text("SELECT detection_id FROM detections"))).scalars()
                legs = (
                    await conn.execute(text("SELECT detection_id FROM detection_legs"))
                ).scalars()
                scenarios = (
                    await conn.execute(text("SELECT detection_id FROM detection_scenarios"))
                ).scalars()
            stored_ids = list(stored)
            assert len(stored_ids) == len(set(stored_ids))
            assert set(legs) <= set(stored_ids)
            assert set(scenarios) <= set(stored_ids)
        finally:
            await engine.dispose()

        app = create_app(Settings(DATABASE_URL=_URL))
        async with httpx_client(app) as client:
            ready = await client.get("/api/v1/health/ready")
        assert ready.status_code == 200
        assert ready.json()["database"] == "ok"
        await resumed.aclose()
    finally:
        _destroy()

"""Retention runs in the maintenance process while ingestion is idle."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text

from consistency_persistence.db import make_engine
from consistency_persistence.store import PersistenceStore
from tests.integration.test_outbox_retention import _seed
from tests.integration.test_postgres_pipeline import _messages

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]


def _pythonpath() -> str:
    roots = [
        "packages/core/src",
        "packages/simulation/src",
        "packages/connectors/src",
        "packages/pipeline/src",
        "packages/persistence/src",
        "apps/api/src",
        "apps/worker/src",
    ]
    return os.pathsep.join(str(ROOT / item) for item in roots)


async def test_maintenance_prunes_while_ingestion_is_idle(db: str) -> None:
    catalog, relationships, _unused = _messages()
    from consistency_worker.bootstrap import fee_calculator
    from tests.conftest import FIXTURES

    store = PersistenceStore(db)
    opened = await store.open(
        preferred_session_id="ses-idle-retention",
        fingerprint="sha256:idle-retention",
        deterministic=True,
        source_label="synthetic:other",
        source_kind="synthetic",
        catalog=catalog,
        relationships=relationships,
        fees=fee_calculator(FIXTURES / "fees"),
        persist_raw=True,
        pinned=False,
        batch_size=10,
        config_document={"test": "idle-retention"},
    )
    await store.close_session(opened.session_id, status="completed", detail=None)
    aged = datetime.now(UTC) - timedelta(days=30)
    async with store.sessions() as session, session.begin():
        await session.execute(
            text(
                """
                UPDATE ingestion_sessions
                SET started_at = :aged
                WHERE session_id = :sid
                """
            ),
            {"sid": opened.session_id, "aged": aged},
        )
        await session.execute(
            text(
                """
                INSERT INTO orderbook_updates (
                    session_id, ordinal, kind, received_at, received_ts_ms,
                    fee_schedule_version, relationship_version, entry_json
                ) VALUES (
                    :sid, 0, 'heartbeat', :received, 0, 'fee', 'rel', CAST('{}' AS jsonb)
                )
                """
            ),
            {"sid": opened.session_id, "received": aged},
        )
    await _seed(db, 5)
    env = os.environ.copy()
    env.update(
        {
            "DATABASE_URL": db,
            "PYTHONPATH": _pythonpath(),
            "RETENTION_INTERVAL_S": "1",
            "RETENTION_MAX_AGE_HOURS": "1",
            "RETENTION_MAX_SESSIONS": "20",
            "OUTBOX_RETENTION_KEEP": "1",
            "DATA_SOURCE": "synthetic",
        }
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "consistency_worker", "maintain"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    engine = make_engine(db)
    try:
        deadline = time.monotonic() + 20
        updates = 1
        outbox = 5
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            async with engine.connect() as conn:
                updates = int(
                    await conn.scalar(text("SELECT COUNT(*) FROM orderbook_updates")) or 0
                )
                outbox = int(
                    await conn.scalar(text("SELECT COUNT(*) FROM notification_outbox")) or 0
                )
            if updates == 0 and outbox == 1:
                break
            await _sleep()
        assert proc.poll() is None
        assert updates == 0
        assert outbox == 1
        async with engine.connect() as conn:
            sessions = int(await conn.scalar(text("SELECT COUNT(*) FROM ingestion_sessions")) or 0)
        assert sessions == 1
    finally:
        if proc.poll() is None:
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        if proc.stdout is not None:
            proc.stdout.close()
        if proc.stderr is not None:
            proc.stderr.close()
        await engine.dispose()
        await store.aclose()


async def _sleep() -> None:
    import asyncio

    await asyncio.sleep(0.25)


async def _completed(db: str, name: str) -> PersistenceStore:
    catalog, relationships, _unused = _messages()
    from consistency_worker.bootstrap import fee_calculator
    from tests.conftest import FIXTURES

    store = PersistenceStore(db)
    opened = await store.open(
        preferred_session_id=f"ses-{name}",
        fingerprint=f"sha256:{name}",
        deterministic=True,
        source_label="synthetic:other",
        source_kind="synthetic",
        catalog=catalog,
        relationships=relationships,
        fees=fee_calculator(FIXTURES / "fees"),
        persist_raw=False,
        pinned=False,
        batch_size=10,
        config_document={"test": name},
    )
    await store.close_session(opened.session_id, status="completed", detail=None)
    return store


def _maintain_env(db: str, heartbeat: Path, *, fault: str | None) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "DATABASE_URL": db,
            "PYTHONPATH": _pythonpath(),
            "RETENTION_INTERVAL_S": "30",
            "DATA_SOURCE": "synthetic",
            "EXPECT_MAINTENANCE": "1",
            "MAINTENANCE_HEARTBEAT_PATH": str(heartbeat),
            "MAINTENANCE_HEARTBEAT_S": "0.2",
            "MAINTENANCE_STALE_S": "30",
        }
    )
    if fault is None:
        env.pop("RETENTION_FAULT", None)
    else:
        env["RETENTION_FAULT"] = fault
    return env


async def test_retention_task_exit_is_unhealthy(
    db: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = await _completed(db, "retention-exit")
    heartbeat = tmp_path / "heartbeat"
    proc = subprocess.Popen(
        [sys.executable, "-m", "consistency_worker", "maintain"],
        cwd=ROOT,
        env=_maintain_env(db, heartbeat, fault="exit"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        code = proc.wait(timeout=20)
        assert code == 1
        text = heartbeat.read_text(encoding="utf-8")
        assert "retention=exited" in text
        monkeypatch.setenv("EXPECT_MAINTENANCE", "1")
        monkeypatch.setenv("MAINTENANCE_HEARTBEAT_PATH", str(heartbeat))
        monkeypatch.setenv("MAINTENANCE_STALE_S", "30")
        from consistency_worker.health import report_health

        assert await report_health(db) == ("unhealthy", 1)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
        if proc.stdout is not None:
            proc.stdout.close()
        if proc.stderr is not None:
            proc.stderr.close()
        await store.aclose()


async def test_retention_task_failure_is_unhealthy(
    db: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = await _completed(db, "retention-fail")
    heartbeat = tmp_path / "heartbeat"
    proc = subprocess.Popen(
        [sys.executable, "-m", "consistency_worker", "maintain"],
        cwd=ROOT,
        env=_maintain_env(db, heartbeat, fault="fail"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        code = proc.wait(timeout=20)
        assert code == 1
        text = heartbeat.read_text(encoding="utf-8")
        assert "retention=failed" in text
        monkeypatch.setenv("EXPECT_MAINTENANCE", "1")
        monkeypatch.setenv("MAINTENANCE_HEARTBEAT_PATH", str(heartbeat))
        monkeypatch.setenv("MAINTENANCE_STALE_S", "30")
        from consistency_worker.health import report_health

        assert await report_health(db) == ("unhealthy", 1)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
        if proc.stdout is not None:
            proc.stdout.close()
        if proc.stderr is not None:
            proc.stderr.close()
        await store.aclose()


async def test_live_maintenance_with_completed_session_stays_healthy(
    db: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = await _completed(db, "retention-live")
    heartbeat = tmp_path / "heartbeat"
    proc = subprocess.Popen(
        [sys.executable, "-m", "consistency_worker", "maintain"],
        cwd=ROOT,
        env=_maintain_env(db, heartbeat, fault=None),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 15
        text = ""
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            if heartbeat.exists():
                text = heartbeat.read_text(encoding="utf-8")
                if "retention=running" in text:
                    break
            await _sleep()
        assert proc.poll() is None
        assert "retention=running" in text
        monkeypatch.setenv("EXPECT_MAINTENANCE", "1")
        monkeypatch.setenv("MAINTENANCE_HEARTBEAT_PATH", str(heartbeat))
        monkeypatch.setenv("MAINTENANCE_STALE_S", "30")
        from consistency_worker.health import report_health

        assert await report_health(db) == ("completed", 0)
    finally:
        if proc.poll() is None:
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        if proc.stdout is not None:
            proc.stdout.close()
        if proc.stderr is not None:
            proc.stderr.close()
        await store.aclose()

"""The health command reads session lifecycle, not only database connectivity.

A completed ingestion session is a healthy terminal state only while maintenance,
when the entrypoint expects it, has a fresh heartbeat. A stale maintenance
process is unhealthy even if the last session completed. A failed session is
unhealthy on its own.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest
from sqlalchemy import update

from consistency_persistence.schema import IngestionSessionRow
from consistency_persistence.store import PersistenceStore
from consistency_worker.bootstrap import fee_calculator
from consistency_worker.health import report_health
from tests.conftest import FIXTURES
from tests.integration.test_postgres_pipeline import _messages

pytestmark = pytest.mark.integration


async def test_report_health_distinguishes_completed_running_and_failed(db: str) -> None:
    catalog, relationships, _unused = _messages()
    store = PersistenceStore(db)
    try:
        assert await report_health(db) == ("starting", 1)
        opened = await store.open(
            preferred_session_id="ses-health",
            fingerprint="sha256:health",
            deterministic=True,
            source_label="synthetic:other",
            source_kind="synthetic",
            catalog=catalog,
            relationships=relationships,
            fees=fee_calculator(FIXTURES / "fees"),
            persist_raw=False,
            pinned=False,
            batch_size=10,
            config_document={"test": "health"},
        )
        assert await report_health(db) == ("running", 0)
        async with store.sessions() as session, session.begin():
            await session.execute(
                update(IngestionSessionRow)
                .where(IngestionSessionRow.session_id == opened.session_id)
                .values(status="completed")
            )
        assert await report_health(db) == ("completed", 0)
        async with store.sessions() as session, session.begin():
            await session.execute(
                update(IngestionSessionRow)
                .where(IngestionSessionRow.session_id == opened.session_id)
                .values(status="failed")
            )
        assert await report_health(db) == ("failed", 1)
    finally:
        await store.aclose()


def _maintenance(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    *,
    epoch: float,
    retention: str = "running",
    pid: int | None = None,
    attest: bool = True,
) -> None:
    process = os.getpid() if pid is None else pid
    if attest:
        path.write_text(f"{epoch:.6f}\nretention={retention}\npid={process}\n", encoding="utf-8")
    else:
        path.write_text(f"{epoch:.6f}\n", encoding="utf-8")
    monkeypatch.setenv("EXPECT_MAINTENANCE", "1")
    monkeypatch.setenv("MAINTENANCE_HEARTBEAT_PATH", str(path))
    monkeypatch.setenv("MAINTENANCE_STALE_S", "30")


async def _completed_session(db: str) -> PersistenceStore:
    catalog, relationships, _unused = _messages()
    store = PersistenceStore(db)
    opened = await store.open(
        preferred_session_id="ses-health-maintenance",
        fingerprint="sha256:health-maintenance",
        deterministic=True,
        source_label="synthetic:other",
        source_kind="synthetic",
        catalog=catalog,
        relationships=relationships,
        fees=fee_calculator(FIXTURES / "fees"),
        persist_raw=False,
        pinned=False,
        batch_size=10,
        config_document={"test": "health-maintenance"},
    )
    async with store.sessions() as session, session.begin():
        await session.execute(
            update(IngestionSessionRow)
            .where(IngestionSessionRow.session_id == opened.session_id)
            .values(status="completed")
        )
    return store


async def test_completed_session_with_fresh_maintenance_is_healthy(
    db: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = await _completed_session(db)
    try:
        _maintenance(monkeypatch, tmp_path / "heartbeat", epoch=time.time())
        assert await report_health(db) == ("completed", 0)
    finally:
        await store.aclose()


async def test_completed_session_with_stale_maintenance_is_unhealthy(
    db: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = await _completed_session(db)
    try:
        _maintenance(monkeypatch, tmp_path / "heartbeat", epoch=time.time() - 120)
        assert await report_health(db) == ("unhealthy", 1)
    finally:
        await store.aclose()


async def test_fresh_timestamp_without_retention_attestation_is_unhealthy(
    db: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = await _completed_session(db)
    try:
        _maintenance(monkeypatch, tmp_path / "heartbeat", epoch=time.time(), attest=False)
        assert await report_health(db) == ("unhealthy", 1)
    finally:
        await store.aclose()


async def test_failed_ingestion_is_unhealthy_even_with_fresh_maintenance(
    db: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = await _completed_session(db)
    try:
        async with store.sessions() as session, session.begin():
            await session.execute(update(IngestionSessionRow).values(status="failed"))
        _maintenance(monkeypatch, tmp_path / "heartbeat", epoch=time.time())
        label, code = await report_health(db)
        assert code == 1
        assert label == "failed"
    finally:
        await store.aclose()

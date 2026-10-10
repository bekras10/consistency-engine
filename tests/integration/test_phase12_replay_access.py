"""Replay mutations on the gateway and the public API require a token and fork viewers."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import httpx
import pytest

from consistency_persistence.recording import BatchJournal, VersionStamp
from consistency_pipeline.journal import JournalEntry
from tests.golden.support import fee_calculator
from tests.integration.test_phase10_correctness import _one_market, _open
from tests.integration.test_phase11_api import _client
from tests.unit.test_ingestion import Feed, snap

pytestmark = pytest.mark.integration

_ROOT = Path(__file__).resolve().parents[2]


def _gateway_app() -> object:
    path = _ROOT / "scripts" / "dashboard_gateway.py"
    spec = importlib.util.spec_from_file_location("dashboard_gateway_phase12", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("dashboard gateway module is missing")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.app


async def _journal(db: str, session_id: str) -> None:
    catalog = _one_market()
    fees = fee_calculator()
    store = await _open(db, catalog, [], fees, session_id)
    try:
        feed = Feed()
        entries = [
            JournalEntry(ordinal=0, message=feed.msg(snap("A", 1, yes=[("0.40", "10")]))),
            JournalEntry(ordinal=1, message=feed.msg(snap("A", 2, yes=[("0.55", "4")]))),
        ]
        batch = BatchJournal(
            store.sessions,
            session_id,
            VersionStamp(fee_schedule_version="fee", relationship_version="rel", rules_hashes={}),
            batch_size=20,
            enabled=True,
        )
        for entry in entries:
            batch.append(entry)
        await batch.flush()
    finally:
        await store.aclose()


async def test_public_api_and_gateway_reject_unauthorized_replay_posts(
    db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("REPLAY_API_TOKEN", "phase12-token")
    monkeypatch.setenv("REPLAY_MUTATIONS_PUBLIC", "false")
    async for client in _client(db, monkeypatch):
        response = await client.post("/api/v1/replay/sessions/ses-phase12/start")
        assert response.status_code == 401
        assert response.json() == {"error": "unauthorized"}

    monkeypatch.setenv("DATABASE_URL", db)
    monkeypatch.setenv("DASHBOARD_GATEWAY_HOST", "0.0.0.0")
    app = _gateway_app()
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as gateway:
            denied = await gateway.post("/internal/replay/ses-phase12/start", json={})
            assert denied.status_code == 401
            assert denied.json() == {"error": "unauthorized"}


async def test_gateway_viewers_seek_independently(db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    await _journal(db, "ses-gw")
    monkeypatch.setenv("DATABASE_URL", db)
    monkeypatch.setenv("REPLAY_API_TOKEN", "phase12-token")
    monkeypatch.setenv("REPLAY_MUTATIONS_PUBLIC", "false")
    monkeypatch.setenv("DASHBOARD_GATEWAY_HOST", "0.0.0.0")
    app = _gateway_app()
    headers = {"X-Replay-Token": "phase12-token"}
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as gateway:
            first = await gateway.post("/internal/replay/ses-gw/start", headers=headers, json={})
            second = await gateway.post("/internal/replay/ses-gw/start", headers=headers, json={})
            assert first.status_code == 200 and second.status_code == 200
            first_id = first.json()["data"]["replay_id"]
            second_id = second.json()["data"]["replay_id"]
            assert first_id != second_id
            assert first_id != "ses-gw"
            early = await gateway.post(
                f"/internal/replay/{first_id}/seek",
                headers=headers,
                json={"timestamp_ms": 0},
            )
            late = await gateway.post(
                f"/internal/replay/{second_id}/seek",
                headers=headers,
                json={"timestamp_ms": 9_000_000_000_000},
            )
            assert early.status_code == 200 and late.status_code == 200
            assert early.json()["data"]["cursor"] == 0
            assert late.json()["data"]["cursor"] == 2
            again = await gateway.get(f"/internal/replay/{second_id}")
            assert again.json()["data"]["cursor"] == 2
            assert again.json()["data"]["replay_id"] == second_id

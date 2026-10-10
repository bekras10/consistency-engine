"""Heartbeats and the connection cap on GET /api/v1/stream."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from consistency_api.main import create_app
from consistency_connectors.settings import Settings
from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.replay_host import BookTail, ReplayHost
from tests.integration.test_phase11_api import _pull_sse

pytestmark = pytest.mark.integration


async def test_idle_stream_sends_heartbeat_and_over_cap_is_refused(
    db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATABASE_URL", db)
    monkeypatch.setenv("SSE_HEARTBEAT_SECONDS", "0.05")
    monkeypatch.setenv("SSE_MAX_CONNECTIONS", "1")
    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    app = create_app(Settings(), sessions=maker, replay=ReplayHost(), books=BookTail())
    release = asyncio.Event()
    seen = asyncio.Event()

    async def hold(body: str) -> None:
        if ": heartbeat" in body:
            seen.set()
            await release.wait()
            raise asyncio.CancelledError

    first = asyncio.create_task(_pull_sse(app, [], hold))
    await asyncio.wait_for(seen.wait(), 5)
    async with (
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
        client.stream("GET", "/api/v1/stream") as denied,
    ):
        assert denied.status_code == 503
        body = await denied.aread()
    release.set()
    status, _headers, text = await first
    assert status == 200
    assert ": heartbeat" in text
    assert json.loads(body) == {"error": "stream_unavailable"}
    await engine.dispose()

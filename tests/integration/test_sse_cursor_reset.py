"""A Last-Event-ID past the log must resync instead of waiting forever."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from consistency_api.main import create_app
from consistency_connectors.settings import Settings
from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.replay_host import BookTail, ReplayHost
from tests.integration.test_phase11_api import _frames, _pull_sse

pytestmark = pytest.mark.integration


async def test_last_event_id_past_the_log_sends_a_full_resync(
    db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    monkeypatch.setenv("DATABASE_URL", db)
    monkeypatch.setenv("SSE_HEARTBEAT_SECONDS", "30")
    try:
        async with maker() as session, session.begin():
            await session.execute(
                text(
                    """
                    INSERT INTO notification_outbox (topic, session_id, payload, created_at)
                    VALUES (
                        'detection', NULL, CAST(:payload AS jsonb), CURRENT_TIMESTAMP
                    )
                    """
                ),
                {"payload": '{"version": 1}'},
            )
            high = await session.scalar(
                text("SELECT COALESCE(MAX(id), 0) FROM notification_outbox")
            )
        assert isinstance(high, int) and high > 0
        app = create_app(Settings(), sessions=maker, replay=ReplayHost(), books=BookTail())
        stale = high + 1_000_000

        async def stop_on_resync(body: str) -> None:
            if any(frame.get("event") == "resync" for frame in _frames(body)):
                raise asyncio.CancelledError

        status, _headers, body = await _pull_sse(
            app,
            [(b"last-event-id", str(stale).encode())],
            stop_on_resync,
        )
        assert status == 200
        resyncs = [frame for frame in _frames(body) if frame.get("event") == "resync"]
        assert resyncs
        data = resyncs[0]["data"]
        assert isinstance(data, dict)
        assert data["outbox_id"] == high
        assert int(str(resyncs[0]["id"])) == high
    finally:
        await engine.dispose()

"""The retention loop prunes the outbox, and SSE does not skip a pruned prefix."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from consistency_api.main import create_app
from consistency_connectors.settings import Settings
from consistency_persistence.outbox import pruned_through
from consistency_persistence.replay_host import BookTail, ReplayHost
from consistency_persistence.retention import RetentionPolicy
from consistency_persistence.store import PersistenceStore
from tests.integration.test_outbox_retention import _seed
from tests.integration.test_phase11_api import _frames, _pull_sse

pytestmark = pytest.mark.integration

_KEEP = 10
_TOTAL = 30


async def test_retention_prunes_outbox_to_the_consumer_watermark(db: str) -> None:
    await _seed(db, _TOTAL)
    store = PersistenceStore(db)
    try:
        await store.run_retention(
            RetentionPolicy(
                max_age_hours=168,
                max_sessions=20,
                third_party_hours=0,
                outbox_keep=_KEEP,
            )
        )
        async with store.sessions() as session:
            floor = await pruned_through(session)
            remaining = await session.scalar(text("SELECT COUNT(*) FROM notification_outbox"))
            claims = await session.scalar(text("SELECT COUNT(*) FROM outbox_claims"))
        assert floor == _TOTAL - _KEEP
        assert remaining == _KEEP
        assert claims == _KEEP
    finally:
        await store.aclose()


async def test_sse_resyncs_below_the_floor_and_replays_inside_it(
    db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _seed(db, _TOTAL)
    store = PersistenceStore(db)
    monkeypatch.setenv("DATABASE_URL", db)
    monkeypatch.setenv("SSE_HEARTBEAT_SECONDS", "30")
    try:
        await store.run_retention(
            RetentionPolicy(
                max_age_hours=168,
                max_sessions=20,
                third_party_hours=0,
                outbox_keep=_KEEP,
            )
        )
        async with store.sessions() as session:
            floor = await pruned_through(session)
        assert floor == _TOTAL - _KEEP
        app = create_app(
            Settings(),
            sessions=store.sessions,
            replay=ReplayHost(),
            books=BookTail(),
        )

        async def stop_on_resync(body: str) -> None:
            frames = _frames(body)
            if any(frame.get("event") == "resync" for frame in frames):
                raise asyncio.CancelledError

        status, _headers, body = await _pull_sse(
            app,
            [(b"last-event-id", b"10")],
            stop_on_resync,
        )
        assert status == 200
        frames = [frame for frame in _frames(body) if frame.get("event")]
        assert frames
        assert frames[0]["event"] == "resync"
        assert all(frame["event"] == "resync" for frame in frames)
        data = frames[0]["data"]
        assert isinstance(data, dict)
        assert data["outbox_id"] == _TOTAL
        delivered = [int(str(frame["id"])) for frame in frames if frame.get("event") != "resync"]
        assert delivered == []

        async def stop_at_tip(body: str) -> None:
            frames = _frames(body)
            if any(str(frame.get("id")) == str(_TOTAL) for frame in frames):
                raise asyncio.CancelledError

        status, _headers, inside = await _pull_sse(
            app,
            [(b"last-event-id", b"25")],
            stop_at_tip,
        )
        assert status == 200
        inside_frames = [frame for frame in _frames(inside) if frame.get("event")]
        assert all(frame.get("event") != "resync" for frame in inside_frames)
        assert [int(str(frame["id"])) for frame in inside_frames] == list(range(25, _TOTAL + 1))
    finally:
        await store.aclose()

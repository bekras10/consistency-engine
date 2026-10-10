"""Viewer cap, idle expiry, and cancellation. A recently used viewer stays."""

from __future__ import annotations

import asyncio

import pytest

from consistency_persistence.replay_host import ReplayCapacityError, ReplayHost
from consistency_pipeline.playback import PlaybackSession
from tests.replay.test_playback_hardening import _recording, _session


def _blocking(replay_id: str) -> PlaybackSession:
    harness, entries = _recording()

    async def block(seconds: float) -> None:
        del seconds
        await asyncio.Event().wait()

    session = _session(harness, entries, replay_id=replay_id)
    session._sleep = block
    return session


def test_viewer_cap_rejects_another_session() -> None:
    host = ReplayHost(ttl_s=30, max_viewers=1, clock=lambda: 0.0)
    host.service.open(_blocking("one"))
    host.touch("one")
    with pytest.raises(ReplayCapacityError):
        host.require_capacity()


async def test_expired_session_is_cancelled_and_a_live_session_stays() -> None:
    now = [0.0]
    host = ReplayHost(ttl_s=10, max_viewers=4, clock=lambda: now[0])
    live = _blocking("live")
    stale = _blocking("stale")
    host.service.open(live)
    host.service.open(stale)
    host.touch("live")
    host.touch("stale")
    live.start()
    stale.start()
    await asyncio.sleep(0)
    stale_task = stale._task
    assert stale_task is not None and not stale_task.done()
    now[0] = 5
    assert host.sweep() == []
    assert host.service.get("stale").status == "playing"
    now[0] = 50
    host.touch("live")
    now[0] = 55
    removed = host.sweep()
    assert removed == ["stale"]
    await asyncio.sleep(0)
    assert stale_task.cancelled()
    assert host.service.get("live").status == "playing"
    with pytest.raises(KeyError):
        host.service.get("stale")

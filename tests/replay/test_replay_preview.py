"""Recording previews are capped and do not block an open viewer."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from consistency_persistence import replay_host as replay_host_module
from consistency_persistence.replay_host import ReplayHost, ReplayPreviewBusyError
from tests.replay.test_replay_limits import _blocking


class _Session:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *_args: object) -> None:
        return None


def _sessions() -> _Session:
    return _Session()


async def test_preview_cap_fails_fast_and_a_viewer_is_not_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    host = ReplayHost(ttl_s=30, max_viewers=2, max_previews=1, clock=lambda: 0.0)
    viewer = _blocking("viewer-live")
    host.service.open(viewer)
    host.touch("viewer-live")
    viewer.start()
    await asyncio.sleep(0)
    assert viewer.status == "playing"
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow(_session: object, replay_id: str) -> Any:
        started.set()
        await release.wait()
        return _blocking("preview-" + replay_id)

    monkeypatch.setattr(replay_host_module, "_load_playback", slow)
    first = asyncio.create_task(host.preview(_sessions, "recording-a"))  # type: ignore[arg-type]
    await asyncio.wait_for(started.wait(), 2)
    began = time.monotonic()
    with pytest.raises(ReplayPreviewBusyError, match="replay preview cap"):
        await host.preview(_sessions, "recording-b")  # type: ignore[arg-type]
    assert time.monotonic() - began < 0.25

    commanded = time.monotonic()
    paused = host.command_existing("viewer-live", "pause")
    assert time.monotonic() - commanded < 0.25
    assert paused["replay_id"] == "viewer-live"
    assert paused["status"] == "paused"

    release.set()
    body = await asyncio.wait_for(first, 2)
    assert body["replay_id"] == "preview-recording-a"
    assert host._previews_in_flight == 0
    again = await host.preview(_sessions, "recording-c")  # type: ignore[arg-type]
    assert again["replay_id"] == "preview-recording-c"

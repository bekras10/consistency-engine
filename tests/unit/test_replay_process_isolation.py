"""Viewer cursors are per process. A second app, or a new one, does not inherit them."""

from __future__ import annotations

from consistency_api.main import create_app
from consistency_connectors.settings import Settings


def test_two_api_processes_do_not_share_viewer_cursors() -> None:
    first = create_app(Settings())
    second = create_app(Settings())
    assert first.state.replay is not second.state.replay
    first.state.replay._seen["viewer-a"] = 1.0
    assert "viewer-a" not in second.state.replay._seen
    restarted = create_app(Settings())
    assert restarted.state.replay._seen == {}
    assert "viewer-a" not in restarted.state.replay._seen

"""Replay mutations are rejected without the shared token."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from consistency_api.main import create_app
from consistency_connectors.settings import Settings


def test_replay_mutation_without_token_is_unauthorized(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPLAY_MUTATIONS_PUBLIC", "false")
    monkeypatch.delenv("REPLAY_API_TOKEN", raising=False)
    client = TestClient(create_app(Settings()))
    response = client.post("/api/v1/replay/sessions/ses/start")
    assert response.status_code == 401
    assert response.json() == {"error": "unauthorized"}


def test_replay_mutation_with_wrong_token_is_unauthorized(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPLAY_MUTATIONS_PUBLIC", "false")
    monkeypatch.setenv("REPLAY_API_TOKEN", "expected")
    client = TestClient(create_app(Settings()))
    response = client.post(
        "/api/v1/replay/sessions/ses/pause",
        headers={"X-Replay-Token": "other"},
    )
    assert response.status_code == 401
    assert response.json() == {"error": "unauthorized"}

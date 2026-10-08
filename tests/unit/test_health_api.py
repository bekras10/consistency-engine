from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from consistency_api.main import create_app
from consistency_connectors.settings import ConfigurationError, DataSourceMode, Settings


def test_live_endpoint() -> None:
    client = TestClient(create_app(Settings()))
    resp = client.get("/api/v1/health/live")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_ready_endpoint_reports_safe_defaults() -> None:
    client = TestClient(create_app(Settings()))
    resp = client.get("/api/v1/health/ready")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ready"
    assert body["configuration"] == {
        "data_source": "synthetic",
        "enable_kalshi_api": "false",
        "kalshi_authorization_confirmed": "false",
        "enable_live_trading": "false",
    }


def test_ready_endpoint_not_ready_on_forbidden_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_LIVE_TRADING", "true")
    client = TestClient(create_app())
    resp = client.get("/api/v1/health/ready")
    assert resp.status_code == 503
    assert resp.json()["status"] == "not_ready"


def test_live_trading_refused() -> None:
    with pytest.raises(ConfigurationError):
        Settings(ENABLE_LIVE_TRADING=True)


@pytest.mark.parametrize(("enable", "confirmed"), [(False, False), (True, False), (False, True)])
def test_kalshi_mode_requires_both_flags(enable: bool, confirmed: bool) -> None:
    with pytest.raises(ConfigurationError):
        Settings(
            DATA_SOURCE=DataSourceMode.KALSHI_AUTHORIZED,
            ENABLE_KALSHI_API=enable,
            KALSHI_AUTHORIZATION_CONFIRMED=confirmed,
        )


def test_kalshi_mode_allowed_with_both_flags() -> None:
    s = Settings(
        DATA_SOURCE=DataSourceMode.KALSHI_AUTHORIZED,
        ENABLE_KALSHI_API=True,
        KALSHI_AUTHORIZATION_CONFIRMED=True,
    )
    assert s.kalshi_access_allowed

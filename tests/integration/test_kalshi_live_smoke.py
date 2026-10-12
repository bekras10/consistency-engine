"""Opt-in Kalshi connectivity smoke. CI does not set KALSHI_LIVE_SMOKE."""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    not (
        os.environ.get("KALSHI_LIVE_SMOKE") == "1"
        and os.environ.get("ENABLE_KALSHI_API", "").lower() == "true"
        and os.environ.get("KALSHI_AUTHORIZATION_CONFIRMED", "").lower() == "true"
        and bool(os.environ.get("KALSHI_API_KEY_ID", "").strip())
        and bool(os.environ.get("KALSHI_PRIVATE_KEY_PATH", "").strip())
    ),
    reason="live Kalshi smoke requires operator flags, credentials, and KALSHI_LIVE_SMOKE=1",
)


async def test_account_limits_connectivity() -> None:
    from consistency_connectors.settings import Settings
    from consistency_connectors.sources.kalshi import KalshiDataSource

    source = KalshiDataSource(Settings())
    result = await source.connectivity_check()
    assert result.get("ok") is True
    assert "private_key" not in result

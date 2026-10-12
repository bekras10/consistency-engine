"""The Kalshi compose override passes operator settings and does not bake secrets."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OVERRIDE = ROOT / "compose.kalshi.yml"
BASE = ROOT / "docker-compose.yml"


def test_kalshi_override_passes_series_and_fee_settings_without_secrets() -> None:
    text = OVERRIDE.read_text(encoding="utf-8")
    for name in (
        "KALSHI_SERIES_TICKERS",
        "KALSHI_MEMBER_CLASS",
        "KALSHI_INTERMEDIARY_FEES",
        "KALSHI_FEE_SCHEDULE_REVISIONS_CHECKED",
        "KALSHI_DISCOVERY_INTERVAL_S",
        "KALSHI_COMPARE_SKEW_MS",
        "STUDY_ID",
        "THIRD_PARTY_RAW_PERSISTENCE_AUTHORIZED",
    ):
        assert name in text
    assert "ENABLE_KALSHI_API: ${ENABLE_KALSHI_API:-false}" in text
    assert "KALSHI_AUTHORIZATION_CONFIRMED: ${KALSHI_AUTHORIZATION_CONFIRMED:-false}" in text
    assert 'ENABLE_LIVE_TRADING: "false"' in text
    assert "BEGIN PRIVATE KEY" not in text
    assert "KALSHI_API_KEY_ID: ${KALSHI_API_KEY_ID:-}" in text
    assert "/run/secrets/kalshi.key" in text
    base = BASE.read_text(encoding="utf-8")
    assert "KALSHI_SERIES_TICKERS" not in base
    assert "kalshi_authorized" not in base

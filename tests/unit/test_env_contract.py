"""`.env.example` names every setting and the other variables the process reads."""

from __future__ import annotations

from pathlib import Path

from consistency_connectors.settings import Settings

ROOT = Path(__file__).resolve().parents[2]

_EXTRA = (
    "POSTGRES_PORT",
    "REPLAY_API_TOKEN",
    "REPLAY_MUTATIONS_PUBLIC",
    "REPLAY_SESSION_TTL_S",
    "REPLAY_MAX_VIEWERS",
    "REPLAY_MAX_PREVIEWS",
    "REPLAY_CAPABILITY_TTL_S",
    "REPLAY_CAPABILITY_MAX",
    "REPLAY_CAPABILITY_ISSUE_LIMIT",
    "REPLAY_CAPABILITY_ISSUE_WINDOW_S",
    "REPLAY_COOKIE_SECURE",
    "SSE_MAX_CONNECTIONS",
    "SSE_HEARTBEAT_SECONDS",
    "SSE_QUEUE_MAX",
    "WEB_PORT",
    "DASHBOARD_GATEWAY_HOST",
    "DASHBOARD_GATEWAY_PORT",
    "DASHBOARD_GATEWAY_URL",
    "BENCHMARK_ALLOW_PORT_5432",
    "BENCHMARK_ALLOW_REMOTE",
)


def test_env_example_documents_every_variable() -> None:
    text = (ROOT / ".env.example").read_text()
    missing = [name for name in _EXTRA if name not in text]
    for field in Settings.model_fields.values():
        alias = field.alias
        if isinstance(alias, str) and alias not in text:
            missing.append(alias)
    assert missing == []

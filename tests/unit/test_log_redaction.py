"""Structured logs keep the event and remove secrets."""

from __future__ import annotations

import json
import logging

import pytest

from consistency_core.redact import redact_text
from consistency_worker import logs


def test_redact_text_strips_url_password_token_and_cookie() -> None:
    raw = (
        "postgresql+asyncpg://consistency:db-secret@127.0.0.1:5433/consistency "
        "password=hunter2 token=replay-secret-value "
        "ce_replay_capability=capability-secret Bearer capability-secret"
    )
    cleaned = redact_text(raw)
    assert "db-secret" not in cleaned
    assert "hunter2" not in cleaned
    assert "capability-secret" not in cleaned
    assert "***" in cleaned


def test_log_line_redacts_secrets(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("REPLAY_API_TOKEN", "replay-secret-value")
    logs.configure("INFO")
    logging.getLogger("consistency.test").info(
        "connect postgresql+asyncpg://consistency:db-secret@127.0.0.1:5433/consistency "
        "cookie ce_replay_capability=capability-secret using replay-secret-value",
        extra={
            "database_url": "postgresql+asyncpg://consistency:db-secret@127.0.0.1:5433/consistency",
            "password": "hunter2",
        },
    )
    line = capsys.readouterr().err.strip().splitlines()[-1]
    obj = json.loads(line)
    rendered = json.dumps(obj)
    assert obj["level"] == "INFO"
    assert "db-secret" not in rendered
    assert "hunter2" not in rendered
    assert "capability-secret" not in rendered
    assert "replay-secret-value" not in rendered
    assert obj["password"] == "***"
    assert obj["database_url"] == "***"

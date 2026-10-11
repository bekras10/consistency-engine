"""Redact secrets from structured logs. The public API does not use this to hide errors.

Error responses omit exception text entirely. This module is for log lines, where
the message is kept and the secret is replaced.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import UTC, datetime
from typing import Any

_URL_PASSWORD = re.compile(
    r"(?P<scheme>[a-z][a-z0-9+.-]*)://(?P<user>[^:/?#\s]+):(?P<password>[^@/?#\s]+)@"
)
_COOKIE = re.compile(r"ce_replay_capability=[^;\s]+")
_BEARER = re.compile(r"(?i)\bBearer\s+\S+")
_KV = re.compile(
    r"(?i)\b(password|passwd|token|secret|api_key|authorization|database_url)="
    r"[^\s,;&]+"
)
_SENSITIVE_KEYS = frozenset(
    {
        "password",
        "passwd",
        "token",
        "secret",
        "api_key",
        "authorization",
        "cookie",
        "set-cookie",
        "database_url",
        "replay_api_token",
    }
)
_RESERVED = frozenset(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}


def redact_text(value: str) -> str:
    """Replace passwords, tokens, capability cookies, and database URL passwords."""
    redacted = _URL_PASSWORD.sub(r"\g<scheme>://\g<user>:***@", value)
    redacted = _COOKIE.sub("ce_replay_capability=***", redacted)
    redacted = _BEARER.sub("Bearer ***", redacted)
    redacted = _KV.sub(r"\1=***", redacted)
    token = os.environ.get("REPLAY_API_TOKEN", "").strip()
    if len(token) >= 4 and token in redacted:
        redacted = redacted.replace(token, "***")
    return redacted


def redact(value: object) -> object:
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, dict):
        cleaned: dict[str, object] = {}
        for key, item in value.items():
            name = str(key)
            if name.lower() in _SENSITIVE_KEYS:
                cleaned[name] = "***"
            else:
                cleaned[name] = redact(item)
        return cleaned
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": redact_text(record.getMessage()),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            out[key] = "***" if key.lower() in _SENSITIVE_KEYS else redact(value)
        if record.exc_info:
            out["exc"] = redact_text(self.formatException(record.exc_info))
        return json.dumps(out, default=str, sort_keys=True)


def configure(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())

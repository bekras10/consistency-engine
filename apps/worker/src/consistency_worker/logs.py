"""Structured JSON logging (one object per line on stderr)."""

from consistency_core.redact import JsonFormatter, configure

__all__ = ["JsonFormatter", "configure"]

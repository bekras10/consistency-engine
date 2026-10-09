"""UTC conversions. Stream clocks are integer milliseconds; columns are timestamptz."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


def ms_to_dt(ms: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=ms)


def now_utc() -> datetime:
    return datetime.now(UTC)

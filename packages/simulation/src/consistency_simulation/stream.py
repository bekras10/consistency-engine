"""Playback of a recorded/generated message stream at a chosen speed."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence

from consistency_core.events import StreamMessage

ALLOWED_SPEEDS = (0.5, 1.0, 2.0, 5.0, 10.0)


def validate_speed(speed: float | None) -> float | None:
    """``None`` = as fast as possible; otherwise one of :data:`ALLOWED_SPEEDS`."""
    if speed is None:
        return None
    if speed not in ALLOWED_SPEEDS:
        raise ValueError(f"playback speed must be one of {ALLOWED_SPEEDS} (or None)")
    return speed


async def playback(
    messages: Sequence[StreamMessage],
    *,
    speed: float | None = None,
    start_position: int = 0,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> AsyncIterator[StreamMessage]:
    """Yield messages in recorded order, pacing by ``received_ts_ms`` deltas divided by speed.

    Pacing never changes content or order, so downstream results are speed-independent.
    """
    speed = validate_speed(speed)
    prev: int | None = None
    for msg in messages[start_position:]:
        if speed is not None and prev is not None:
            gap_ms = msg.received_ts_ms - prev
            if gap_ms > 0:
                await sleep(gap_ms / 1000.0 / speed)
        prev = msg.received_ts_ms
        yield msg

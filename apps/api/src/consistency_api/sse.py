"""Bounded SSE fan-out: connection cap, heartbeats, and a fixed queue."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable

DEFAULT_MAX_CONNECTIONS = 32
DEFAULT_HEARTBEAT_S = 15.0
DEFAULT_QUEUE_MAX = 32
DEFAULT_POLL_S = 0.05


def max_connections() -> int:
    return _positive_int("SSE_MAX_CONNECTIONS", DEFAULT_MAX_CONNECTIONS)


def heartbeat_seconds() -> float:
    raw = os.environ.get("SSE_HEARTBEAT_SECONDS", str(DEFAULT_HEARTBEAT_S))
    value = float(raw)
    if value <= 0:
        raise ValueError("SSE_HEARTBEAT_SECONDS must be positive")
    return value


def queue_max() -> int:
    return _positive_int("SSE_QUEUE_MAX", DEFAULT_QUEUE_MAX)


def _positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    value = default if raw is None or raw == "" else int(raw)
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


class StreamSlots:
    """How many event-stream responses this process will hold open."""

    def __init__(self, limit: int) -> None:
        if limit < 1:
            raise ValueError("connection limit must be positive")
        self.limit = limit
        self.open = 0
        self._lock = asyncio.Lock()

    async def try_acquire(self) -> bool:
        async with self._lock:
            if self.open >= self.limit:
                return False
            self.open += 1
            return True

    async def release(self) -> None:
        async with self._lock:
            if self.open > 0:
                self.open -= 1


def offer(queue: asyncio.Queue[str | None], item: str) -> bool:
    """Queue one frame. False means the consumer is too far behind."""
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        return False
    return True


async def run_producer(
    queue: asyncio.Queue[str | None],
    poll: Callable[[], Awaitable[list[str]]],
    *,
    heartbeat_s: float,
    poll_s: float,
    overflow: asyncio.Event,
) -> None:
    """Copy polled frames into ``queue`` until it is full or the task is cancelled.

    A full queue stops the producer. Frames that did not fit stay in the database
    and are delivered when the client reconnects from the last id it applied.
    Idle time emits an SSE comment so proxies do not treat the socket as dead.
    """
    loop = asyncio.get_running_loop()
    last_beat = loop.time()
    try:
        while True:
            frames = await poll()
            if frames:
                for frame in frames:
                    if not offer(queue, frame):
                        overflow.set()
                        return
                last_beat = loop.time()
            elif loop.time() - last_beat >= heartbeat_s:
                if not offer(queue, ": heartbeat\n\n"):
                    overflow.set()
                    return
                last_beat = loop.time()
            await asyncio.sleep(poll_s)
    finally:
        try:
            queue.put_nowait(None)
        except asyncio.QueueFull:
            overflow.set()


async def iter_queue(
    queue: asyncio.Queue[str | None], overflow: asyncio.Event
) -> AsyncIterator[str]:
    while True:
        if overflow.is_set() and queue.empty():
            break
        try:
            item = await asyncio.wait_for(queue.get(), timeout=0.2)
        except TimeoutError:
            if overflow.is_set() and queue.empty():
                break
            continue
        if item is None:
            break
        yield item

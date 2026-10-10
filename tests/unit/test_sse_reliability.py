"""A slow SSE consumer stops the producer instead of buffering without a bound."""

from __future__ import annotations

import asyncio

from consistency_api.sse import offer, run_producer


async def test_slow_consumer_stops_at_the_queue_cap() -> None:
    calls = 0

    async def poll() -> list[str]:
        nonlocal calls
        calls += 1
        return ["frame\n\n"] * 10

    queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=2)
    overflow = asyncio.Event()
    await run_producer(queue, poll, heartbeat_s=30, poll_s=0, overflow=overflow)
    assert overflow.is_set()
    assert queue.qsize() <= 2
    assert calls == 1
    assert offer(queue, "one-more\n\n") is False


async def test_idle_stream_emits_a_heartbeat_comment() -> None:
    async def poll() -> list[str]:
        return []

    queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=4)
    overflow = asyncio.Event()
    producer = asyncio.create_task(
        run_producer(queue, poll, heartbeat_s=0.01, poll_s=0.01, overflow=overflow)
    )
    item = await asyncio.wait_for(queue.get(), 1)
    producer.cancel()
    await asyncio.gather(producer, return_exceptions=True)
    assert item == ": heartbeat\n\n"
    assert overflow.is_set() is False

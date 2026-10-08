"""Async ingestion loop: source -> bounded inbound queue -> BookManager -> coalescing queue.

Failure handling:

* stream errors (``ConnectionError``/``OSError``/timeouts) -> books on the affected connection
  are marked UNSYNCHRONIZED and the subscription is retried with bounded exponential backoff;
* ``SourceAuthenticationError`` -> never retried; the runner stops in FAILED state;
* no message within ``heartbeat_timeout_s`` -> treated as a dead connection (same path);
* every recovery request from the manager is forwarded to ``source.request_recovery``.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field

from consistency_connectors.base import MarketDataSource, SourceAuthenticationError
from consistency_connectors.ingestion.book_manager import BookManager, BookUpdate, DesyncReason
from consistency_connectors.ingestion.queues import CoalescingQueue
from consistency_core.events import StreamMessage


@dataclass(frozen=True)
class Backoff:
    """Bounded exponential backoff with deterministic (seeded) jitter."""

    base_s: float = 0.5
    factor: float = 2.0
    max_s: float = 30.0
    max_attempts: int = 8
    jitter: float = 0.2
    seed: int = 0
    reset_after_messages: int = 50
    """A connection counts as healthy (attempt counter resets) only after this many messages,
    so a peer that accepts the subscription and then goes silent cannot retry forever."""

    def delays(self) -> list[float]:
        rng = random.Random(self.seed)
        out = []
        for attempt in range(self.max_attempts):
            d = min(self.max_s, self.base_s * self.factor**attempt)
            out.append(d * (1 + self.jitter * (2 * rng.random() - 1)))
        return out


class _EndOfStream:
    pass


@dataclass(frozen=True)
class _LostConnection:
    kind: str


_EOS = _EndOfStream()
type _Inbound = StreamMessage | _EndOfStream | _LostConnection


@dataclass
class RunnerStats:
    reconnect_attempts: int = 0
    heartbeat_timeouts: int = 0
    recovery_requests: int = 0
    messages: int = 0
    inbound_high_water: int = 0
    errors: list[str] = field(default_factory=list)


class IngestionRunner:
    def __init__(
        self,
        source: MarketDataSource,
        manager: BookManager,
        updates: CoalescingQueue[BookUpdate],
        *,
        inbound_maxsize: int = 10_000,
        heartbeat_timeout_s: float | None = 10.0,
        backoff: Backoff | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.source = source
        self.manager = manager
        self.updates = updates
        self.inbound: asyncio.Queue[_Inbound] = asyncio.Queue(inbound_maxsize)
        self.heartbeat_timeout_s = heartbeat_timeout_s
        self.backoff = backoff or Backoff()
        self._sleep = sleep
        self.stats = RunnerStats()
        self.failed: str | None = None
        self._last_connection: str | None = None

    async def run(self, market_ids: Sequence[str] | None = None) -> None:
        reader = asyncio.create_task(self._reader(market_ids))
        try:
            await self._consumer()
        finally:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)

    async def _reader(self, market_ids: Sequence[str] | None) -> None:
        delays = self.backoff.delays()
        attempt = 0
        while True:
            received = 0
            try:
                stream = self.source.subscribe_orderbooks(market_ids)
                while True:
                    if self.heartbeat_timeout_s is None:
                        msg = await anext(stream)
                    else:
                        msg = await asyncio.wait_for(anext(stream), self.heartbeat_timeout_s)
                    received += 1
                    if received >= self.backoff.reset_after_messages:
                        attempt = 0
                    await self.inbound.put(msg)
                    self.stats.inbound_high_water = max(
                        self.stats.inbound_high_water, self.inbound.qsize()
                    )
            except StopAsyncIteration:
                await self.inbound.put(_EOS)
                return
            except SourceAuthenticationError as exc:
                self.failed = f"authentication: {exc}"
                await self.inbound.put(_EOS)
                return
            except (TimeoutError, ConnectionError, OSError) as exc:
                if isinstance(exc, TimeoutError):
                    self.stats.heartbeat_timeouts += 1
                self.stats.errors.append(type(exc).__name__)
                await self.inbound.put(_LostConnection(type(exc).__name__))
                if attempt >= len(delays):
                    self.failed = "reconnect attempts exhausted"
                    await self.inbound.put(_EOS)
                    return
                self.stats.reconnect_attempts += 1
                await self._sleep(delays[attempt])
                attempt += 1

    async def _consumer(self) -> None:
        while True:
            item = await self.inbound.get()
            if isinstance(item, _EndOfStream):
                return
            if isinstance(item, _LostConnection):
                if self._last_connection is not None:
                    reason = (
                        DesyncReason.HEARTBEAT_TIMEOUT
                        if item.kind == "TimeoutError"
                        else DesyncReason.CONNECTION_LOST
                    )
                    self.manager.mark_connection_lost(self._last_connection, reason)
                continue
            self.stats.messages += 1
            conn = getattr(item.event, "connection_id", None)
            if conn is not None:
                self._last_connection = conn
            for update in self.manager.process(item):
                await self.updates.put(update.market_id, update)
            for req in self.manager.drain_recovery_requests():
                self.stats.recovery_requests += 1
                await self.source.request_recovery(req)

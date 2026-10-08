"""Async ingestion loop: source -> bounded inbound queue -> BookManager -> coalescing queue.

Failure handling. Every path that ends trust in a connection publishes an explicit
UNSYNCHRONIZED ``BookUpdate`` (kind ``"desync"``) for every affected market, without waiting for
any further market-data message, via the non-blocking ``CoalescingQueue.put_urgent``:

* stream errors (``ConnectionError``/``OSError``) -> ``CONNECTION_LOST``; the subscription is
  retried with bounded exponential backoff;
* no message within ``heartbeat_timeout_s`` -> ``HEARTBEAT_TIMEOUT`` (same retry path);
* the loss that exhausts the retry budget -> ``RECONNECT_EXHAUSTED``; the runner stops FAILED;
* ``SourceAuthenticationError`` -> ``AUTHENTICATION_FAILED``; never retried, stops FAILED;
* end of stream of a live source (``stream_is_finite`` false) -> ``END_OF_STREAM``, stops FAILED
  (the end of a finite recording is expected and does not desync);
* any other exception from the source -> ``SOURCE_ERROR``, stops FAILED (never hangs);
* cancellation of :meth:`IngestionRunner.run` -> ``RUNNER_STOPPED``.

Every recovery request from the manager is forwarded to ``source.request_recovery``. Updates are
always coalesced with :func:`merge_book_updates`, so a desync superseded by a resync before the
consumer read it surfaces as ``interrupted=True`` (see ``queues``).
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field

from consistency_connectors.base import MarketDataSource, SourceAuthenticationError
from consistency_connectors.ingestion.book_manager import BookManager, BookUpdate, DesyncReason
from consistency_connectors.ingestion.queues import CoalescingQueue, merge_book_updates
from consistency_core.events import ConnectionEvent, StreamMessage


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
    reason: str


_EOS = _EndOfStream()
type _Inbound = StreamMessage | _EndOfStream | _LostConnection


@dataclass
class RunnerStats:
    reconnect_attempts: int = 0
    heartbeat_timeouts: int = 0
    recovery_requests: int = 0
    messages: int = 0
    inbound_high_water: int = 0
    desyncs_published: int = 0
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
        self._connections: set[str] = set()
        """Connections seen on the current subscription; all are lost together."""

    async def run(self, market_ids: Sequence[str] | None = None) -> None:
        reader = asyncio.create_task(self._reader(market_ids))
        try:
            await self._consumer()
        except asyncio.CancelledError:
            self._publish_loss(DesyncReason.RUNNER_STOPPED)
            raise
        finally:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)

    async def _fail(self, failed: str, reason: str) -> None:
        self.failed = failed
        await self.inbound.put(_LostConnection(reason))
        await self.inbound.put(_EOS)

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
                if self.source.stream_is_finite:
                    await self.inbound.put(_EOS)
                else:
                    await self._fail("unexpected end of stream", DesyncReason.END_OF_STREAM)
                return
            except SourceAuthenticationError as exc:
                await self._fail(f"authentication: {exc}", DesyncReason.AUTHENTICATION_FAILED)
                return
            except (TimeoutError, ConnectionError, OSError) as exc:
                if isinstance(exc, TimeoutError):
                    self.stats.heartbeat_timeouts += 1
                self.stats.errors.append(type(exc).__name__)
                if attempt >= len(delays):
                    await self._fail(
                        "reconnect attempts exhausted", DesyncReason.RECONNECT_EXHAUSTED
                    )
                    return
                await self.inbound.put(
                    _LostConnection(
                        DesyncReason.HEARTBEAT_TIMEOUT
                        if isinstance(exc, TimeoutError)
                        else DesyncReason.CONNECTION_LOST
                    )
                )
                self.stats.reconnect_attempts += 1
                await self._sleep(delays[attempt])
                attempt += 1
            except Exception as exc:  # any other source failure must not hang the consumer
                self.stats.errors.append(type(exc).__name__)
                await self._fail(
                    f"source error: {type(exc).__name__}: {exc}", DesyncReason.SOURCE_ERROR
                )
                return

    def _publish_loss(self, reason: str) -> None:
        for conn in sorted(self._connections):
            for update in self.manager.connection_lost(conn, reason):
                self.updates.put_urgent(update.market_id, update, merge=merge_book_updates)
                self.stats.desyncs_published += 1
        self._connections.clear()

    async def _consumer(self) -> None:
        while True:
            item = await self.inbound.get()
            if isinstance(item, _EndOfStream):
                return
            if isinstance(item, _LostConnection):
                self._publish_loss(item.reason)
                continue
            self.stats.messages += 1
            conn = getattr(item.event, "connection_id", None)
            if isinstance(item.event, ConnectionEvent) and item.event.state == "disconnected":
                self._connections.discard(item.event.connection_id)  # handled by the manager
            elif conn is not None:
                self._connections.add(conn)
            for update in self.manager.process(item):
                await self.updates.put(update.market_id, update, merge=merge_book_updates)
            for req in self.manager.drain_recovery_requests():
                self.stats.recovery_requests += 1
                await self.source.request_recovery(req)

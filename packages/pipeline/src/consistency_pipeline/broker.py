"""In-process async publish/subscribe (no Redis/Kafka; spec section 2).

The worker publishes detection lifecycle events, market sync/top-of-book changes and system
notices; Phase 11's SSE endpoint subscribes. Every message gets a broker-wide monotonically
increasing ``seq`` (usable as an SSE ``id`` for resumption).

Backpressure: ``publish`` never blocks the pipeline. Each subscriber has a bounded queue; when it
is full the *oldest* queued message is dropped, the subscriber's ``dropped`` counter grows and
the next delivered message carries ``lagged=True`` so the client knows to resynchronize from the
database (REST) instead of trusting a gapless stream. Detection state is persisted before it is
published, so a lagging subscriber never loses durable state.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

DEFAULT_SUBSCRIBER_MAXSIZE = 1_000


@dataclass(frozen=True)
class BrokerMessage:
    seq: int
    topic: str
    payload: dict[str, Any]
    lagged: bool = False


@dataclass(eq=False)
class Subscription:
    topics: frozenset[str] | None
    maxsize: int
    _broker: Broker
    _queue: deque[BrokerMessage] = field(default_factory=deque)
    _waiter: asyncio.Future[None] | None = None
    dropped: int = 0
    _lagged: bool = False
    closed: bool = False

    def wants(self, topic: str) -> bool:
        return self.topics is None or topic in self.topics

    def _offer(self, msg: BrokerMessage) -> None:
        if len(self._queue) >= self.maxsize:
            self._queue.popleft()
            self.dropped += 1
            self._lagged = True
        self._queue.append(msg)
        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_result(None)

    async def get(self) -> BrokerMessage:
        while not self._queue:
            if self.closed:
                raise StopAsyncIteration
            self._waiter = asyncio.get_running_loop().create_future()
            await self._waiter
        msg = self._queue.popleft()
        if self._lagged:
            self._lagged = False
            msg = BrokerMessage(msg.seq, msg.topic, msg.payload, lagged=True)
        return msg

    def get_nowait(self) -> BrokerMessage | None:
        if not self._queue:
            return None
        msg = self._queue.popleft()
        if self._lagged:
            self._lagged = False
            msg = BrokerMessage(msg.seq, msg.topic, msg.payload, lagged=True)
        return msg

    def close(self) -> None:
        self.closed = True
        self._broker._subs.discard(self)
        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_result(None)

    def __aiter__(self) -> AsyncIterator[BrokerMessage]:
        return self

    async def __anext__(self) -> BrokerMessage:
        return await self.get()


class Broker:
    def __init__(self) -> None:
        self._subs: set[Subscription] = set()
        self.seq = 0
        self.published = 0

    def subscribe(
        self, topics: set[str] | None = None, *, maxsize: int = DEFAULT_SUBSCRIBER_MAXSIZE
    ) -> Subscription:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        sub = Subscription(None if topics is None else frozenset(topics), maxsize, self)
        self._subs.add(sub)
        return sub

    @property
    def subscriber_count(self) -> int:
        return len(self._subs)

    def publish(self, topic: str, payload: dict[str, Any]) -> BrokerMessage:
        self.seq += 1
        self.published += 1
        msg = BrokerMessage(self.seq, topic, payload)
        for sub in list(self._subs):
            if sub.wants(topic):
                sub._offer(msg)
        return msg

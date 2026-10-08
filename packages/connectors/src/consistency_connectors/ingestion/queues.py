"""Bounded queues for backpressure.

* Inbound raw messages use a plain bounded ``asyncio.Queue``: when full, the reader awaits
  (backpressure to the socket) instead of growing memory. Messages are never dropped, because
  dropping would silently corrupt sequence tracking.
* Book-change notifications to the detection engine use :class:`CoalescingQueue`: only the
  latest notification per market is kept, so the queue is bounded by the number of markets and
  a slow consumer sees the newest state rather than a backlog of obsolete ones.

Desync semantics (nothing that reports a loss of trust can be dropped or hidden):

* Updates are only ever replaced by *newer* updates for the same key, in processing order, so a
  pending UNSYNCHRONIZED notification can only be superseded by a later state of that market,
  never by an older "synchronized" one.
* ``merge`` (per queue or per ``put``) decides what the surviving value is when coalescing. The
  ingestion runner always passes :func:`merge_book_updates`, which marks a synchronized update
  that replaced a pending desync as ``interrupted=True``, so the outage stays visible even when
  the consumer only sees the resynchronized state.
* :meth:`CoalescingQueue.put_urgent` never blocks and never drops: when the queue is full it
  admits the item anyway (``stats.urgent_overflow``). It is used for desync notifications, which
  are bounded by the number of markets, so the overflow is bounded too.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

from consistency_core.models import SyncStatus

if TYPE_CHECKING:
    from consistency_connectors.ingestion.book_manager import BookUpdate

T = TypeVar("T")


@dataclass
class QueueStats:
    put: int = 0
    coalesced: int = 0
    got: int = 0
    high_water: int = 0
    urgent: int = 0
    urgent_overflow: int = 0


def merge_book_updates(old: BookUpdate, new: BookUpdate) -> BookUpdate:
    """Newest update wins, but a superseded desync (or interruption) is carried forward."""
    if new.interrupted or not (old.interrupted or old.sync_status is SyncStatus.UNSYNCHRONIZED):
        return new
    return dataclasses.replace(new, interrupted=True)


class CoalescingQueue[T]:
    """Keyed latest-value queue with FIFO order of first pending arrival."""

    def __init__(self, maxsize: int, *, merge: Callable[[T, T], T] | None = None) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self.maxsize = maxsize
        self._merge = merge
        self._items: OrderedDict[str, T] = OrderedDict()
        self._getters: deque[asyncio.Future[None]] = deque()
        self._putters: deque[asyncio.Future[None]] = deque()
        self.stats = QueueStats()

    def __len__(self) -> int:
        return len(self._items)

    @staticmethod
    def _wake(waiters: deque[asyncio.Future[None]]) -> None:
        while waiters:
            w = waiters.popleft()
            if not w.done():
                w.set_result(None)

    def _store(self, key: str, item: T, merge: Callable[[T, T], T] | None) -> None:
        if key in self._items:
            fn = merge or self._merge
            old = self._items[key]
            self._items[key] = fn(old, item) if fn is not None else item  # keep position
            self.stats.coalesced += 1
        else:
            self._items[key] = item
        self.stats.put += 1
        self.stats.high_water = max(self.stats.high_water, len(self._items))
        self._wake(self._getters)

    async def put(self, key: str, item: T, *, merge: Callable[[T, T], T] | None = None) -> None:
        """Blocks only when ``key`` is new and the queue is full."""
        while key not in self._items and len(self._items) >= self.maxsize:
            fut = asyncio.get_running_loop().create_future()
            self._putters.append(fut)
            await fut
        self._store(key, item, merge)

    def put_urgent(self, key: str, item: T, *, merge: Callable[[T, T], T] | None = None) -> None:
        """Never blocks or drops; may exceed ``maxsize`` (counted in ``stats.urgent_overflow``)."""
        if key not in self._items and len(self._items) >= self.maxsize:
            self.stats.urgent_overflow += 1
        self.stats.urgent += 1
        self._store(key, item, merge)

    async def get(self) -> tuple[str, T]:
        while not self._items:
            fut = asyncio.get_running_loop().create_future()
            self._getters.append(fut)
            await fut
        return self._pop()

    def get_nowait(self) -> tuple[str, T] | None:
        if not self._items:
            return None
        return self._pop()

    def _pop(self) -> tuple[str, T]:
        key, item = self._items.popitem(last=False)
        self.stats.got += 1
        self._wake(self._putters)
        return key, item

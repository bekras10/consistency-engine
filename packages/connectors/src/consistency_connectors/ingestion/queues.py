"""Bounded queues for backpressure.

* Inbound raw messages use a plain bounded ``asyncio.Queue``: when full, the reader awaits
  (backpressure to the socket) instead of growing memory. Messages are never dropped, because
  dropping would silently corrupt sequence tracking.
* Book-change notifications to the detection engine use :class:`CoalescingQueue`: only the
  latest notification per market is kept, so the queue is bounded by the number of markets and
  a slow consumer sees the newest state rather than a backlog of obsolete ones.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass
class QueueStats:
    put: int = 0
    coalesced: int = 0
    got: int = 0
    high_water: int = 0


class CoalescingQueue[T]:
    """Keyed latest-value queue with FIFO order of first pending arrival."""

    def __init__(self, maxsize: int) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self.maxsize = maxsize
        self._items: OrderedDict[str, T] = OrderedDict()
        self._cond = asyncio.Condition()
        self.stats = QueueStats()

    def __len__(self) -> int:
        return len(self._items)

    async def put(self, key: str, item: T) -> None:
        async with self._cond:
            if key in self._items:
                self._items[key] = item  # keep original position, newest value
                self.stats.coalesced += 1
            else:
                while len(self._items) >= self.maxsize:
                    await self._cond.wait()
                self._items[key] = item
            self.stats.put += 1
            self.stats.high_water = max(self.stats.high_water, len(self._items))
            self._cond.notify_all()

    async def get(self) -> tuple[str, T]:
        async with self._cond:
            while not self._items:
                await self._cond.wait()
            key, item = self._items.popitem(last=False)
            self.stats.got += 1
            self._cond.notify_all()
            return key, item

    def get_nowait(self) -> tuple[str, T] | None:
        if not self._items:
            return None
        key, item = self._items.popitem(last=False)
        self.stats.got += 1
        return key, item

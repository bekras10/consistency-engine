"""SyntheticDataSource: runs the deterministic synthetic exchange and streams its output.

Milestone-1 note: the session is simulated up-front (fast: well under a second per simulated
minute for the default families) and then played back at the requested speed. Exchange-side
truth is recorded per tick so :meth:`get_market_orderbook` can answer REST-style queries for
the current playback time.
"""

from __future__ import annotations

import asyncio
from bisect import bisect_right
from collections.abc import Awaitable, Callable

from consistency_connectors.sources.recorded import RecordedStreamSource
from consistency_core.events import LevelPair, OrderBookSnapshotEvent
from consistency_core.models.common import DataSourceKind, Side
from consistency_simulation.datasets import PRESETS
from consistency_simulation.exchange import SessionConfig, SessionResult, SyntheticExchange
from consistency_simulation.families import SIM_EPOCH_MS

_Levels = tuple[tuple[LevelPair, ...], tuple[LevelPair, ...]]


class SyntheticDataSource(RecordedStreamSource):
    def __init__(
        self,
        config: SessionConfig | str = "smoke",
        *,
        speed: float | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        cfg = PRESETS[config] if isinstance(config, str) else config
        self.config = cfg
        self._truth: dict[str, tuple[list[int], list[_Levels]]] = {}
        exchange = SyntheticExchange(cfg)
        self.result: SessionResult = exchange.run(observer=self._record_truth)
        super().__init__(
            DataSourceKind.SYNTHETIC,
            self.result.catalog,
            self.result.messages,
            speed=speed,
            sleep=sleep,
        )

    def _record_truth(self, t: int, ex: SyntheticExchange) -> None:
        for rt in ex.runtimes:
            for mid, book in rt.books.items():
                state: _Levels = (tuple(book.levels(Side.YES)), tuple(book.levels(Side.NO)))
                times, states = self._truth.setdefault(mid, ([], []))
                if not states or states[-1] != state:
                    times.append(SIM_EPOCH_MS + t)
                    states.append(state)

    async def get_market_orderbook(self, market_id: str) -> OrderBookSnapshotEvent:
        """Exchange-side truth at the emission time of the latest delivered message.

        REST-style: ``sid="rest"``, ``seq=0``; it is not sequence-aligned with the stream.
        """
        if market_id not in self._truth:
            raise LookupError(market_id)
        now = self.messages[self.position].emitted_ts_ms if self.position >= 0 else SIM_EPOCH_MS
        times, states = self._truth[market_id]
        i = max(0, bisect_right(times, now) - 1)
        yes, no = states[i]
        return OrderBookSnapshotEvent(
            market_id=market_id,
            sid="rest",
            seq=0,
            connection_id="rest",
            exchange_ts_ms=times[i],
            yes_bids=yes,
            no_bids=no,
        )

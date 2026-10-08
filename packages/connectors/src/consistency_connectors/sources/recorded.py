"""Shared implementation for sources backed by an ordered message list."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence

from consistency_connectors.base import (
    ConnectionHealth,
    ConnectionState,
    MarketDataSource,
    RecoveryRequest,
)
from consistency_core.events import OrderBookSnapshotEvent, StreamMessage
from consistency_core.models.common import DataSourceKind
from consistency_core.models.market import Catalog, Event, Market, MarketRules, Series
from consistency_simulation.stream import playback, validate_speed


class RecordedStreamSource(MarketDataSource):
    """Plays an ordered stream. Recovery is *in-stream*: recordings made by a correct client
    already contain the resubscription snapshots that followed each detectable fault, so
    :meth:`request_recovery` only records the request (exposed via health/stats)."""

    stream_is_finite = True

    def __init__(
        self,
        kind: DataSourceKind,
        catalog: Catalog,
        messages: Sequence[StreamMessage],
        *,
        speed: float | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.kind = kind
        self.catalog = catalog
        self.messages = list(messages)
        self.speed = validate_speed(speed)
        self._sleep = sleep
        self.position = -1
        self.recovery_requests: list[RecoveryRequest] = []
        self.unsubscribed: set[str] = set()
        self._state = ConnectionState.IDLE

    async def get_series(self) -> Sequence[Series]:
        return self.catalog.series

    async def get_events(self) -> Sequence[Event]:
        return self.catalog.events

    async def get_markets(self) -> Sequence[Market]:
        return self.catalog.markets

    async def get_market_rules(self, market_id: str) -> MarketRules:
        return self.catalog.market(market_id).rules

    async def get_market_orderbook(self, market_id: str) -> OrderBookSnapshotEvent:
        """Latest snapshot for the market delivered at or before the current position."""
        upto = self.position if self.position >= 0 else len(self.messages) - 1
        for msg in reversed(self.messages[: upto + 1]):
            ev = msg.event
            if isinstance(ev, OrderBookSnapshotEvent) and ev.market_id == market_id:
                return ev
        raise LookupError(f"no snapshot recorded for {market_id} up to position {upto}")

    async def subscribe_orderbooks(
        self, market_ids: Sequence[str] | None = None
    ) -> AsyncIterator[StreamMessage]:
        """Yields the full stream: sequence numbers are per subscription, so filtering messages
        by market would fabricate gaps. Consumers restrict markets via the BookManager catalog."""
        del market_ids
        self._state = ConnectionState.CONNECTED
        async for msg in playback(
            self.messages, speed=self.speed, start_position=self.position + 1, sleep=self._sleep
        ):
            self.position = msg.position
            yield msg
        self._state = ConnectionState.DISCONNECTED

    async def unsubscribe_orderbooks(self, market_ids: Sequence[str]) -> None:
        self.unsubscribed.update(market_ids)

    async def request_recovery(self, request: RecoveryRequest) -> None:
        self.recovery_requests.append(request)

    def get_connection_health(self) -> ConnectionHealth:
        last = self.messages[self.position] if 0 <= self.position < len(self.messages) else None
        return ConnectionHealth(
            source_kind=self.kind,
            state=self._state,
            messages_delivered=self.position + 1,
            last_message_received_ts_ms=None if last is None else last.received_ts_ms,
            detail=f"{len(self.recovery_requests)} recovery requests (served in-stream)",
        )

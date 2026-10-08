"""KalshiDataSource — authorization-gated skeleton (full read-only connector: milestone 4).

This module performs **no network I/O** and imports no HTTP/WebSocket client. Construction is
refused unless ``ENABLE_KALSHI_API`` and ``KALSHI_AUTHORIZATION_CONFIRMED`` are both true; even
then every data method raises ``NotImplementedError`` until the connector milestone lands with
offline contract tests against the official schemas. Status: "Implemented, not activated" will
only be claimed once that work exists; today it is "skeleton, not activated".
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import NoReturn

from consistency_connectors.base import (
    ConnectionHealth,
    ConnectionState,
    MarketDataSource,
    RecoveryRequest,
)
from consistency_connectors.settings import Settings
from consistency_core.events import OrderBookSnapshotEvent, StreamMessage
from consistency_core.models.common import DataSourceKind
from consistency_core.models.market import Event, Market, MarketRules, Series


class KalshiAccessRefusedError(PermissionError):
    pass


_NOT_YET = (
    "The authorized Kalshi connector is a milestone-4 deliverable; this skeleton performs no "
    "network access."
)


class KalshiDataSource(MarketDataSource):
    kind = DataSourceKind.KALSHI_AUTHORIZED

    def __init__(self, settings: Settings) -> None:
        if not settings.kalshi_access_allowed:
            raise KalshiAccessRefusedError(
                "Kalshi access refused: requires ENABLE_KALSHI_API=true and "
                "KALSHI_AUTHORIZATION_CONFIRMED=true, and the operator must hold the required "
                "authorization under Kalshi's Developer Agreement."
            )
        if settings.enable_live_trading:  # pragma: no cover - Settings already refuses this
            raise KalshiAccessRefusedError("live trading is out of scope")
        self.settings = settings

    def _refuse(self) -> NoReturn:
        raise NotImplementedError(_NOT_YET)

    async def get_series(self) -> Sequence[Series]:
        self._refuse()

    async def get_events(self) -> Sequence[Event]:
        self._refuse()

    async def get_markets(self) -> Sequence[Market]:
        self._refuse()

    async def get_market_rules(self, market_id: str) -> MarketRules:
        self._refuse()

    async def get_market_orderbook(self, market_id: str) -> OrderBookSnapshotEvent:
        self._refuse()

    def subscribe_orderbooks(
        self, market_ids: Sequence[str] | None = None
    ) -> AsyncIterator[StreamMessage]:
        self._refuse()

    async def unsubscribe_orderbooks(self, market_ids: Sequence[str]) -> None:
        self._refuse()

    async def request_recovery(self, request: RecoveryRequest) -> None:
        self._refuse()

    def get_connection_health(self) -> ConnectionHealth:
        return ConnectionHealth(
            source_kind=self.kind,
            state=ConnectionState.DISABLED,
            detail="skeleton only; not activated",
        )

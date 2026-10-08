"""The exchange-agnostic data-source interface (spec 4.1).

Core business logic depends only on this interface and on core models; it never knows which
concrete source supplies the data.
"""

from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Sequence
from enum import StrEnum

from consistency_core.events import OrderBookSnapshotEvent, StreamMessage
from consistency_core.models.common import DataSourceKind, FrozenModel
from consistency_core.models.market import Event, Market, MarketRules, Series


class ConnectionState(StrEnum):
    IDLE = "idle"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"
    DISABLED = "disabled"


class ConnectionHealth(FrozenModel):
    source_kind: DataSourceKind
    state: ConnectionState
    connection_id: str | None = None
    messages_delivered: int = 0
    reconnects: int = 0
    last_message_received_ts_ms: int | None = None
    detail: str = ""


class RecoveryRequest(FrozenModel):
    """Ask the source to re-establish a trustworthy book (resubscribe -> fresh snapshots)."""

    connection_id: str | None
    market_ids: tuple[str, ...]
    reason: str


class SourceAuthenticationError(RuntimeError):
    """Credentials rejected. Never retried automatically."""


class MarketDataSource(abc.ABC):
    kind: DataSourceKind

    @abc.abstractmethod
    async def get_series(self) -> Sequence[Series]: ...

    @abc.abstractmethod
    async def get_events(self) -> Sequence[Event]: ...

    @abc.abstractmethod
    async def get_markets(self) -> Sequence[Market]: ...

    @abc.abstractmethod
    async def get_market_rules(self, market_id: str) -> MarketRules: ...

    @abc.abstractmethod
    async def get_market_orderbook(self, market_id: str) -> OrderBookSnapshotEvent:
        """A REST-style point-in-time book. Not sequence-aligned with any stream: it must not
        be used to resynchronize a streamed book (use :meth:`request_recovery`)."""

    @abc.abstractmethod
    def subscribe_orderbooks(
        self, market_ids: Sequence[str] | None = None
    ) -> AsyncIterator[StreamMessage]:
        """Stream of order-book (and connection/status) messages."""

    @abc.abstractmethod
    async def unsubscribe_orderbooks(self, market_ids: Sequence[str]) -> None: ...

    @abc.abstractmethod
    async def request_recovery(self, request: RecoveryRequest) -> None:
        """Resubscribe the given markets so that fresh snapshots follow on the stream."""

    @abc.abstractmethod
    def get_connection_health(self) -> ConnectionHealth: ...

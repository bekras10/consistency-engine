"""Exchange-agnostic market-data stream events.

Sequence numbers are scoped to a *subscription* (``sid``), not to a market, mirroring the
documented Kalshi WebSocket behaviour where one subscription may carry several markets and
``seq`` increments per message on that subscription. Consumers must never assume per-market
sequence spaces.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field

from consistency_core.models.common import FrozenModel, MarketStatus, Side
from consistency_core.models.market import Market
from consistency_core.money import Dec

LevelPair = tuple[Dec, Dec]
"""(price, quantity) as parsed numbers, before grid/market validation."""


class OrderBookSnapshotEvent(FrozenModel):
    type: Literal["orderbook_snapshot"] = "orderbook_snapshot"
    market_id: str
    sid: str
    seq: int
    connection_id: str
    exchange_ts_ms: int | None = None
    yes_bids: tuple[LevelPair, ...] = ()
    no_bids: tuple[LevelPair, ...] = ()


class OrderBookDeltaEvent(FrozenModel):
    type: Literal["orderbook_delta"] = "orderbook_delta"
    market_id: str
    sid: str
    seq: int
    connection_id: str
    exchange_ts_ms: int | None = None
    side: Side
    price: Dec
    delta: Dec
    """Signed change in resting quantity at (side, price)."""


class MarketStatusEvent(FrozenModel):
    type: Literal["market_status"] = "market_status"
    market_id: str
    status: MarketStatus
    exchange_ts_ms: int | None = None


class MarketLifecycleEvent(FrozenModel):
    type: Literal["market_lifecycle"] = "market_lifecycle"
    action: Literal["created", "removed"]
    market_id: str
    market: Market | None = None
    exchange_ts_ms: int | None = None


class ConnectionEvent(FrozenModel):
    type: Literal["connection"] = "connection"
    state: Literal["connected", "disconnected"]
    connection_id: str


class HeartbeatEvent(FrozenModel):
    type: Literal["heartbeat"] = "heartbeat"
    connection_id: str


StreamEvent = Annotated[
    OrderBookSnapshotEvent
    | OrderBookDeltaEvent
    | MarketStatusEvent
    | MarketLifecycleEvent
    | ConnectionEvent
    | HeartbeatEvent,
    Field(discriminator="type"),
]


class StreamMessage(FrozenModel):
    """One message as delivered to the client.

    ``emitted_ts_ms`` is exchange-side time; ``received_ts_ms`` is local receipt time (includes
    simulated network delay). ``synthetic_tag`` is a ground-truth label written by the
    simulator for tests and reports; the ingestion engine never reads it.
    """

    position: int
    emitted_ts_ms: int
    received_ts_ms: int
    event: StreamEvent
    synthetic_tag: str | None = None

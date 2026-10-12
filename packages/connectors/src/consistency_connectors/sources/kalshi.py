"""Authorized read-only Kalshi source.

Construction is refused unless both enable flags are set. Data methods also
require a key id and a private-key file, and they still only issue documented
GET requests plus the orderbook WebSocket. No order, position, or balance call
exists on this object.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import cast

from consistency_connectors.base import (
    ConnectionHealth,
    ConnectionState,
    MarketDataSource,
    RecoveryRequest,
    SourceAuthenticationError,
)
from consistency_connectors.ingestion.book_manager import BookManager
from consistency_connectors.kalshi.auth import KalshiCredentialsError
from consistency_connectors.kalshi.mapping import CatalogLoadError, load_catalog
from consistency_connectors.kalshi.rest import HttpTransport, KalshiRestClient
from consistency_connectors.kalshi.stream import OrderBookStream, WsTransport
from consistency_connectors.settings import Settings
from consistency_core.events import OrderBookSnapshotEvent, StreamMessage
from consistency_core.models.common import DataSourceKind, MarketStatus
from consistency_core.models.market import Catalog, Event, Market, MarketRules, Series
from consistency_core.models.orderbook import BookValidationError
from consistency_core.normalization import normalize_rest_orderbook
from consistency_core.ticks import PriceGrid


class KalshiAccessRefusedError(PermissionError):
    pass


_STATE = {
    "idle": ConnectionState.IDLE,
    "connecting": ConnectionState.RECONNECTING,
    "connected": ConnectionState.CONNECTED,
    "disconnected": ConnectionState.DISCONNECTED,
    "authentication_failed": ConnectionState.FAILED,
}


class KalshiDataSource(MarketDataSource):
    kind = DataSourceKind.KALSHI_AUTHORIZED
    stream_is_finite = False

    def __init__(
        self,
        settings: Settings,
        *,
        transport: HttpTransport | None = None,
        ws_transport: WsTransport | None = None,
        clock_ms: object | None = None,
        sleep: object | None = None,
    ) -> None:
        if not settings.kalshi_access_allowed:
            raise KalshiAccessRefusedError(
                "Kalshi access refused: requires ENABLE_KALSHI_API=true and "
                "KALSHI_AUTHORIZATION_CONFIRMED=true, and the operator must hold the required "
                "authorization under Kalshi's Developer Agreement."
            )
        if settings.enable_live_trading:
            raise KalshiAccessRefusedError("live trading is out of scope")
        self.settings = settings
        clock = cast(Callable[[], int] | None, clock_ms if callable(clock_ms) else None)
        sleeper = cast(
            Callable[[float], Awaitable[None]] | None, sleep if callable(sleep) else None
        )
        self._rest = KalshiRestClient(
            settings,
            transport=transport,
            clock_ms=clock,
            sleep=sleeper,
        )
        self._clock = self._rest.clock_ms
        self._catalog: Catalog | None = None
        self._skipped: list[str] = []
        self._pages = 0
        self._loaded_at: datetime | None = None
        self._stream: OrderBookStream | None = None
        self._ws_transport = ws_transport
        self._sleep = sleeper
        self._last_compare_ms = 0
        self._last_refresh_ms = 0
        self._compare_lock = asyncio.Lock()

    def _credentials_error(self, exc: KalshiCredentialsError) -> KalshiAccessRefusedError:
        return KalshiAccessRefusedError(str(exc))

    async def _ensure(self) -> Catalog:
        if self._catalog is not None:
            return self._catalog
        try:
            self._rest.require_credentials()
            catalog, skipped, pages = await load_catalog(self._rest, self.settings)
        except KalshiCredentialsError as exc:
            raise self._credentials_error(exc) from None
        except CatalogLoadError as exc:
            raise KalshiAccessRefusedError(str(exc)) from None
        self._catalog = catalog
        self._skipped = skipped
        self._pages = pages
        self._loaded_at = datetime.now(UTC)
        return catalog

    def _stream_or_new(self) -> OrderBookStream:
        if self._stream is None:
            try:
                signer = self._rest.signer()
            except KalshiCredentialsError as exc:
                raise self._credentials_error(exc) from None
            if signer is None:
                raise KalshiAccessRefusedError("Kalshi credentials are not configured.")
            self._stream = OrderBookStream(
                self.settings,
                signer=signer,
                transport=self._ws_transport,
                clock_ms=self._clock,
                sleep=self._sleep,
            )
        return self._stream

    async def get_series(self) -> Sequence[Series]:
        return (await self._ensure()).series

    async def get_events(self) -> Sequence[Event]:
        return (await self._ensure()).events

    async def get_markets(self) -> Sequence[Market]:
        return (await self._ensure()).markets

    async def get_market_rules(self, market_id: str) -> MarketRules:
        catalog = await self._ensure()
        return catalog.market(market_id).rules

    async def get_market_orderbook(self, market_id: str) -> OrderBookSnapshotEvent:
        """A point-in-time REST book. It is not a sequenced stream snapshot."""
        catalog = await self._ensure()
        market = catalog.market(market_id)
        body = await self._rest.get_json(f"/markets/{market.ticker}/orderbook")
        return _snapshot_from_rest(market.market_id, market.price_grid, body)

    def subscribe_orderbooks(
        self, market_ids: Sequence[str] | None = None
    ) -> AsyncIterator[StreamMessage]:
        tickers = list(market_ids or ())
        if not tickers and self._catalog is not None:
            tickers = [
                market.market_id
                for market in self._catalog.markets
                if market.status is MarketStatus.OPEN
            ]
        if not tickers:
            raise KalshiAccessRefusedError("No open Kalshi markets are selected.")

        async def _iterate() -> AsyncIterator[StreamMessage]:
            stream = self._stream_or_new()
            async for message in stream.messages(tickers):
                yield message

        return _iterate()

    async def unsubscribe_orderbooks(self, market_ids: Sequence[str]) -> None:
        stream = self._stream
        if stream is not None:
            await stream.unsubscribe(list(market_ids))

    async def request_recovery(self, request: RecoveryRequest) -> None:
        stream = self._stream
        if stream is None:
            raise ConnectionError("Kalshi order book is not subscribed.")
        await stream.request_recovery(request)

    def get_connection_health(self) -> ConnectionHealth:
        stream = self._stream
        if stream is None:
            return ConnectionHealth(
                source_kind=self.kind,
                state=ConnectionState.IDLE,
                detail="authorized; not connected",
            )
        return ConnectionHealth(
            source_kind=self.kind,
            state=_STATE.get(stream.state, ConnectionState.DISCONNECTED),
            connection_id=stream._connection_id or None,
            messages_delivered=stream.message_count,
            reconnects=stream.reconnects,
            detail=stream.state,
        )

    def feed_status(self) -> dict[str, object]:
        stream = self._stream.status() if self._stream is not None else {"state": "idle"}
        markets = 0 if self._catalog is None else len(self._catalog.markets)
        return {
            "state": stream.get("state", "idle"),
            "environment": self.settings.kalshi_environment,
            "markets_selected": markets,
            "discovery_pages": self._pages,
            "skipped": self._skipped,
            "raw_retention_authorized": self.settings.third_party_raw_persistence_authorized,
            "replay_available": False,
            "loaded_at": None if self._loaded_at is None else self._loaded_at.isoformat(),
            "stream": stream,
        }

    async def consider_maintenance(self, manager: BookManager) -> None:
        """Compare REST books and refresh rules. REST snapshots are not applied."""
        if self._catalog is None or self._stream is None:
            return
        async with self._compare_lock:
            now = self._clock()
            if self.settings.kalshi_book_compare_s > 0 and now - self._last_compare_ms >= int(
                self.settings.kalshi_book_compare_s * 1000
            ):
                self._last_compare_ms = now
                await self._compare(manager)
            if self.settings.kalshi_metadata_refresh_s > 0 and now - self._last_refresh_ms >= int(
                self.settings.kalshi_metadata_refresh_s * 1000
            ):
                self._last_refresh_ms = now
                await self._refresh_rules()

    async def _compare(self, manager: BookManager) -> None:
        assert self._catalog is not None
        stream = self._stream
        if stream is None:
            return
        open_ids = [
            market.market_id
            for market in self._catalog.markets
            if market.status is MarketStatus.OPEN and manager.has_market(market.market_id)
        ]
        batch = self.settings.kalshi_orderbook_batch_size
        for start in range(0, len(open_ids), batch):
            chunk = open_ids[start : start + batch]
            books = await self._orderbooks(chunk)
            for ticker, payload in books:
                market = self._catalog.market(ticker)
                try:
                    rest = normalize_rest_orderbook(payload, market.price_grid)
                except BookValidationError:
                    stream.record_mismatch(ticker, equal=False)
                    stream.suspend(ticker, MarketStatus.PAUSED, "rest_mismatch")
                    continue
                local = manager.book(ticker)
                same = rest.yes_bids == local.yes_bids and rest.no_bids == local.no_bids
                stream.record_mismatch(ticker, equal=same)
                if not same and local.sync_status.value == "synchronized":
                    stream.suspend(ticker, MarketStatus.PAUSED, "rest_mismatch")
                    await stream.request_recovery(
                        RecoveryRequest(
                            connection_id=stream._connection_id or None,
                            market_ids=(ticker,),
                            reason="rest_mismatch",
                        )
                    )

    async def _orderbooks(self, tickers: list[str]) -> list[tuple[str, dict[str, object]]]:
        rows = await self._rest.get_orderbooks(tickers)
        out: list[tuple[str, dict[str, object]]] = []
        for row in rows:
            ticker = row.get("ticker")
            book = row.get("orderbook_fp")
            if isinstance(ticker, str) and isinstance(book, dict):
                out.append((ticker, {"orderbook_fp": book}))
        return out

    async def _refresh_rules(self) -> None:
        assert self._catalog is not None
        stream = self._stream
        if stream is None:
            return
        for market in self._catalog.markets:
            body = await self._rest.get_json(f"/markets/{market.ticker}")
            raw = body.get("market")
            if not isinstance(raw, dict):
                continue
            rules = _rules_text(raw, "rules_primary")
            secondary = _rules_text(raw, "rules_secondary")
            text = "\n".join(part for part in (rules, secondary) if part)
            if text and text != market.settlement_rules:
                stream.suspend(market.market_id, MarketStatus.PAUSED, "rules_changed")
            status = raw.get("status")
            if isinstance(status, str) and status not in ("active", "open"):
                stream.suspend(market.market_id, MarketStatus.CLOSED, "not_open")

    async def connectivity_check(self) -> dict[str, object]:
        """Signed GET /account/limits. Usage tier only; not a balance or a position."""
        try:
            self._rest.require_credentials()
        except KalshiCredentialsError as exc:
            raise self._credentials_error(exc) from None
        body = await self._rest.get_json("/account/limits")
        tier = body.get("usage_tier")
        return {"ok": True, "usage_tier": tier if isinstance(tier, str) else None}


def _rules_text(raw: dict[str, object], key: str) -> str:
    value = raw.get(key)
    return value if isinstance(value, str) else ""


def _snapshot_from_rest(
    market_id: str, grid: PriceGrid, payload: dict[str, object]
) -> OrderBookSnapshotEvent:
    book = normalize_rest_orderbook(payload, grid)
    return OrderBookSnapshotEvent(
        market_id=market_id,
        sid="rest",
        seq=0,
        connection_id="rest",
        exchange_ts_ms=None,
        yes_bids=tuple((level.price, level.quantity) for level in book.yes_bids),
        no_bids=tuple((level.price, level.quantity) for level in book.no_bids),
    )


# Imported for callers that catch authentication separately from a refused start.
__all__ = [
    "KalshiAccessRefusedError",
    "KalshiDataSource",
    "SourceAuthenticationError",
]

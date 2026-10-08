"""Small builders shared by tests. Prices/quantities are always given as strings."""

from __future__ import annotations

from datetime import UTC, datetime

from consistency_core.models import (
    DataSourceKind,
    MarketStatus,
    OrderBook,
    Provenance,
    Side,
    SyncStatus,
)
from consistency_core.models.market import Market
from consistency_core.models.orderbook import build_side
from consistency_core.models.settlement import SettlementSpec
from consistency_core.money import dec
from consistency_core.ticks import PriceGrid

T0 = datetime(2026, 7, 15, 14, 0, tzinfo=UTC)
T0_MS = int(T0.timestamp() * 1000)
PROV = Provenance(source_kind=DataSourceKind.SYNTHETIC, source_id="unit-test")


def market(
    market_id: str,
    *,
    series_id: str = "SYN-TEST",
    event_id: str = "SYN-TEST-EV",
    grid: PriceGrid | None = None,
    quantity_increment: str = "1",
    settlement: SettlementSpec | None = None,
    source: str = "Synthetic Bureau",
    provenance: Provenance = PROV,
) -> Market:
    return Market(
        market_id=market_id,
        ticker=market_id,
        event_id=event_id,
        series_id=series_id,
        title=f"Test market {market_id}",
        status=MarketStatus.OPEN,
        open_time=T0,
        close_time=datetime(2026, 12, 31, tzinfo=UTC),
        expiration_time=datetime(2026, 12, 31, tzinfo=UTC),
        settlement_source=source,
        settlement_rules=f"Rules for {market_id}",
        rules_version="v1",
        price_grid=grid or PriceGrid.uniform(dec("0.0001")),
        quantity_increment=dec(quantity_increment),
        settlement=settlement or SettlementSpec(),
        provenance=provenance,
    )


def book(
    market_id: str,
    *,
    yes: list[tuple[str, str]] | None = None,
    no: list[tuple[str, str]] | None = None,
    received_ts_ms: int = T0_MS,
    exchange_ts_ms: int | None = None,
    sync: SyncStatus = SyncStatus.SYNCHRONIZED,
    seq: int | None = 1,
) -> OrderBook:
    return OrderBook(
        market_id=market_id,
        source="unit-test",
        source_sequence=seq,
        subscription_id="1",
        connection_id="c1",
        exchange_ts_ms=exchange_ts_ms,
        received_ts_ms=received_ts_ms,
        last_sync_ts_ms=received_ts_ms,
        yes_bids=build_side(Side.YES, [(dec(p), dec(q)) for p, q in (yes or [])]),
        no_bids=build_side(Side.NO, [(dec(p), dec(q)) for p, q in (no or [])]),
        sync_status=sync,
    )


def book_with_asks(
    market_id: str,
    *,
    yes_asks: list[tuple[str, str]] | None = None,
    no_asks: list[tuple[str, str]] | None = None,
    **kw: object,
) -> OrderBook:
    """Build a book from desired *ask* curves: a YES ask at p is a NO bid at 1 - p."""
    one = dec("1")
    no_bids = [(str(one - dec(p)), q) for p, q in (yes_asks or [])]
    yes_bids = [(str(one - dec(p)), q) for p, q in (no_asks or [])]
    return book(market_id, yes=yes_bids, no=no_bids, **kw)  # type: ignore[arg-type]

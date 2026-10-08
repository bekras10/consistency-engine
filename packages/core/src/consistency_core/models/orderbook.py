"""Binary order books.

A binary exchange displays only *bids*: YES bids and NO bids. Buying YES means lifting a resting
NO bid, so the executable YES ask curve is derived from NO bids::

    YES ask = 1 - NO bid,   NO ask = 1 - YES bid

with the opposing bid's quantity. Ask curves are built by sorting the opposite bids high -> low
and complementing, which yields asks cheapest -> most expensive. No liquidity is invented: every
ask level corresponds to exactly one displayed bid level.
"""

from __future__ import annotations

from collections.abc import Iterable
from enum import StrEnum

from pydantic import field_validator, model_validator

from consistency_core.models.common import FrozenModel, HealthStatus, Side, SyncStatus
from consistency_core.money import ONE, PRICE_DP, QUANTITY_DP, ZERO, Dec, decimal_places


class BookErrorCode(StrEnum):
    NEGATIVE_QUANTITY = "NEGATIVE_QUANTITY"
    PRICE_OUT_OF_RANGE = "PRICE_OUT_OF_RANGE"
    PRICE_OFF_GRID = "PRICE_OFF_GRID"
    PRICE_PRECISION = "PRICE_PRECISION"
    QUANTITY_PRECISION = "QUANTITY_PRECISION"
    DUPLICATE_LEVEL = "DUPLICATE_LEVEL"
    UNSORTED_LEVELS = "UNSORTED_LEVELS"
    ZERO_QUANTITY_LEVEL = "ZERO_QUANTITY_LEVEL"
    WRONG_SIDE = "WRONG_SIDE"
    CROSSED_BOOK = "CROSSED_BOOK"
    MALFORMED = "MALFORMED"
    UNKNOWN_MARKET = "UNKNOWN_MARKET"


class BookValidationError(ValueError):
    def __init__(self, code: BookErrorCode, detail: str) -> None:
        super().__init__(f"{code.value}: {detail}")
        self.code = code
        self.detail = detail


class PriceLevel(FrozenModel):
    """A displayed resting bid: ``quantity`` contracts bid at ``price`` for ``side``."""

    side: Side
    price: Dec
    quantity: Dec

    @field_validator("price")
    @classmethod
    def _price(cls, v: Dec) -> Dec:
        if not (ZERO < v < ONE):
            raise BookValidationError(BookErrorCode.PRICE_OUT_OF_RANGE, f"price {v} not in (0,1)")
        if decimal_places(v) > PRICE_DP:
            raise BookValidationError(BookErrorCode.PRICE_PRECISION, f"price {v} > {PRICE_DP} dp")
        return v

    @field_validator("quantity")
    @classmethod
    def _qty(cls, v: Dec) -> Dec:
        if v < ZERO:
            raise BookValidationError(BookErrorCode.NEGATIVE_QUANTITY, f"quantity {v} < 0")
        if decimal_places(v) > QUANTITY_DP:
            raise BookValidationError(
                BookErrorCode.QUANTITY_PRECISION, f"quantity {v} > {QUANTITY_DP} dp"
            )
        return v


class AskLevel(FrozenModel):
    """An executable ask for buying ``side``, derived from one opposing bid level."""

    side: Side
    price: Dec
    quantity: Dec
    source_bid_side: Side
    source_bid_price: Dec


def ask_curve(opposite_bids: Iterable[PriceLevel], buy_side: Side) -> tuple[AskLevel, ...]:
    """Derive the ask curve for buying ``buy_side`` from the *opposite* side's bids.

    Steps (spec 2.3): take opposite bids, sort high -> low, complement each price, keep the
    quantity, and return asks cheapest-first. Zero-quantity levels contribute nothing.
    """
    bids = sorted(opposite_bids, key=lambda lvl: lvl.price, reverse=True)
    out: list[AskLevel] = []
    for lvl in bids:
        if lvl.side is not buy_side.opposite:
            raise BookValidationError(
                BookErrorCode.WRONG_SIDE, f"{lvl.side} bid cannot back a {buy_side} ask"
            )
        if lvl.quantity == ZERO:
            continue
        out.append(
            AskLevel(
                side=buy_side,
                price=ONE - lvl.price,
                quantity=lvl.quantity,
                source_bid_side=lvl.side,
                source_bid_price=lvl.price,
            )
        )
    return tuple(out)


def _check_side(levels: tuple[PriceLevel, ...], side: Side) -> None:
    prev = None
    for lvl in levels:
        if lvl.side is not side:
            raise BookValidationError(BookErrorCode.WRONG_SIDE, f"{lvl.side} level in {side} bids")
        if lvl.quantity == ZERO:
            raise BookValidationError(
                BookErrorCode.ZERO_QUANTITY_LEVEL, "normalized books hold no zero-quantity levels"
            )
        if prev is not None:
            if lvl.price == prev:
                raise BookValidationError(BookErrorCode.DUPLICATE_LEVEL, f"price {lvl.price}")
            if lvl.price > prev:
                raise BookValidationError(BookErrorCode.UNSORTED_LEVELS, "bids must be descending")
        prev = lvl.price


class OrderBook(FrozenModel):
    """Locally held book state for one binary market plus its synchronization metadata.

    ``yes_bids``/``no_bids`` are sorted by price descending, unique, strictly positive quantity.
    Timestamps are integer epoch milliseconds (exact and replay-friendly).
    """

    market_id: str
    source: str
    source_sequence: int | None = None
    connection_id: str | None = None
    subscription_id: str | None = None
    exchange_ts_ms: int | None = None
    received_ts_ms: int
    last_sync_ts_ms: int | None = None
    confirmed_through_ms: int | None = None
    """Exchange time up to which this book is known complete: the emission time of the latest
    in-order message received on its connection. A quiet market stays fresh as long as its
    connection keeps delivering (deltas for other markets or heartbeats)."""
    yes_bids: tuple[PriceLevel, ...] = ()
    no_bids: tuple[PriceLevel, ...] = ()
    sync_status: SyncStatus = SyncStatus.SYNCHRONIZED
    health_status: HealthStatus = HealthStatus.HEALTHY

    @model_validator(mode="after")
    def _check(self) -> OrderBook:
        _check_side(self.yes_bids, Side.YES)
        _check_side(self.no_bids, Side.NO)
        if self.yes_bids and self.no_bids:
            total = self.yes_bids[0].price + self.no_bids[0].price
            if total >= ONE:
                raise BookValidationError(
                    BookErrorCode.CROSSED_BOOK,
                    f"best YES bid {self.yes_bids[0].price} + best NO bid "
                    f"{self.no_bids[0].price} = {total} >= 1 (would have matched)",
                )
        return self

    # ------------------------------------------------------------------ derived curves
    def asks(self, side: Side) -> tuple[AskLevel, ...]:
        """Executable ask curve for buying ``side`` (cheapest first)."""
        return ask_curve(self.no_bids if side is Side.YES else self.yes_bids, side)

    @property
    def yes_asks(self) -> tuple[AskLevel, ...]:
        return self.asks(Side.YES)

    @property
    def no_asks(self) -> tuple[AskLevel, ...]:
        return self.asks(Side.NO)

    def best_bid(self, side: Side) -> Dec | None:
        levels = self.yes_bids if side is Side.YES else self.no_bids
        return levels[0].price if levels else None

    def best_ask(self, side: Side) -> Dec | None:
        opp = self.best_bid(side.opposite)
        return None if opp is None else ONE - opp

    def displayed_quantity(self, side: Side) -> Dec:
        """Total quantity available to *buy* ``side`` (i.e. opposite-side bid depth)."""
        levels = self.no_bids if side is Side.YES else self.yes_bids
        return sum((lvl.quantity for lvl in levels), ZERO)

    @property
    def observed_ts_ms(self) -> int:
        """Time at which this book state is known valid (used for age and cross-market skew).

        Preference: ``confirmed_through_ms``, else the exchange timestamp of the last update,
        else local receipt time.
        """
        if self.confirmed_through_ms is not None:
            return self.confirmed_through_ms
        return self.exchange_ts_ms if self.exchange_ts_ms is not None else self.received_ts_ms


def build_side(side: Side, levels: Iterable[tuple[Dec, Dec]]) -> tuple[PriceLevel, ...]:
    """Validate raw (price, qty) pairs, drop zero-quantity levels, sort descending.

    Duplicate prices are malformed (a book cannot display one price twice).
    """
    seen: set[Dec] = set()
    out: list[PriceLevel] = []
    for price, qty in levels:
        lvl = PriceLevel(side=side, price=price, quantity=qty)
        if lvl.price in seen:
            raise BookValidationError(BookErrorCode.DUPLICATE_LEVEL, f"{side} price {lvl.price}")
        seen.add(lvl.price)
        if lvl.quantity > ZERO:
            out.append(lvl)
    out.sort(key=lambda lvl: lvl.price, reverse=True)
    return tuple(out)

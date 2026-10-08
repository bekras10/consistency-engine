"""Spec step 2.4 normalization tests."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from consistency_core.models import BookErrorCode, BookValidationError, OrderBook, Side
from consistency_core.models.orderbook import PriceLevel, ask_curve
from consistency_core.money import dec
from consistency_core.normalization import (
    normalize_rest_orderbook,
    normalize_ws_message,
    parse_price,
    parse_quantity,
    parse_signed_quantity,
)
from consistency_core.ticks import PriceGrid, parse_price_grid
from tests.factories import book

CENT_GRID = PriceGrid.uniform()
FINE_GRID = PriceGrid.uniform(dec("0.0001"))


def code_of(exc: BaseException) -> BookErrorCode:
    if isinstance(exc, BookValidationError):
        return exc.code
    assert isinstance(exc, ValidationError)
    inner = exc.errors()[0]["ctx"]["error"]
    assert isinstance(inner, BookValidationError)
    return inner.code


class TestScalarParsing:
    def test_whole_cent(self) -> None:
        assert parse_price("0.38") == dec("0.38")

    def test_sub_cent(self) -> None:
        assert parse_price("0.3825") == dec("0.3825")

    def test_price_precision_limit(self) -> None:
        with pytest.raises(BookValidationError) as e:
            parse_price("0.38251")
        assert e.value.code is BookErrorCode.PRICE_PRECISION

    @pytest.mark.parametrize("raw", ["0", "1", "1.0000", "0.0000", "1.5"])
    def test_price_out_of_range(self, raw: str) -> None:
        with pytest.raises(BookValidationError) as e:
            parse_price(raw)
        assert e.value.code is BookErrorCode.PRICE_OUT_OF_RANGE

    @pytest.mark.parametrize("raw", [0.38, 38, "-0.38", "3.8e-1", "", " 0.38", "0.38 ", None])
    def test_price_malformed(self, raw: object) -> None:
        with pytest.raises(BookValidationError) as e:
            parse_price(raw)
        assert e.value.code is BookErrorCode.MALFORMED

    def test_fractional_quantity(self) -> None:
        assert parse_quantity("12.50") == dec("12.5")
        assert parse_quantity("0.00") == dec("0")

    def test_quantity_precision(self) -> None:
        with pytest.raises(BookValidationError) as e:
            parse_quantity("1.005")
        assert e.value.code is BookErrorCode.QUANTITY_PRECISION

    def test_negative_quantity_literal_rejected(self) -> None:
        with pytest.raises(BookValidationError):
            parse_quantity("-1.00")

    def test_signed_delta(self) -> None:
        assert parse_signed_quantity("-54.00") == dec("-54")
        assert parse_signed_quantity("+3.25") == dec("3.25")
        with pytest.raises(BookValidationError):
            parse_signed_quantity("0.00")


class TestBinaryBook:
    def test_complements_from_spec_example(self) -> None:
        # Spec 2.3: best NO bid 0.6200 -> best YES ask 0.3800, same quantity.
        b = book("M", yes=[("0.3000", "5")], no=[("0.6200", "40")])
        assert b.best_ask(Side.YES) == dec("0.3800")
        assert b.yes_asks[0].quantity == dec("40")
        assert b.best_ask(Side.NO) == dec("0.7000")
        assert b.no_asks[0].quantity == dec("5")

    def test_ask_curve_sorted_cheapest_first_from_bids_high_to_low(self) -> None:
        b = book("M", no=[("0.60", "10"), ("0.62", "40"), ("0.55", "7")])
        asks = b.yes_asks
        assert [a.price for a in asks] == [dec("0.38"), dec("0.40"), dec("0.45")]
        assert [a.quantity for a in asks] == [dec("40"), dec("10"), dec("7")]
        assert [a.source_bid_price for a in asks] == [dec("0.62"), dec("0.60"), dec("0.55")]

    def test_no_invented_liquidity(self) -> None:
        b = book("M", yes=[("0.40", "10")])
        assert b.yes_asks == ()  # no NO bids -> nothing to buy YES from
        assert b.displayed_quantity(Side.YES) == dec("0")
        assert b.displayed_quantity(Side.NO) == dec("10")

    def test_empty_book(self) -> None:
        b = book("M")
        assert b.yes_asks == () and b.no_asks == ()
        assert b.best_ask(Side.YES) is None and b.best_bid(Side.NO) is None

    def test_complement_consistency_every_level(self) -> None:
        b = book("M", yes=[("0.41", "1"), ("0.3333", "2.5")], no=[("0.5", "3"), ("0.1234", "9")])
        for a in b.yes_asks:
            assert a.price + a.source_bid_price == dec("1")
        for a in b.no_asks:
            assert a.price + a.source_bid_price == dec("1")

    def test_crossed_book_rejected(self) -> None:
        with pytest.raises(ValidationError) as e:
            book("M", yes=[("0.40", "1")], no=[("0.60", "1")])  # sum == 1 would have matched
        assert code_of(e.value) is BookErrorCode.CROSSED_BOOK

    def test_negative_level_rejected(self) -> None:
        with pytest.raises(ValidationError) as e:
            PriceLevel(side=Side.YES, price=dec("0.5"), quantity=dec("-1"))
        assert code_of(e.value) is BookErrorCode.NEGATIVE_QUANTITY

    def test_unsorted_levels_rejected(self) -> None:
        lv = [
            PriceLevel(side=Side.YES, price=dec("0.3"), quantity=dec("1")),
            PriceLevel(side=Side.YES, price=dec("0.4"), quantity=dec("1")),
        ]
        with pytest.raises(ValidationError):
            OrderBook(market_id="M", source="t", received_ts_ms=0, yes_bids=tuple(lv))

    def test_wrong_side_curve(self) -> None:
        lvl = PriceLevel(side=Side.YES, price=dec("0.3"), quantity=dec("1"))
        with pytest.raises(BookValidationError):
            ask_curve([lvl], Side.YES)

    def test_float_price_refused(self) -> None:
        with pytest.raises(ValidationError):
            PriceLevel(side=Side.YES, price=0.5, quantity=dec("1"))  # type: ignore[arg-type]

    def test_json_serialization_keeps_strings(self) -> None:
        b = book("M", yes=[("0.3333", "3.33")])
        data = json.loads(b.model_dump_json())
        assert data["yes_bids"][0] == {"side": "yes", "price": "0.3333", "quantity": "3.33"}


class TestRestNormalization:
    def test_fixed_point_payload(self) -> None:
        payload = {
            "orderbook_fp": {
                "yes_dollars": [["0.3500", "10.00"], ["0.3600", "5.50"]],
                "no_dollars": [["0.6200", "40.00"]],
            }
        }
        rb = normalize_rest_orderbook(payload, FINE_GRID)
        assert [lv.price for lv in rb.yes_bids] == [dec("0.36"), dec("0.35")]
        assert rb.yes_bids[0].quantity == dec("5.5")
        assert rb.no_bids[0].price == dec("0.62")

    def test_empty_sides(self) -> None:
        rb = normalize_rest_orderbook({"orderbook_fp": {"yes_dollars": None}}, CENT_GRID)
        assert rb.yes_bids == () and rb.no_bids == ()

    def test_zero_quantity_levels_dropped(self) -> None:
        rb = normalize_rest_orderbook(
            {"orderbook_fp": {"yes_dollars": [["0.30", "0.00"], ["0.29", "1.00"]]}}, CENT_GRID
        )
        assert [lv.price for lv in rb.yes_bids] == [dec("0.29")]

    def test_legacy_cents_payload(self) -> None:
        rb = normalize_rest_orderbook(
            {"orderbook": {"yes": [[45, 100]], "no": [[50, 3]]}}, CENT_GRID
        )
        assert rb.yes_bids[0].price == dec("0.45")
        assert rb.no_bids[0].quantity == dec("3")

    def test_grid_boundaries(self) -> None:
        grid = parse_price_grid(
            {
                "price_ranges": [
                    {"start": "0.0000", "end": "0.1000", "step": "0.0010"},
                    {"start": "0.1000", "end": "1.0000", "step": "0.0100"},
                ]
            }
        )
        ok = normalize_rest_orderbook(
            {"orderbook_fp": {"yes_dollars": [["0.0010", "1"], ["0.1000", "1"]]}}, grid
        )
        assert len(ok.yes_bids) == 2
        with pytest.raises(BookValidationError) as e:
            normalize_rest_orderbook({"orderbook_fp": {"yes_dollars": [["0.1050", "1"]]}}, grid)
        assert e.value.code is BookErrorCode.PRICE_OFF_GRID

    @pytest.mark.parametrize(
        ("payload", "code"),
        [
            ({}, BookErrorCode.MALFORMED),
            ({"orderbook_fp": []}, BookErrorCode.MALFORMED),
            ({"orderbook_fp": {"yes_dollars": [["0.30"]]}}, BookErrorCode.MALFORMED),
            ({"orderbook_fp": {"yes_dollars": [[0.30, "1"]]}}, BookErrorCode.MALFORMED),
            ({"orderbook_fp": {"yes_dollars": [["0.30", "-1"]]}}, BookErrorCode.MALFORMED),
            (
                {"orderbook_fp": {"yes_dollars": [["0.30", "1"], ["0.30", "2"]]}},
                BookErrorCode.DUPLICATE_LEVEL,
            ),
            (
                {"orderbook_fp": {"yes_dollars": [["0.45", "1"]], "no_dollars": [["0.55", "1"]]}},
                BookErrorCode.CROSSED_BOOK,
            ),
            ({"orderbook_fp": {"yes_dollars": [["0.305", "1"]]}}, BookErrorCode.PRICE_OFF_GRID),
            (
                {"orderbook_fp": {"yes_dollars": [["1.0000", "1"]]}},
                BookErrorCode.PRICE_OUT_OF_RANGE,
            ),
        ],
    )
    def test_malformed(self, payload: dict[str, object], code: BookErrorCode) -> None:
        with pytest.raises(BookValidationError) as e:
            normalize_rest_orderbook(payload, CENT_GRID)
        assert e.value.code is code


class TestWsNormalization:
    def test_snapshot(self) -> None:
        ev = normalize_ws_message(
            {
                "type": "orderbook_snapshot",
                "sid": 2,
                "seq": 7,
                "msg": {
                    "market_ticker": "SYN-A",
                    "yes_dollars_fp": [["0.0800", "300.00"]],
                    "no_dollars_fp": [["0.5400", "20.25"]],
                },
            },
            connection_id="c1",
            received_ts_ms=0,
        )
        assert ev.type == "orderbook_snapshot"
        assert ev.sid == "2" and ev.seq == 7
        assert ev.yes_bids == ((dec("0.08"), dec("300")),)

    def test_delta(self) -> None:
        ev = normalize_ws_message(
            {
                "type": "orderbook_delta",
                "sid": 2,
                "seq": 8,
                "msg": {
                    "market_ticker": "SYN-A",
                    "price_dollars": "0.9600",
                    "delta_fp": "-54.00",
                    "side": "yes",
                },
            },
            connection_id="c1",
            received_ts_ms=0,
        )
        assert ev.type == "orderbook_delta"
        assert ev.delta == dec("-54") and ev.side is Side.YES

    @pytest.mark.parametrize(
        "raw",
        [
            {"type": "orderbook_delta", "sid": 1, "seq": -1, "msg": {}},
            {"type": "orderbook_delta", "sid": 1, "seq": 1, "msg": {"market_ticker": "X"}},
            {"type": "orderbook_delta", "sid": "1", "seq": 1, "msg": {"market_ticker": "X"}},
            {"type": "ticker", "sid": 1, "seq": 1, "msg": {"market_ticker": "X"}},
            {
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "X",
                    "price_dollars": "0.5",
                    "delta_fp": "1",
                    "side": "maybe",
                },
            },
        ],
    )
    def test_malformed(self, raw: dict[str, object]) -> None:
        with pytest.raises(BookValidationError):
            normalize_ws_message(raw, connection_id="c", received_ts_ms=0)

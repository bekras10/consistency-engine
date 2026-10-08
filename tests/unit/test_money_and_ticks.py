from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from consistency_core.money import (
    ceil_to,
    dec,
    dec_str,
    decimal_places,
    floor_to,
    is_multiple,
)
from consistency_core.ticks import PriceGrid, TickRange, parse_price_grid


class TestDec:
    def test_rejects_float(self) -> None:
        with pytest.raises(TypeError):
            dec(0.1)

    def test_rejects_bool(self) -> None:
        with pytest.raises(TypeError):
            dec(True)

    def test_rejects_nan_and_inf(self) -> None:
        for bad in ("NaN", "Infinity", "-inf"):
            with pytest.raises(ValueError, match="non-finite"):
                dec(bad)

    def test_rejects_garbage(self) -> None:
        with pytest.raises(ValueError, match="not a decimal"):
            dec("0.1x")

    def test_exact_string(self) -> None:
        assert dec("0.1") + dec("0.2") == Decimal("0.3")

    def test_decimal_places(self) -> None:
        assert decimal_places(dec("0.3800")) == 2
        assert decimal_places(dec("0.3333")) == 4
        assert decimal_places(dec("10")) == 0


class TestRounding:
    def test_ceil_floor_positive(self) -> None:
        assert ceil_to(dec("0.00363825"), dec("0.000001")) == dec("0.003639")
        assert floor_to(dec("0.00363825"), dec("0.000001")) == dec("0.003638")

    def test_floor_negative_goes_away_from_zero(self) -> None:
        # Balance rounding of a buyer's revenue: -0.058639 floors to -0.06 on the cent grid.
        assert floor_to(dec("-0.058639"), dec("0.01")) == dec("-0.06")
        assert ceil_to(dec("-0.058639"), dec("0.01")) == dec("-0.05")

    def test_already_on_grid_unchanged(self) -> None:
        assert ceil_to(dec("1.75"), dec("0.01")) == dec("1.75")
        assert floor_to(dec("-51.75"), dec("0.01")) == dec("-51.75")

    def test_bad_increment(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            ceil_to(dec("1"), dec("0"))

    def test_is_multiple(self) -> None:
        assert is_multiple(dec("0.35"), dec("0.01"))
        assert not is_multiple(dec("0.355"), dec("0.01"))

    def test_dec_str_never_uses_exponent(self) -> None:
        assert dec_str(dec("0E-8")) == "0.00000000"
        assert dec_str(dec("1E+2")) == "100"
        assert dec_str(dec("-0.00")) == "0.00"
        assert dec_str(dec("0.3800")) == "0.3800"


class TestPriceGrid:
    def test_uniform_cent_grid(self) -> None:
        g = PriceGrid.uniform()
        assert g.is_valid_price(dec("0.01"))
        assert g.is_valid_price(dec("0.99"))
        assert not g.is_valid_price(dec("0.995"))
        assert not g.is_valid_price(dec("0"))
        assert not g.is_valid_price(dec("1"))

    def test_tapered_price_ranges(self) -> None:
        g = parse_price_grid(
            {
                "price_ranges": [
                    {"start": "0.0000", "end": "0.1000", "step": "0.0010"},
                    {"start": "0.1000", "end": "0.9000", "step": "0.0100"},
                    {"start": "0.9000", "end": "1.0000", "step": "0.0010"},
                ]
            }
        )
        assert g.is_valid_price(dec("0.0010"))  # sub-cent near the bottom
        assert g.is_valid_price(dec("0.0990"))
        assert g.is_valid_price(dec("0.1000"))  # range boundary belongs to both ranges
        assert not g.is_valid_price(dec("0.1050"))  # sub-cent not allowed mid-range
        assert g.is_valid_price(dec("0.9000"))
        assert g.is_valid_price(dec("0.9990"))
        assert not g.is_valid_price(dec("0.9995"))
        assert g.min_step == dec("0.001")

    def test_legacy_tick_size(self) -> None:
        g = parse_price_grid({"tick_size": 1})
        assert g.is_valid_price(dec("0.42"))
        assert not g.is_valid_price(dec("0.425"))

    def test_tick_size_dollars(self) -> None:
        g = parse_price_grid({"tick_size_dollars": "0.0001"})
        assert g.is_valid_price(dec("0.3333"))

    @pytest.mark.parametrize(
        "meta",
        [
            {},
            {"tick_size": 0},
            {"tick_size": True},
            {"price_ranges": "nope"},
            {"price_ranges": [{"start": "0.0", "end": "0.105", "step": "0.01"}]},
            {"price_ranges": [{"start": "0.5", "end": "0.4", "step": "0.01"}]},
            {"price_ranges": [{"start": "0.0", "end": "1.1", "step": "0.01"}]},
        ],
    )
    def test_malformed_grids_rejected(self, meta: dict[str, object]) -> None:
        with pytest.raises((ValueError, ValidationError)):
            parse_price_grid(meta)

    def test_overlapping_ranges_rejected(self) -> None:
        with pytest.raises(ValidationError):
            PriceGrid(
                ranges=(
                    TickRange(start=dec("0"), end=dec("0.6"), step=dec("0.01")),
                    TickRange(start=dec("0.5"), end=dec("1"), step=dec("0.01")),
                )
            )

    def test_valid_prices_listing(self) -> None:
        g = PriceGrid.uniform(dec("0.25"))
        assert g.valid_prices() == [dec("0.25"), dec("0.50"), dec("0.75")]

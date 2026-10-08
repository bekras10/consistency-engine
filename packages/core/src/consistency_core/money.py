"""Exact monetary arithmetic.

All prices, quantities, fees, costs and edges are :class:`decimal.Decimal`. Binary floats are
rejected at every construction boundary. Rounding happens only through :func:`ceil_to` and
:func:`floor_to` with an explicit power-of-ten increment, which are exact operations.

Conventions (Kalshi fixed-point style):

* prices are dollars in (0, 1) with at most :data:`PRICE_DP` decimal places;
* order-book quantities are contracts with at most :data:`QUANTITY_DP` decimal places;
* fees are computed exactly, then rounded up to :data:`FEE_INCREMENT` ($0.000001).
"""

from __future__ import annotations

import decimal
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Annotated, Final

from pydantic import BeforeValidator, PlainSerializer

PRICE_DP: Final = 4
QUANTITY_DP: Final = 2

ZERO: Final = Decimal(0)
ONE: Final = Decimal(1)
FEE_INCREMENT: Final = Decimal("0.000001")
CENT: Final = Decimal("0.01")
BASIS_POINT_DOLLAR: Final = Decimal("0.0001")

# The default decimal context (28 significant digits) is sufficient: the longest product in the
# engine is coefficient(4 dp) x quantity(2 dp) x price(4 dp) x (1 - price)(4 dp), i.e. 14
# fractional digits, exact for any quantity below 10^13 contracts. Only VWAP divides, and it is
# explicitly quantized and labelled informational.
VWAP_QUANTUM: Final = Decimal("0.00000001")


class MoneyTypeError(TypeError):
    """Raised when a binary float (or bool) is offered as money."""


def dec(value: object) -> Decimal:
    """Convert ``value`` to a finite Decimal, refusing floats and bools.

    Accepts ``Decimal``, ``int`` and ``str``. Strings are parsed exactly, so ``dec("0.1")`` is
    exactly one tenth (``Decimal(0.1)`` would not be).
    """
    if isinstance(value, bool):
        raise MoneyTypeError("bool is not a monetary value")
    if isinstance(value, float):
        raise MoneyTypeError(f"binary float {value!r} refused; pass a string or Decimal")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, str):
        text = value.strip()
        try:
            result = Decimal(text)
        except decimal.InvalidOperation as exc:
            raise ValueError(f"not a decimal number: {value!r}") from exc
    else:
        raise MoneyTypeError(f"unsupported monetary type {type(value).__name__}")
    if not result.is_finite():
        raise ValueError(f"non-finite decimal {value!r}")
    return result


def decimal_places(value: Decimal) -> int:
    """Number of significant fractional digits (trailing zeros ignored)."""
    exponent = value.normalize().as_tuple().exponent
    assert isinstance(exponent, int)
    return max(0, -exponent)


def _check_increment(increment: Decimal) -> None:
    if increment <= ZERO:
        raise ValueError(f"increment must be positive, got {increment}")


def ceil_to(value: Decimal, increment: Decimal) -> Decimal:
    """Smallest multiple of ``increment`` that is >= ``value`` (exact)."""
    _check_increment(increment)
    return (value / increment).to_integral_value(rounding=ROUND_CEILING) * increment


def floor_to(value: Decimal, increment: Decimal) -> Decimal:
    """Largest multiple of ``increment`` that is <= ``value`` (exact)."""
    _check_increment(increment)
    return (value / increment).to_integral_value(rounding=ROUND_FLOOR) * increment


def is_multiple(value: Decimal, increment: Decimal) -> bool:
    _check_increment(increment)
    return value % increment == ZERO


def dec_str(value: Decimal) -> str:
    """Fixed-point string (never exponent notation), preserving the value's scale."""
    if value == ZERO:
        # Avoid "-0" and "0E-8" style representations while keeping the scale.
        value = abs(value)
    return format(value, "f")


def _validate_dec(value: object) -> Decimal:
    try:
        return dec(value)
    except MoneyTypeError as exc:  # pydantic only converts ValueError into ValidationError
        raise ValueError(str(exc)) from exc


Dec = Annotated[
    Decimal,
    BeforeValidator(_validate_dec),
    PlainSerializer(dec_str, return_type=str, when_used="json"),
]
"""Pydantic field type: exact Decimal in, fixed-point string out in JSON mode."""

"""Layer (a): the fee *model* (before any rounding).

Published Kalshi event-contract formulas (schedule effective 2026-07-07):

    taker_fee = M_taker * 0.07   * C * P * (1 - P)
    maker_fee = M_maker * 0.0175 * C * P * (1 - P)

``C`` = contracts (fractional allowed), ``P`` = execution price in dollars, ``M`` = series
multiplier (defaults: M_taker = 1, M_maker = 0). There is no settlement fee. The coefficients
live in versioned schedule files, never in code; the synthetic exchange uses its own clearly
labelled fictional coefficients through exactly the same functions.

The result is exact (Decimal multiplication of terminating decimals) and is *not* rounded here:
rounding is layer (b), :mod:`consistency_core.fees.rounding`.
"""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum

from consistency_core.money import ONE, ZERO


class Role(StrEnum):
    TAKER = "taker"
    MAKER = "maker"


def model_fee(
    *, coefficient: Decimal, multiplier: Decimal, price: Decimal, quantity: Decimal
) -> Decimal:
    if not (ZERO < price < ONE):
        raise ValueError(f"execution price {price} outside (0, 1)")
    if quantity < ZERO or coefficient < ZERO or multiplier < ZERO:
        raise ValueError("fee inputs must be non-negative")
    return multiplier * coefficient * quantity * price * (ONE - price)

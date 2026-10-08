"""Executable price calculation by walking a displayed ask curve (spec 6.1).

Start at the cheapest ask, consume its size, move to the next level, and continue until the
requested quantity is filled or displayed liquidity is exhausted. Every consumed level is kept
(each one becomes a separate fill of the same order for fee purposes).
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import ROUND_FLOOR, Decimal

from consistency_core.models.common import FrozenModel, Side
from consistency_core.models.orderbook import AskLevel
from consistency_core.money import VWAP_QUANTUM, ZERO, Dec


class ConsumedLevel(FrozenModel):
    level_index: int
    price: Dec
    quantity: Dec
    """Quantity taken from this level (<= displayed)."""
    displayed_quantity: Dec
    premium: Dec


class DepthWalkResult(FrozenModel):
    market_id: str
    side: Side
    requested_quantity: Dec
    available_quantity: Dec
    """Total displayed quantity on the curve."""
    filled_quantity: Dec
    unfilled_quantity: Dec
    total_premium: Dec
    vwap: Dec | None
    """Informational only: ``total_premium / filled`` floored to 1e-8. Never used in money math."""
    marginal_price: Dec | None
    """Price of the last (most expensive) level touched."""
    levels: tuple[ConsumedLevel, ...]

    @property
    def fully_filled(self) -> bool:
        return self.unfilled_quantity == ZERO

    @property
    def fills(self) -> list[tuple[Decimal, Decimal]]:
        return [(lv.price, lv.quantity) for lv in self.levels]


def walk_asks(
    market_id: str, side: Side, asks: Sequence[AskLevel], quantity: Decimal
) -> DepthWalkResult:
    if quantity < ZERO:
        raise ValueError("requested quantity must be non-negative")
    prev = None
    for a in asks:
        if a.side is not side:
            raise ValueError(f"{a.side} ask on the {side} curve")
        if prev is not None and a.price <= prev:
            raise ValueError("ask curve must be strictly ascending")
        prev = a.price
    remaining = quantity
    premium = ZERO
    consumed: list[ConsumedLevel] = []
    for i, a in enumerate(asks):
        if remaining == ZERO:
            break
        take = min(remaining, a.quantity)
        cost = take * a.price
        consumed.append(
            ConsumedLevel(
                level_index=i,
                price=a.price,
                quantity=take,
                displayed_quantity=a.quantity,
                premium=cost,
            )
        )
        premium += cost
        remaining -= take
    filled = quantity - remaining
    vwap = (
        None if filled == ZERO else (premium / filled).quantize(VWAP_QUANTUM, rounding=ROUND_FLOOR)
    )
    return DepthWalkResult(
        market_id=market_id,
        side=side,
        requested_quantity=quantity,
        available_quantity=sum((a.quantity for a in asks), ZERO),
        filled_quantity=filled,
        unfilled_quantity=remaining,
        total_premium=premium,
        vwap=vwap,
        marginal_price=consumed[-1].price if consumed else None,
        levels=tuple(consumed),
    )

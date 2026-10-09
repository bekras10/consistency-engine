"""Layer (b): fee rounding, exactly as documented at
https://docs.kalshi.com/getting_started/fee_rounding (copy: docs/sources/kalshi-fee-rounding-*.md).

Per fill, given signed revenue (negative for buyers) and the model fee:

1. ``trade_fee      = ceil_6dp(model_fee)``            (never rounded to cents first)
2. ``aligned_change = floor_precision(revenue - trade_fee)``
3. ``rounding_fee   = (revenue - trade_fee) - aligned_change``
4. the rounding fee is added to the *order's* accumulator (shared by taker and maker fills)
5. a rebate is paid from the accumulator in whole precision increments, capped so the fill's net
   fee cannot be negative.

Precision: DIRECT members $0.0001, NON_DIRECT members $0.01.

Rebate interpretation (documented ambiguity): the page says rebates are "in increments of the
user's target balance precision, capped so the fill's net fee cannot be negative". We implement

    rebate = floor_precision(min(accumulator, trade_fee + rounding_fee))

i.e. the cap is applied *before* aligning to the precision grid, so the rebate is always a whole
number of increments and the post-rebate balance stays on the user's precision grid. A literal
reading "min(floor_precision(accumulator), cap)" can produce an off-grid rebate whenever the cap
binds; we consider that inconsistent with the alignment rule. When the cap does not bind, both
readings agree (they agree on every official example we have).
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from enum import StrEnum

from consistency_core.fees.model import Role, model_fee
from consistency_core.models.common import FrozenModel
from consistency_core.money import FEE_INCREMENT, ZERO, Dec, ceil_to, floor_to


class MemberClass(StrEnum):
    DIRECT = "direct"
    NON_DIRECT = "non_direct"

    @property
    def precision(self) -> Decimal:
        return Decimal("0.0001") if self is MemberClass.DIRECT else Decimal("0.01")


def q6(value: Decimal) -> Decimal:
    """Present a value that is already on the $0.000001 grid with exactly six decimals."""
    out = value.quantize(FEE_INCREMENT)
    if out != value:
        raise ArithmeticError(f"{value} is not a multiple of {FEE_INCREMENT}")
    return out


class FillFee(FrozenModel):
    fill_index: int
    role: Role
    price: Dec
    quantity: Dec
    revenue: Dec
    """Signed: negative for a buyer (``-price * quantity``)."""
    model_fee: Dec
    """Exact layer-(a) fee, unrounded."""
    trade_fee: Dec
    aligned_change: Dec
    rounding_fee: Dec
    accumulator_before: Dec
    rebate: Dec
    accumulator_after: Dec
    net_fee: Dec
    """``trade_fee + rounding_fee - rebate`` (>= 0)."""
    balance_change: Dec
    """``aligned_change + rebate``: what the user's balance actually moves by."""


class OrderFees(FrozenModel):
    member_class: MemberClass
    precision: Dec
    fills: tuple[FillFee, ...]
    total_revenue: Dec
    total_model_fee: Dec
    total_trade_fee: Dec
    total_rounding_fee: Dec
    total_rebate: Dec
    total_net_fee: Dec
    accumulator_remaining: Dec
    total_balance_change: Dec

    @property
    def total_cost(self) -> Decimal:
        """Cash paid by a buyer: premium plus net fees (= -total_balance_change)."""
        return -self.total_balance_change


def fill_components(
    revenue: Decimal, model_fee: Decimal, accumulator: Decimal, precision: Decimal
) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
    """The documented per-fill algorithm. Returns
    ``(trade_fee, aligned_change, rounding_fee, rebate, accumulator_after)``.

    This is the single implementation used by both the itemised and the fast path."""
    if model_fee < ZERO:
        raise ValueError("model fee cannot be negative")
    trade_fee = ceil_to(model_fee, FEE_INCREMENT)
    pre = revenue - trade_fee
    aligned = floor_to(pre, precision)
    rounding_fee = pre - aligned
    acc = accumulator + rounding_fee
    rebate = floor_to(min(acc, trade_fee + rounding_fee), precision)
    return trade_fee, aligned, rounding_fee, rebate, acc - rebate


def order_net_fee(
    fills: Sequence[tuple[Decimal, Decimal]],
    *,
    coefficient: Decimal,
    multiplier: Decimal,
    precision: Decimal,
) -> Decimal:
    """Fast path (no itemisation): total net fee of one buy order."""
    acc = ZERO
    total = ZERO
    for price, qty in fills:
        fee = model_fee(coefficient=coefficient, multiplier=multiplier, price=price, quantity=qty)
        trade, _, rounding, rebate, acc = fill_components(-(price * qty), fee, acc, precision)
        total += trade + rounding - rebate
    return total


def order_net_fee_lower_bound(
    fills: Sequence[tuple[Decimal, Decimal]], *, coefficient: Decimal, multiplier: Decimal
) -> Decimal:
    """Sum of unrounded model fees: a lower bound on :func:`order_net_fee` for the same fills.

    Proof: per fill ``trade_fee = ceil(model_fee) >= model_fee`` and ``rounding_fee >= 0``. The
    accumulator starts at 0 and only gains rounding fees, and a rebate never exceeds the
    accumulator, so after the order ``0 <= accumulator = sum(rounding) - sum(rebate)``. Hence
    ``net = sum(trade) + sum(rounding) - sum(rebate) >= sum(trade) >= sum(model_fee)``.
    For fixed price levels the bound is linear in the filled quantities.
    """
    return sum(
        (
            model_fee(coefficient=coefficient, multiplier=multiplier, price=p, quantity=q)
            for p, q in fills
        ),
        ZERO,
    )


class OrderFeeAccumulator:
    """One order's rounding accumulator; every fill of the order goes through :meth:`fill`."""

    def __init__(self, member_class: MemberClass) -> None:
        self.member_class = member_class
        self.precision = member_class.precision
        self.accumulator = ZERO
        self.fills: list[FillFee] = []

    def fill(
        self, *, revenue: Decimal, model_fee: Decimal, role: Role, price: Decimal, quantity: Decimal
    ) -> FillFee:
        before = self.accumulator
        trade_fee, aligned, rounding_fee, rebate, after = fill_components(
            revenue, model_fee, before, self.precision
        )
        self.accumulator = after
        out = FillFee(
            fill_index=len(self.fills),
            role=role,
            price=price,
            quantity=quantity,
            revenue=q6(revenue),
            model_fee=model_fee,
            trade_fee=q6(trade_fee),
            aligned_change=q6(aligned),
            rounding_fee=q6(rounding_fee),
            accumulator_before=q6(before),
            rebate=q6(rebate),
            accumulator_after=q6(after),
            net_fee=q6(trade_fee + rounding_fee - rebate),
            balance_change=q6(aligned + rebate),
        )
        self.fills.append(out)
        return out

    def summary(self) -> OrderFees:
        fs = self.fills

        def total(attr: str) -> Decimal:
            return sum((getattr(f, attr) for f in fs), ZERO)

        return OrderFees(
            member_class=self.member_class,
            precision=self.precision,
            fills=tuple(fs),
            total_revenue=q6(total("revenue")),
            total_model_fee=total("model_fee"),
            total_trade_fee=q6(total("trade_fee")),
            total_rounding_fee=q6(total("rounding_fee")),
            total_rebate=q6(total("rebate")),
            total_net_fee=q6(total("net_fee")),
            accumulator_remaining=q6(self.accumulator),
            total_balance_change=q6(total("balance_change")),
        )


def buy_order_fees(
    fills: Sequence[tuple[Decimal, Decimal]],
    *,
    coefficient: Decimal,
    multiplier: Decimal,
    member_class: MemberClass,
    role: Role = Role.TAKER,
) -> OrderFees:
    """Fees for one buy order filled at ``[(price, quantity), ...]`` (one fill per level)."""
    acc = OrderFeeAccumulator(member_class)
    for price, qty in fills:
        acc.fill(
            revenue=-(price * qty),
            model_fee=model_fee(
                coefficient=coefficient, multiplier=multiplier, price=price, quantity=qty
            ),
            role=role,
            price=price,
            quantity=qty,
        )
    return acc.summary()

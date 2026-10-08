"""Property tests for the fee rounding layer (official fee-rounding algorithm + accumulator)."""

from __future__ import annotations

from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from consistency_core.fees import MemberClass, OrderFeeAccumulator, Role, buy_order_fees, model_fee
from consistency_core.fees.rounding import order_net_fee
from consistency_core.money import ceil_to

D = Decimal
SIX_DP = D("0.000001")

prices = st.integers(1, 9999).map(lambda i: D(i) / 10000)
quantities = st.integers(1, 100000).map(lambda i: D(i) / 100)
fills_st = st.lists(st.tuples(prices, quantities), min_size=1, max_size=8)
multipliers = st.sampled_from([D(0), D("0.5"), D(1), D(2)])
coefficients = st.sampled_from([D("0.07"), D("0.0175"), D("0.05")])
members = st.sampled_from(list(MemberClass))


@settings(max_examples=300, deadline=None)
@given(fills_st, coefficients, multipliers, members)
def test_fill_and_order_invariants(
    fills: list[tuple[Decimal, Decimal]], c: Decimal, m: Decimal, mc: MemberClass
) -> None:
    out = buy_order_fees(fills, coefficient=c, multiplier=m, member_class=mc)
    p = mc.precision
    ceil_trade_sum = Decimal(0)
    for (price, qty), f in zip(fills, out.fills, strict=True):
        # net fee >= 0 on every fill
        assert f.net_fee >= 0
        # trade fee is ceil_6dp of the exact model fee (never cents-first)
        exact = model_fee(coefficient=c, multiplier=m, price=price, quantity=qty)
        assert f.model_fee == exact
        assert f.trade_fee == ceil_to(exact, SIX_DP)
        ceil_trade_sum += f.trade_fee
        # balance changes land on the member's precision grid
        assert f.aligned_change % p == 0
        assert f.balance_change % p == 0
        assert 0 <= f.rounding_fee < p
        # rebate: whole increments, capped by this fill's own fees
        assert f.rebate % p == 0
        assert 0 <= f.rebate <= f.trade_fee + f.rounding_fee
        assert f.accumulator_after >= 0
    # order net >= sum of ceil_6dp trade fees; the excess is the un-rebated accumulator
    assert out.total_net_fee >= ceil_trade_sum
    assert out.total_net_fee == ceil_trade_sum + out.accumulator_remaining
    # fast path used by the quantity search equals the itemised path
    assert order_net_fee(fills, coefficient=c, multiplier=m, precision=p) == out.total_net_fee


@settings(max_examples=200, deadline=None)
@given(fills_st, coefficients, multipliers, members, st.lists(st.booleans(), min_size=8))
def test_accumulator_persists_across_taker_and_maker_fills(
    fills: list[tuple[Decimal, Decimal]],
    c: Decimal,
    m: Decimal,
    mc: MemberClass,
    roles: list[bool],
) -> None:
    acc = OrderFeeAccumulator(mc)
    prev_after = Decimal(0)
    for (price, qty), is_maker in zip(fills, roles, strict=False):
        f = acc.fill(
            revenue=-(price * qty),
            model_fee=model_fee(coefficient=c, multiplier=m, price=price, quantity=qty),
            role=Role.MAKER if is_maker else Role.TAKER,
            price=price,
            quantity=qty,
        )
        assert f.accumulator_before == prev_after
        assert f.accumulator_after == f.accumulator_before + f.rounding_fee - f.rebate
        prev_after = f.accumulator_after
    assert acc.summary().accumulator_remaining == prev_after

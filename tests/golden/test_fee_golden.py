"""Fee golden tests: published Kalshi formulas, the documented rounding layer, schedule
selection and the live-fee verification gate. Sources: docs/sources/kalshi-fee-*.md."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from consistency_core.fees import (
    FeeAssumptions,
    FeeCalculator,
    FeeReason,
    FeeScheduleRegistry,
    IntermediaryFees,
    MemberClass,
    OrderFeeAccumulator,
    Role,
    buy_order_fees,
    load_schedule,
    model_fee,
)
from consistency_core.fees.rounding import fill_components, order_net_fee
from consistency_core.models.common import DataSourceKind, Provenance
from consistency_core.models.market import Market
from tests.conftest import FIXTURES
from tests.factories import market

pytestmark = pytest.mark.golden

D = Decimal
KALSHI = FIXTURES / "fees" / "kalshi-2026-07-07.yaml"
REAL = Provenance(source_kind=DataSourceKind.KALSHI_AUTHORIZED, source_id="offline-fixture")
AFTER = datetime(2026, 8, 1, tzinfo=UTC)
BEFORE = datetime(2026, 7, 6, 23, 59, 59, tzinfo=UTC)
FULL = FeeAssumptions(
    member_class=MemberClass.NON_DIRECT,
    intermediary_fees=IntermediaryFees.EXPLICITLY_EXCLUDED,
    schedule_revisions_checked=True,
)


def calc() -> FeeCalculator:
    return FeeCalculator(FeeScheduleRegistry.from_directory(FIXTURES / "fees"))


def real_market(series: str, mid: str | None = None) -> Market:
    """Offline construction of a market record shaped like a real series (no network)."""
    return market(mid or f"{series}-TEST", series_id=series, provenance=REAL)


# ------------------------------------------------------------------ official examples
def test_official_single_fill_non_direct() -> None:
    """1 contract @ $0.055, NON_DIRECT. Revenue -0.055000.
    model fee = 0.07 x 1 x 0.055 x 0.945 = 0.00363825
    trade fee = ceil_6dp(0.00363825) = 0.003639   (NOT rounded to cents first)
    aligned   = floor_0.01(-0.055 - 0.003639 = -0.058639) = -0.06
    rounding  = -0.058639 - (-0.06) = 0.001361
    net before rebate = 0.003639 + 0.001361 = 0.005000"""
    fee = model_fee(coefficient=D("0.07"), multiplier=D(1), price=D("0.055"), quantity=D(1))
    assert fee == D("0.00363825")
    acc = OrderFeeAccumulator(MemberClass.NON_DIRECT)
    f = acc.fill(
        revenue=D("-0.055"), model_fee=fee, role=Role.TAKER, price=D("0.055"), quantity=D(1)
    )
    assert f.trade_fee == D("0.003639")
    assert f.aligned_change == D("-0.060000")
    assert f.rounding_fee == D("0.001361")
    assert f.trade_fee + f.rounding_fee == D("0.005000")
    assert f.rebate == 0
    assert f.net_fee == D("0.005")


def test_official_accumulator_three_fills() -> None:
    """Three fills, each contributing $0.004 rounding (NON_DIRECT). Construct each fill as a buy
    of 1 contract @ $0.50 with model fee 0.006: revenue -0.5, trade 0.006, -0.506 -> aligned
    -0.51, rounding 0.004; cap per fill = 0.006 + 0.004 = 0.010.
      accumulator 0.004 -> rebate floor_0.01(min(0.004, 0.010)) = 0
      accumulator 0.008 -> rebate 0
      accumulator 0.012 -> rebate floor_0.01(min(0.012, 0.010)) = 0.010; remaining 0.002"""
    acc = OrderFeeAccumulator(MemberClass.NON_DIRECT)
    seen = []
    for _ in range(3):
        f = acc.fill(
            revenue=D("-0.5"), model_fee=D("0.006"), role=Role.TAKER, price=D("0.5"), quantity=D(1)
        )
        assert f.rounding_fee == D("0.004")
        seen.append((f.accumulator_before + f.rounding_fee, f.rebate, f.accumulator_after))
    assert seen == [
        (D("0.004"), D("0"), D("0.004")),
        (D("0.008"), D("0"), D("0.008")),
        (D("0.012"), D("0.010"), D("0.002")),
    ]
    s = acc.summary()
    assert s.accumulator_remaining == D("0.002")
    assert s.total_net_fee == D("0.02")  # 3 x 0.010 - 0.010


def test_rebate_cap_binds() -> None:
    """Cap binding. DIRECT (precision $0.0001): accumulator 0.0120 + this fill's rounding, but the
    fill's own trade + rounding fees are only 0.0053 -> rebate floor_0.0001(0.0053) = 0.0053;
    net fee 0 (never negative).

    NON_DIRECT ambiguity case: accumulator would allow 0.03 but cap is 0.015 ->
    rebate floor_0.01(0.015) = 0.01 (our interpretation keeps balances on the 1c grid; a literal
    'min(floor(acc), cap)' would give the off-grid 0.015)."""
    trade, aligned, rounding, rebate, after = fill_components(
        revenue=D("-0.5000"),
        model_fee=D("0.0050"),
        accumulator=D("0.0117"),
        precision=MemberClass.DIRECT.precision,
    )
    assert (trade, aligned, rounding) == (D("0.005"), D("-0.5050"), D("0"))
    assert rebate == D("0.0050")  # cap = 0.005 binds (acc 0.0117)
    assert after == D("0.0067")
    assert trade + rounding - rebate == 0
    t2, a2, r2, rb2, after2 = fill_components(
        revenue=D("-0.50"),
        model_fee=D("0.011"),
        accumulator=D("0.026"),
        precision=MemberClass.NON_DIRECT.precision,
    )
    assert (t2, a2, r2) == (D("0.011"), D("-0.52"), D("0.009"))
    cap = t2 + r2
    assert cap == D("0.020")
    assert rb2 == D("0.02")
    assert after2 == D("0.015")  # 0.026 + 0.009 - 0.02
    t3, _, r3, rb3, after3 = fill_components(
        revenue=D("-0.50"),
        model_fee=D("0.006"),
        accumulator=D("0.026"),
        precision=MemberClass.NON_DIRECT.precision,
    )
    assert t3 + r3 == D("0.010")  # cap 0.010; accumulator 0.030
    assert rb3 == D("0.01")
    assert after3 == D("0.020")
    t4, _, r4, rb4, _ = fill_components(
        revenue=D("-0.495"),
        model_fee=D("0.0005"),
        accumulator=D("0.026"),
        precision=MemberClass.NON_DIRECT.precision,
    )
    assert t4 + r4 == D("0.005")  # cap 0.005 < 1c
    assert rb4 == 0  # literal reading would rebate an off-grid 0.005


@pytest.mark.parametrize(
    ("price", "qty", "raw", "net"),
    [
        ("0.50", "1", "0.0175", "0.02"),
        ("0.50", "100", "1.75", "1.75"),
        ("0.10", "100", "0.63", "0.63"),
    ],
)
def test_reference_taker_fees(price: str, qty: str, raw: str, net: str) -> None:
    """Published reference fees, M_taker = 1, NON_DIRECT:
    0.07 x 1 x 0.5 x 0.5 = 0.0175 -> -0.5175 floors to -0.52 -> net 0.02
    0.07 x 100 x 0.5 x 0.5 = 1.75 (whole cents, no rounding fee)
    0.07 x 100 x 0.1 x 0.9 = 0.63"""
    fee = model_fee(coefficient=D("0.07"), multiplier=D(1), price=D(price), quantity=D(qty))
    assert fee == D(raw)
    out = buy_order_fees(
        [(D(price), D(qty))],
        coefficient=D("0.07"),
        multiplier=D(1),
        member_class=MemberClass.NON_DIRECT,
    )
    assert out.total_net_fee == D(net)


def test_maker_multipliers() -> None:
    """Maker coefficient 0.0175. KXCPI maker M=1 -> 0.0175 x 100 x 0.4 x 0.6 = 0.42;
    KXMVE specified combo maker M=2 -> 0.84; default maker M=0 -> 0; unlisted series is
    FEE_UNVERIFIED even though the estimate uses the default."""
    c = calc()
    cpi = c.resolve(real_market("KXCPI"), AFTER, FULL, role=Role.MAKER)
    assert (cpi.multiplier, cpi.coefficient, cpi.verified) == (D(1), D("0.0175"), True)
    o = c.order_fees(cpi, [(D("0.40"), D(100))])
    assert o is not None
    assert o.total_trade_fee == D("0.42")
    mve_m = real_market("KXMVE", "KXMVE-COMBO-1")
    mve = c.resolve(
        mve_m,
        AFTER,
        FULL.model_copy(update={"combo_classifications": {mve_m.market_id: "specified"}}),
        role=Role.MAKER,
    )
    assert (mve.multiplier, mve.verified, mve.matched_exception) == (D(2), True, "KXMVE[specified]")
    o2 = c.order_fees(mve, [(D("0.40"), D(100))])
    assert o2 is not None
    assert o2.total_trade_fee == D("0.84")
    mve_taker = c.resolve(
        mve_m,
        AFTER,
        FULL.model_copy(update={"combo_classifications": {mve_m.market_id: "specified"}}),
    )
    assert mve_taker.multiplier == D(1)
    btc = c.resolve(real_market("KXBTCY"), AFTER, FULL, role=Role.MAKER)
    assert (btc.multiplier, btc.verified) == (D(0), True)
    unlisted = c.resolve(real_market("KXNEWSERIES"), AFTER, FULL, role=Role.MAKER)
    assert unlisted.multiplier == D(0)
    assert not unlisted.verified
    assert FeeReason.SERIES_NOT_IN_PARTIAL_TABLE in unlisted.reasons


def test_zero_multiplier_still_has_rounding_fee() -> None:
    """KXBTCY taker M = 0: buy 3.33 @ 0.3333, NON_DIRECT. model fee 0, trade 0;
    revenue -1.109889 -> aligned -1.11 -> rounding fee 0.000111 (nonzero)."""
    c = calc()
    r = c.resolve(real_market("KXBTCY"), AFTER, FULL)
    assert r.multiplier == 0
    o = c.order_fees(r, [(D("0.3333"), D("3.33"))])
    assert o is not None
    f = o.fills[0]
    assert (f.model_fee, f.trade_fee) == (0, 0)
    assert f.rounding_fee == D("0.000111")
    assert f.net_fee == D("0.000111")


def test_multi_level_fills_share_one_accumulator() -> None:
    """Walked levels are separate fills of one order. NON_DIRECT, taker 0.07, M = 1:
      fill 0: 1 @ 0.50: model 0.0175, -0.5175 -> -0.52, rounding 0.0025, acc 0.0025
      fill 1: 1 @ 0.51: model 0.0174930 -> trade 0.017493; -0.527493 -> -0.53, rounding 0.002507,
              acc 0.005007
      fill 2: 3 @ 0.52: model 0.0524160 -> -1.612416 -> -1.62, rounding 0.007584, acc 0.012591
              -> rebate floor_0.01(min(0.012591, 0.06)) = 0.01, acc 0.002591
    Totals: trade 0.087409, rounding 0.012591, rebate 0.01, net 0.09."""
    out = buy_order_fees(
        [(D("0.50"), D(1)), (D("0.51"), D(1)), (D("0.52"), D(3))],
        coefficient=D("0.07"),
        multiplier=D(1),
        member_class=MemberClass.NON_DIRECT,
    )
    assert [f.rounding_fee for f in out.fills] == [D("0.0025"), D("0.002507"), D("0.007584")]
    assert [f.rebate for f in out.fills] == [D(0), D(0), D("0.01")]
    assert out.total_trade_fee == D("0.087409")
    assert out.total_rounding_fee == D("0.012591")
    assert out.total_rebate == D("0.01")
    assert out.total_net_fee == D("0.09")
    assert out.accumulator_remaining == D("0.002591")
    assert out.total_cost == D("2.66")  # 0.50 + 0.51 + 1.56 + 0.09
    assert (
        order_net_fee(
            [(D("0.50"), D(1)), (D("0.51"), D(1)), (D("0.52"), D(3))],
            coefficient=D("0.07"),
            multiplier=D(1),
            precision=D("0.01"),
        )
        == out.total_net_fee
    )


def test_fractional_quantities_direct_member() -> None:
    """DIRECT ($0.0001): 2.5 @ 0.4321, taker 0.07, M=1.
    model = 0.07 x 2.5 x (0.4321 x 0.5679 = 0.24538959) = 0.04294317825 -> trade 0.042944
    revenue -1.08025 -> -1.080250 - 0.042944 = -1.123194 -> floor_0.0001 = -1.1232
    -> rounding 0.000006"""
    out = buy_order_fees(
        [(D("0.4321"), D("2.5"))],
        coefficient=D("0.07"),
        multiplier=D(1),
        member_class=MemberClass.DIRECT,
    )
    f = out.fills[0]
    assert f.model_fee == D("0.04294317825")
    assert f.trade_fee == D("0.042944")
    assert f.aligned_change == D("-1.1232")
    assert f.rounding_fee == D("0.000006")


# ------------------------------------------------------------------ schedule selection / gate
def test_schedule_version_selection_by_timestamp() -> None:
    c = calc()
    m = real_market("KXCPI")
    before = c.resolve(m, BEFORE, FULL)
    assert before.schedule_id is None
    assert not before.verified
    assert FeeReason.NO_SCHEDULE_EFFECTIVE in before.reasons
    at = c.resolve(m, datetime(2026, 7, 7, tzinfo=UTC), FULL)
    assert (at.schedule_id, at.verified) == ("kalshi-2026-07-07", True)
    assert c.resolve(m, AFTER, FULL).verified


def test_series_match_is_exact_not_prefix() -> None:
    c = calc()
    for series in ("KXCPIYOY", "KXCP", "kxcpi", "KXFEDDECISION"):
        r = c.resolve(real_market(series), AFTER, FULL)
        assert not r.verified, series
        assert r.matched_exception is None
        assert FeeReason.SERIES_NOT_IN_PARTIAL_TABLE in r.reasons
    assert c.resolve(real_market("KXFED"), AFTER, FULL).matched_exception == "KXFED"


def test_kxmve_requires_combo_classification() -> None:
    c = calc()
    m = real_market("KXMVE", "KXMVE-X")
    r = c.resolve(m, AFTER, FULL)
    assert FeeReason.COMBO_CLASSIFICATION_UNKNOWN in r.reasons
    other = c.resolve(
        m, AFTER, FULL.model_copy(update={"combo_classifications": {m.market_id: "uncorrelated"}})
    )
    assert FeeReason.COMBO_CLASSIFICATION_NOT_LISTED in other.reasons


@pytest.mark.parametrize(
    ("assumptions", "reason"),
    [
        (FULL.model_copy(update={"member_class": None}), FeeReason.MEMBER_CLASS_UNKNOWN),
        (
            FULL.model_copy(update={"intermediary_fees": IntermediaryFees.UNKNOWN}),
            FeeReason.INTERMEDIARY_FEES_UNKNOWN,
        ),
        (
            FULL.model_copy(update={"schedule_revisions_checked": False}),
            FeeReason.SCHEDULE_REVISIONS_NOT_CHECKED,
        ),
    ],
)
def test_live_fee_gate_requires_every_fact(assumptions: FeeAssumptions, reason: FeeReason) -> None:
    r = calc().resolve(real_market("KXCPI"), AFTER, assumptions)
    assert not r.verified
    assert reason in r.reasons


def test_unknown_member_class_estimates_conservatively_at_non_direct() -> None:
    r = calc().resolve(real_market("KXCPI"), AFTER, FULL.model_copy(update={"member_class": None}))
    assert r.member_class is MemberClass.NON_DIRECT
    assert r.member_class_assumed


def test_synthetic_schedule_is_fictional_and_uses_same_rounding() -> None:
    c = calc()
    r = c.resolve(market("SYN-X"), AFTER)
    assert r.verified
    assert r.fictional
    assert r.schedule_id == "synthetic-fictional-v1"
    assert r.member_class is MemberClass.NON_DIRECT
    o = c.order_fees(r, [(D("0.30"), D(1))])
    assert o is not None
    assert (o.fills[0].trade_fee, o.fills[0].rounding_fee, o.total_net_fee) == (
        D("0.0105"),
        D("0.0095"),
        D("0.02"),
    )


def test_unknown_venue_is_unverified() -> None:
    m = market("X", provenance=Provenance(source_kind=DataSourceKind.REPLAY, source_id="file"))
    r = calc().resolve(m, AFTER)
    assert not r.verified
    assert FeeReason.UNKNOWN_VENUE in r.reasons
    assert not r.can_estimate


def test_kalshi_schedule_file_contents() -> None:
    s = load_schedule(KALSHI)
    assert s.effective_from == datetime(2026, 7, 7, tzinfo=UTC)
    assert s.effective_to is None
    assert s.completeness.value == "PARTIAL"
    assert s.verification_status.value == "VERIFIED_AGAINST_PUBLISHED_SCHEDULE"
    assert (s.taker_coefficient, s.maker_coefficient) == (D("0.07"), D("0.0175"))
    assert (s.default_taker_multiplier, s.default_maker_multiplier) == (D(1), D(0))
    rows = {
        (r.series, r.combo_classification): (r.maker_multiplier, r.taker_multiplier)
        for r in s.exceptions
    }
    assert rows == {
        ("KXCPI", None): (D(1), D(1)),
        ("KXFED", None): (D(1), D(1)),
        ("KXMLBGAME", None): (D(1), D(1)),
        ("KXNFLGAME", None): (D(1), D(1)),
        ("KXBALLONDOR", None): (D(1), D(1)),
        ("KXBTCY", None): (D(0), D(0)),
        ("KXETHY", None): (D(0), D(0)),
        ("KXDOED", None): (D(0), D(0)),
        ("KXMVE", "specified"): (D(2), D(1)),
    }

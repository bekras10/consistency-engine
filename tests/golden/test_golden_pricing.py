"""Golden pricing fixtures A-F and J (spec section 15). Data: ``fixtures/golden/*.yaml``.

Fee arithmetic used throughout (SYNTHETIC-FICTIONAL schedule: taker coefficient 0.05, M = 1,
NON_DIRECT balance precision $0.01; one buy order per leg, one fill per price level):

    model_fee      = 0.05 * C * P * (1 - P)
    trade_fee      = ceil_6dp(model_fee)
    aligned_change = floor_0.01(-P*C - trade_fee)
    rounding_fee   = (-P*C - trade_fee) - aligned_change
    rebate         = floor_0.01(min(accumulator, trade_fee + rounding_fee))   (0 for one fill
                     whose rounding is < 1c)
    net_fee        = trade_fee + rounding_fee - rebate

For integer C and cent prices, P*C is whole cents, so a single-fill leg's net fee is simply
ceil_cent(model_fee). Execution-adjusted profit subtracts slippage 0.005 x contracts per leg and a
fee buffer of one balance-precision increment ($0.01) per leg.
"""

from __future__ import annotations

import pytest

from consistency_core.models.detection import Classification
from consistency_core.pricing.portfolio import Template, canonical_portfolios
from consistency_core.relationships.discovery import discover
from tests.factories import T0
from tests.golden.support import catalog, check_expected, find_relationship, load, run

pytestmark = pytest.mark.golden


def test_A_correct_implication() -> None:
    """A: GE3 => GE2 is discovered and VERIFIED. NO(A) $0.30 + YES(B) $0.35 = $0.65 premium for a
    guaranteed $1 (admissible states (0,0), (0,1), (1,1) all pay >= 1): gross $0.35 per unit.

    Quantity domain 10..100 (min size 10, depth 100 each). At Q = 100:
      premium = 30.00 + 35.00 = 65.00; gross = 100 - 65 = 35.00
      NO(A):  model 0.05*100*0.30*0.70 = 1.05   -> trade 1.05, aligned -31.05, rounding 0
      YES(B): model 0.05*100*0.35*0.65 = 1.1375 -> trade 1.1375, aligned -36.14, rounding 0.0025
      net fees = 1.05 + 1.14 = 2.19; net = 35 - 2.19 = 32.81
      execution-adjusted = 32.81 - (100*2*0.005 = 1.00) - (2*0.01) = 31.79 >= 0.01*100
    Increasing Q by one adds ~0.318 of execution-adjusted profit, so Q* = 100.
    """
    fx = load("A")
    ev, rel = run(fx, fx["cases"]["default"])
    assert rel.is_verified
    check_expected(ev, fx["cases"]["default"]["expected"])


def test_A_single_unit_fees() -> None:
    """A at Q = 1 (the per-unit view): each leg pays a 2c net fee.
    NO(A):  model 0.0105   -> trade 0.010500; -0.30 - 0.0105 = -0.3105 -> aligned -0.32,
            rounding 0.0095, net 0.02, leg cost 0.32
    YES(B): model 0.011375 -> trade 0.011375; -0.361375 -> aligned -0.37, rounding 0.008625,
            net 0.02, leg cost 0.37
    net = 1 - 0.65 - 0.04 = 0.31; execution-adjusted = 0.31 - 0.01 - 0.02 = 0.28
    """
    fx = load("A")
    ev, _ = run(fx, fx["cases"]["one_unit"])
    check_expected(ev, fx["cases"]["one_unit"]["expected"])


def test_B_exhaustive_basket() -> None:
    """B: complete 3-outcome event -> EXHAUSTIVE_PARTITION (exactly one YES; every admissible
    state pays exactly $1). YES asks 0.25 + 0.30 + 0.35 = 0.90 -> gross $0.10 per unit.

    At Q = 100: premium 90.00, gross 10.00
      fees: ceil_cent(0.05*100*0.25*0.75 = 0.9375) = 0.94
            ceil_cent(0.05*100*0.30*0.70 = 1.05)   = 1.05
            ceil_cent(0.05*100*0.35*0.65 = 1.1375) = 1.14      total 3.13
      net = 10 - 3.13 = 6.87; execution-adjusted = 6.87 - 1.50 - 0.03 = 5.34
    Q = 99 gives fees 0.93 + 1.04 + 1.13 = 3.10 and execution-adjusted 5.285 < 5.34, so Q* = 100.
    The NO basket has no YES bids to lift: INSUFFICIENT_LIQUIDITY (NO_ASKS).
    """
    fx = load("B")
    for name in ("yes_basket", "no_basket_without_yes_bids"):
        ev, rel = run(fx, fx["cases"][name])
        assert rel.is_verified
        assert rel.exhaustive
        check_expected(ev, fx["cases"][name]["expected"])


def test_C_non_exhaustive_yes_basket_rejected() -> None:
    """C: three exclusive outcomes, but a fourth (W) is possible. The relationship is
    MUTUALLY_EXCLUSIVE (not exhaustive), so:
      * the canonical constructions do NOT include a YES basket;
      * evaluating the YES basket anyway: admissible states include "all NO" (W happens), which
        pays $0 -> min payoff 0, edge 0 - 0.90 = -0.90 -> NO_OPPORTUNITY, although the naive
        exhaustive reading would show +$0.10;
      * the NO basket pays >= N - 1 = 2 in every state; NO asks 0.80 + 0.75 + 0.70 = 2.25
        -> edge -0.25 -> NO_OPPORTUNITY.
    """
    fx = load("C")
    cat = catalog(fx)
    rel = find_relationship(discover(cat, as_of=T0), fx["cases"]["no_basket"]["relationship"])
    assert rel.is_verified
    assert not rel.exhaustive
    templates = [p.template for p in canonical_portfolios(rel)]
    assert templates == [Template.NO_BASKET]
    for name in ("yes_basket_is_not_a_guarantee", "no_basket"):
        ev, _ = run(fx, fx["cases"][name])
        check_expected(ev, fx["cases"][name]["expected"])


def test_D_insufficient_depth() -> None:
    """D: NO(A) has 100 contracts, YES(B) only 3. Max depth-supported basket = min(100, 3) = 3.
    A 10-basket request walks NO(A) for 10 (filled) and YES(B) for 3 filled / 7 unfilled ->
    INSUFFICIENT_LIQUIDITY (DEPTH_BELOW_MINIMUM); the engine never claims 10 are supported.

    With minimum size 1 the domain is 1..3 and Q* = 3:
      premium 0.90 + 1.05 = 1.95, gross 3 - 1.95 = 1.05
      fees ceil_cent(0.0315) + ceil_cent(0.034125) = 0.04 + 0.04 = 0.08 -> net 0.97
      execution-adjusted 0.97 - 0.03 - 0.02 = 0.92
    """
    fx = load("D")
    for name in ("ten_requested", "small_size_allowed"):
        ev, _ = run(fx, fx["cases"][name])
        check_expected(ev, fx["cases"][name]["expected"])


def test_E_fees_remove_profitability() -> None:
    """E: premium 0.49 + 0.49 = 0.98, gross $0.02 per unit (positive pre-fee edge).
    Fees per unit before rounding: 2 x 0.05 x 0.49 x 0.51 = 0.02499 > 0.02, so every quantity is
    negative after fees. Best net over Q in 10..100:
      Q = 10: gross 0.20; each leg ceil_cent(0.12495) = 0.13 -> fees 0.26 -> net -0.06
      Q = 11, 12 also give -0.06; Q = 13 gives -0.08; ties resolve to the smaller Q = 10.
    -> THEORETICAL_ONLY (FEES_EXCEED_EDGE).
    """
    fx = load("E")
    ev, _ = run(fx, fx["cases"]["default"])
    check_expected(ev, fx["cases"]["default"]["expected"])


def test_F_stale_books() -> None:
    """F: B's profitable basket with books last confirmed 5000 ms ago (> 2000 ms) ->
    STALE_DATA (BOOK_TOO_OLD). Variant: ages 100 / 800 / 300 ms are each fresh, but the skew
    800 - 100 = 700 ms > 500 ms -> STALE_DATA (CROSS_MARKET_SKEW)."""
    fx = load("F")
    for name in ("stale", "skewed"):
        ev, _ = run(fx, fx["cases"][name])
        check_expected(ev, fx["cases"][name]["expected"])
        assert ev.classification is Classification.STALE_DATA


def test_J_fractional_precision() -> None:
    """J: 3.33 baskets, 4-dp prices, 0.01 quantity increments. All values are exact Decimals.
    premium NO(A) = 0.3333 x 3.33 = 1.109889; YES(B) = 0.4444 x 3.33 = 1.479852
    total premium 2.589741; min payoff 3.33; gross 0.740259
    NO(A):  model 0.05 x 3.33 x 0.3333 x 0.6667 = 0.036998149815 -> trade 0.036999
            -1.109889 - 0.036999 = -1.146888 -> aligned -1.15, rounding 0.003112,
            net 0.040111, leg cost 1.15
    YES(B): model 0.05 x 3.33 x 0.4444 x 0.5556 = 0.04111028856 -> trade 0.041111
            -1.479852 - 0.041111 = -1.520963 -> aligned -1.53, rounding 0.009037,
            net 0.050148, leg cost 1.53
    fees 0.090259; total cost 2.68; net 3.33 - 2.68 = 0.65
    execution-adjusted 0.65 - (3.33 x 2 x 0.005 = 0.0333) - 0.02 = 0.5967 >= 0.0333
    per-unit net 0.65 / 3.33 = 0.195195... (informational, floored to 0.19519519)
    """
    fx = load("J")
    ev, _ = run(fx, fx["cases"]["default"])
    check_expected(ev, fx["cases"]["default"]["expected"])


def test_J_serialization_has_no_float_drift() -> None:
    """The certificate is canonical JSON with every Decimal as a fixed-point string; parsing it
    back yields no JSON floats, and key money values round-trip exactly."""
    import json
    from decimal import Decimal

    fx = load("J")
    ev, _ = run(fx, fx["cases"]["default"])
    text = ev.certificate_json()

    def walk(x: object) -> None:
        assert not isinstance(x, float), x
        if isinstance(x, dict):
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    data = json.loads(text)
    walk(data)
    evaluation = data["evaluation"]
    assert evaluation["total_premium"] == "2.589741"
    assert Decimal(evaluation["net_profit"]) == Decimal("0.65")
    assert evaluation["legs"][0]["fees"]["fills"][0]["trade_fee"] == "0.036999"
    assert evaluation["legs"][0]["fees"]["fills"][0]["model_fee"] == "0.036998149815"
    # determinism: same inputs -> byte-identical certificate and hash
    ev2, _ = run(fx, fx["cases"]["default"])
    assert ev2.certificate_json() == text
    assert ev2.certificate_hash == ev.certificate_hash

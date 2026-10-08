"""Evaluator edge cases: precedence, gates, scanner role, live-fee gating, quantity search."""

from __future__ import annotations

import copy
import json
from decimal import Decimal
from typing import Any

import pytest

from consistency_core.fees import Role
from consistency_core.models import MarketStatus, Side, SyncStatus
from consistency_core.models.common import DataSourceKind, Provenance
from consistency_core.models.detection import Classification
from consistency_core.models.relationship import VerificationStatus
from consistency_core.pricing.certificate import EvaluationConfig
from consistency_core.pricing.evaluator import Evaluation, evaluate
from consistency_core.pricing.portfolio import Leg, Portfolio, Template, parse_strategy_id
from consistency_core.relationships.discovery import discover
from tests.factories import T0
from tests.golden.support import (
    NOW_MS,
    books,
    catalog,
    fee_calculator,
    find_relationship,
    load,
    portfolio,
)

D = Decimal
A_REL = {"type": "implication", "members": ["GOLD-A-GE3", "GOLD-A-GE2"]}
A_PF = {"template": "implication", "legs": [["GOLD-A-GE3", "no"], ["GOLD-A-GE2", "yes"]]}


def run_a(
    *,
    fx: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
    duration: int | None = 5000,
    markets_update: dict[str, dict[str, Any]] | None = None,
    rel_update: dict[str, Any] | None = None,
    book_update: dict[str, dict[str, Any]] | None = None,
) -> Evaluation:
    fx = fx or load("A")
    cat = catalog(fx)
    rel = find_relationship(discover(cat, as_of=T0), A_REL)
    if rel_update:
        rel = rel.model_copy(update=rel_update)
    markets = cat.markets_by_id()
    for mid, upd in (markets_update or {}).items():
        markets[mid] = markets[mid].model_copy(update=upd)
    bk = books(fx, NOW_MS)
    for mid, upd in (book_update or {}).items():
        bk[mid] = bk[mid].model_copy(update=upd)
    return evaluate(
        rel,
        portfolio(A_PF),
        markets=markets,
        books=bk,
        now_ms=NOW_MS,
        fees=fee_calculator(),
        config=EvaluationConfig.model_validate(config or {}),
        observed_duration_ms=duration,
    )


def test_baseline_is_candidate_and_trace_complete() -> None:
    ev = run_a()
    assert ev.classification is Classification.FEE_ADJUSTED_CANDIDATE
    assert [t.outcome for t in ev.certificate.trace] == ["pass"] * 10


def test_unverified_relationship_is_invalid_first() -> None:
    """Precedence: the relationship check runs before everything, even with a stale book."""
    ev = run_a(
        rel_update={"verification_status": VerificationStatus.CANDIDATE_REVIEW},
        book_update={"GOLD-A-GE3": {"sync_status": SyncStatus.UNSYNCHRONIZED}},
    )
    assert ev.classification is Classification.INVALID_RELATIONSHIP
    assert ev.reason_codes == ("RELATIONSHIP_NOT_VERIFIED",)
    assert [t.outcome for t in ev.certificate.trace][1:] == ["not_reached"] * 9


def test_rules_changed_invalidates() -> None:
    ev = run_a(markets_update={"GOLD-A-GE2": {"settlement_rules": "Rules were edited"}})
    assert ev.classification is Classification.INVALID_RELATIONSHIP
    assert "RULES_CHANGED" in ev.reason_codes


def test_unsynchronized_beats_stale() -> None:
    ev = run_a(
        book_update={"GOLD-A-GE3": {"sync_status": SyncStatus.UNSYNCHRONIZED}},
        config={"max_book_age_ms": 1},
    )
    assert ev.classification is Classification.UNSYNCHRONIZED_DATA


def test_closed_market_is_insufficient_liquidity() -> None:
    ev = run_a(markets_update={"GOLD-A-GE2": {"status": MarketStatus.CLOSED}})
    assert ev.classification is Classification.INSUFFICIENT_LIQUIDITY
    assert ev.reason_codes == ("MARKET_NOT_OPEN",)


def test_target_off_quantity_grid() -> None:
    ev = run_a(config={"target_quantity": "2.5", "minimum_available_quantity": "1"})
    assert ev.classification is Classification.INSUFFICIENT_LIQUIDITY
    assert ev.reason_codes == ("TARGET_OFF_QUANTITY_GRID",)


@pytest.mark.parametrize(
    ("duration", "codes"),
    [(None, ("DURATION_UNKNOWN",)), (999, ("DURATION_BELOW_MINIMUM",)), (1000, ())],
)
def test_duration_gate(duration: int | None, codes: tuple[str, ...]) -> None:
    ev = run_a(duration=duration)
    expected = Classification.DEPTH_SUPPORTED if codes else Classification.FEE_ADJUSTED_CANDIDATE
    assert ev.classification is expected
    assert ev.reason_codes == codes


def test_maker_assumption_never_yields_candidate() -> None:
    """Scanner default is TAKER on every leg. A maker evaluation is flagged non-default and can
    never be FEE_ADJUSTED_CANDIDATE, however good its numbers."""
    ev = run_a(config={"execution_role": Role.MAKER})
    assert ev.classification is Classification.DEPTH_SUPPORTED
    assert ev.reason_codes == ("NON_DEFAULT_MAKER_ASSUMPTION",)


def test_real_venue_market_is_fee_unverified() -> None:
    """A KALSHI_AUTHORIZED-provenance market in a series absent from the PARTIAL table, with
    unknown member class / intermediary fees -> FEE_UNVERIFIED (never a candidate)."""
    real = Provenance(source_kind=DataSourceKind.KALSHI_AUTHORIZED, source_id="offline")
    ev = run_a(
        markets_update={
            "GOLD-A-GE3": {"provenance": real, "series_id": "KXNOTLISTED"},
            "GOLD-A-GE2": {"provenance": real, "series_id": "KXNOTLISTED"},
        }
    )
    assert ev.classification is Classification.FEE_UNVERIFIED
    assert ev.reason_codes[0] == "FEE_UNVERIFIED"
    assert "FEE:SERIES_NOT_IN_PARTIAL_TABLE" in ev.reason_codes
    assert "FEE:MEMBER_CLASS_UNKNOWN" in ev.reason_codes
    assert "FEE:INTERMEDIARY_FEES_UNKNOWN" in ev.reason_codes


def _thin_top_deep_book() -> dict[str, Any]:
    """20 contracts at 0.30/0.35 (per-unit execution-adjusted ~0.32), then 1500 at 0.33/0.635
    (per unit: gross 0.035 - fees ~0.0226 - slippage 0.01 = ~0.0024 < minimum edge 0.01)."""
    fx = copy.deepcopy(load("A"))
    fx["books"] = {
        "GOLD-A-GE3": {"yes_bids": [["0.70", "20"], ["0.67", "1500"]]},
        "GOLD-A-GE2": {"no_bids": [["0.65", "20"], ["0.365", "1500"]]},
    }
    return fx


def test_edge_gate_picks_best_qualifying_quantity() -> None:
    """Regression (found by golden Test I / S8): the per-unit edge gate must be applied per
    quantity. Absolute execution-adjusted profit keeps rising to Q = 1520 (~9.9) where it is far
    below 0.01 x 1520 = 15.2, but smaller quantities clear the gate; the reported quantity is the
    most profitable qualifying one, not the unconstrained maximum."""
    ev = run_a(fx=_thin_top_deep_book())
    assert ev.classification is Classification.FEE_ADJUSTED_CANDIDATE, ev.reason_codes
    cap = ev.certificate.capacity
    e = ev.certificate.evaluation
    assert cap is not None and e is not None
    assert cap.max_supported_quantity == D(1520)
    assert cap.method == "exhaustive"
    assert cap.best_execution_quantity is not None
    assert D(700) < cap.best_execution_quantity < D(900)
    assert e.execution_adjusted_profit >= D("0.01") * e.quantity
    assert cap.edge_qualifying_points is not None and 0 < cap.edge_qualifying_points < 1511
    at_max = run_a(fx=_thin_top_deep_book(), config={"target_quantity": "1520"})
    assert at_max.classification is Classification.DEPTH_SUPPORTED
    assert at_max.reason_codes == ("EDGE_BELOW_MINIMUM",)


def test_breakpoint_search_agrees_with_exhaustive() -> None:
    """Forcing the breakpoint method on the same book keeps the classification and the pre-fee
    optimum (the pre-fee objective is piecewise linear with kinks only at breakpoints)."""
    full = run_a(fx=_thin_top_deep_book())
    bp = run_a(fx=_thin_top_deep_book(), config={"exhaustive_search_limit": 10})
    assert bp.certificate.capacity is not None and full.certificate.capacity is not None
    assert bp.certificate.capacity.method == "breakpoints"
    assert bp.certificate.capacity.points_evaluated < full.certificate.capacity.points_evaluated
    assert bp.classification is full.classification
    assert (
        bp.certificate.capacity.best_gross_quantity == full.certificate.capacity.best_gross_quantity
    )
    bp_e = bp.certificate.evaluation
    assert bp_e is not None
    assert bp_e.execution_adjusted_profit >= D("0.01") * bp_e.quantity


def test_certificate_is_deterministic_and_hash_excludes_latency() -> None:
    a = run_a()
    b = run_a()
    assert a.certificate_json() == b.certificate_json()
    assert a.certificate_hash == b.certificate_hash
    data = json.loads(a.certificate_json())
    assert "processing_latency_ns" not in json.dumps(data)


def test_strategy_id_round_trip() -> None:
    pf = Portfolio(
        template=Template.CUSTOM,
        legs=(
            Leg(market_id="SYN-AxB-1", side=Side.YES),
            Leg(market_id="SYN-C", side=Side.NO, ratio=D(2)),
        ),
    )
    assert pf.strategy_id == "custom|YES:SYN-AxB-1,NO:SYN-Cx2"
    assert parse_strategy_id(pf.strategy_id) == pf
    with pytest.raises(ValueError, match="not a canonical strategy id"):
        parse_strategy_id("implication|YES:A,NO:Bx1")

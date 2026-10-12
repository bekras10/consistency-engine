"""Real-venue fees stay unverified until the live gate is satisfied."""

from __future__ import annotations

from consistency_core.fees.calculator import FeeAssumptions, IntermediaryFees
from consistency_core.fees.rounding import MemberClass
from consistency_core.models.common import DataSourceKind, Provenance
from consistency_core.models.detection import Classification
from consistency_core.pricing.certificate import EvaluationConfig
from consistency_core.relationships.discovery import discover
from tests.factories import T0
from tests.golden.support import NOW_MS, books, catalog, fee_calculator, load, portfolio

A_REL = {"type": "implication", "members": ["GOLD-A-GE3", "GOLD-A-GE2"]}
A_PF = {"template": "implication", "legs": [["GOLD-A-GE3", "no"], ["GOLD-A-GE2", "yes"]]}


def _kalshi_case(assumptions: FeeAssumptions):
    from consistency_core.pricing.evaluator import evaluate

    fx = load("A")
    cat = catalog(fx)
    provenance = Provenance(
        source_kind=DataSourceKind.KALSHI_AUTHORIZED,
        source_id="kalshi:production",
        retrieved_at=T0,
    )
    markets = {
        market.market_id: market.model_copy(update={"series_id": "KXCPI", "provenance": provenance})
        for market in cat.markets
    }
    rel = next(
        item
        for item in discover(cat, as_of=T0)
        if item.relationship_type.value == A_REL["type"] and list(item.members) == A_REL["members"]
    )
    return evaluate(
        rel,
        portfolio(A_PF),
        markets=markets,
        books=books(fx, NOW_MS),
        now_ms=NOW_MS,
        fees=fee_calculator(),
        config=EvaluationConfig(fee_assumptions=assumptions),
        observed_duration_ms=5000,
    )


def test_verified_kalshi_schedule_can_justify_a_fee_adjusted_candidate() -> None:
    ev = _kalshi_case(
        FeeAssumptions(
            member_class=MemberClass.DIRECT,
            intermediary_fees=IntermediaryFees.EXPLICITLY_EXCLUDED,
            schedule_revisions_checked=True,
        )
    )
    assert ev.certificate.fictional_fees is False
    assert ev.certificate.fees is not None
    assert all(item.verified for item in ev.certificate.fees)
    assert ev.classification is Classification.FEE_ADJUSTED_CANDIDATE


def test_unknown_kalshi_fees_cannot_be_a_fee_verified_positive() -> None:
    ev = _kalshi_case(FeeAssumptions())
    assert ev.classification is Classification.FEE_UNVERIFIED
    assert ev.classification is not Classification.FEE_ADJUSTED_CANDIDATE
    assert ev.certificate.fees is not None
    assert any(not item.verified for item in ev.certificate.fees)
    assert all(item.fictional is False for item in ev.certificate.fees)

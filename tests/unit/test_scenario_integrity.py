"""Settlement-scenario integrity: supplied/overridden scenario specs are validated against the
admissible set derived from the relationship semantics (hardening pass, P1)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from consistency_core.models import Side
from consistency_core.models.detection import Classification
from consistency_core.models.market import Catalog, Market
from consistency_core.models.relationship import (
    Relationship,
    RelationshipType,
    ScenarioKind,
    ScenarioSpec,
    VerificationStatus,
)
from consistency_core.models.settlement import SettlementSpec
from consistency_core.pricing.evaluator import evaluate
from consistency_core.pricing.portfolio import Leg, Portfolio, Template
from consistency_core.relationships.discovery import discover
from consistency_core.relationships.review import ManualReview, ReviewFile, apply_reviews
from consistency_core.relationships.scenarios import ScenarioSpace
from tests.factories import T0, market
from tests.golden.support import NOW_MS, books, catalog, fee_calculator, load

V = VerificationStatus.VERIFIED
RT = RelationshipType
REMOVES = "SCENARIO_OVERRIDE_REMOVES_ADMISSIBLE_STATES"

COMMON: dict[str, Any] = {
    "underlying_id": "integrity-index",
    "measurement": "level",
    "location": "synthetic",
    "observation_window": "2026-08",
    "settlement_source": "Synthetic Bureau",
    "methodology": "published",
    "unit": "points",
    "early_close_policy": "none",
    "exceptional_resolution": [],
}


def _threshold(mid: str, value: str, comparator: str = ">=") -> Market:
    spec = SettlementSpec.model_validate(
        {**COMMON, "terms": {"kind": "threshold", "comparator": comparator, "value": value}}
    )
    return market(mid, settlement=spec)


def _interval(mid: str, lo: str | None, hi: str | None) -> Market:
    terms: dict[str, Any] = {"kind": "interval"}
    if lo is not None:
        terms["lower"] = lo
    if hi is not None:
        terms["upper"] = hi
    spec = SettlementSpec.model_validate({**COMMON, "terms": terms})
    return market(mid, event_id="SYN-BINS", settlement=spec)


def _explicit(*states: tuple[int, ...]) -> ScenarioSpec:
    return ScenarioSpec(kind=ScenarioKind.EXPLICIT, explicit_states=tuple(states))


def _review(
    cat: Catalog,
    rtype: RelationshipType,
    members: tuple[str, ...],
    *,
    spec: ScenarioSpec | None = None,
    exhaustive: bool = False,
    metadata: bool = True,
    **extra: Any,
) -> ManualReview:
    by = cat.markets_by_id()
    data: dict[str, Any] = {
        "review_id": f"review-{rtype.value}",
        "relationship_type": rtype,
        "members": members,
        "decision": V,
        "reviewer": "integrity-test-reviewer",
        "reviewed_at": T0,
        "reasoning": "reviewer asserts the relationship",
        "rules_hashes": {m: by[m].rules_hash for m in members},
        "scenario_spec": spec,
        "exhaustive": exhaustive,
    }
    if metadata:
        data |= {
            "scenario_justification": "reviewer claims a stronger relationship",
            "scenario_evidence": ("ticket:INTEGRITY-1",),
        }
    data |= extra
    return ManualReview.model_validate(data)


def _apply(cat: Catalog, *reviews: ManualReview) -> list[Relationship]:
    rels, _ = apply_reviews(
        discover(cat, as_of=T0), ReviewFile(schema_version=1, reviews=reviews), cat, as_of=T0
    )
    return rels


def _find(
    rels: list[Relationship], rtype: RelationshipType, members: tuple[str, ...]
) -> Relationship:
    hits = [r for r in rels if r.relationship_type is rtype and set(r.members) == set(members)]
    assert len(hits) == 1, [(r.relationship_type, r.members) for r in rels]
    return hits[0]


def _states(rel: Relationship) -> set[tuple[int, ...]]:
    return set(ScenarioSpace(rel.scenario_spec, len(rel.members)).iter_states())


def _codes(rel: Relationship) -> set[str]:
    return {e.check for e in rel.evidence}


# ---------------------------------------------------------------- derived admissible sets
def test_implication_admits_exactly_00_01_11() -> None:
    from consistency_core.relationships.scenario_integrity import derived_states

    assert derived_states(RT.IMPLICATION, 2, exhaustive=False) == {(0, 0), (0, 1), (1, 1)}
    cat = catalog(load("A"))
    rel = _find(discover(cat, as_of=T0), RT.IMPLICATION, ("GOLD-A-GE3", "GOLD-A-GE2"))
    assert rel.members == ("GOLD-A-GE3", "GOLD-A-GE2")
    assert _states(rel) == {(0, 0), (0, 1), (1, 1)}


def test_derived_sets_for_every_type() -> None:
    from consistency_core.relationships.scenario_integrity import derived_states

    assert derived_states(RT.EQUIVALENT, 2, exhaustive=False) == {(0, 0), (1, 1)}
    assert derived_states(RT.NESTED_THRESHOLDS, 3, exhaustive=False) == {
        (0, 0, 0),
        (0, 0, 1),
        (0, 1, 1),
        (1, 1, 1),
    }
    one_hot = {(1, 0, 0), (0, 1, 0), (0, 0, 1)}
    assert derived_states(RT.MUTUALLY_EXCLUSIVE, 3, exhaustive=False) == one_hot | {(0, 0, 0)}
    assert derived_states(RT.EXHAUSTIVE_PARTITION, 3, exhaustive=True) == one_hot
    assert derived_states(RT.DISJOINT_INTERVALS, 3, exhaustive=False) == one_hot | {(0, 0, 0)}
    assert derived_states(RT.DISJOINT_INTERVALS, 3, exhaustive=True) == one_hot


# ---------------------------------------------------------------- implication: removing 01
def test_review_removing_01_from_implication_is_not_verified() -> None:
    cat = catalog(load("A"))
    members = ("GOLD-A-GE3", "GOLD-A-GE2")
    rel = _find(
        _apply(cat, _review(cat, RT.IMPLICATION, members, spec=_explicit((0, 0), (1, 1)))),
        RT.IMPLICATION,
        members,
    )
    assert rel.verification_status is not V
    assert REMOVES in _codes(rel)
    assert rel.scenario_provenance.source == "overridden"
    assert rel.scenario_provenance.removed_states == ((0, 1),)


def _yes_a_no_b_books(fx: dict[str, Any]) -> dict[str, Any]:
    fx["books"] = {
        "GOLD-A-GE3": {"no_bids": [["0.70", "100"]]},  # YES(A) ask 0.30
        "GOLD-A-GE2": {"yes_bids": [["0.65", "100"]]},  # NO(B) ask 0.35
    }
    return fx


YES_A_NO_B = Portfolio(
    template=Template.CUSTOM,
    legs=(Leg(market_id="GOLD-A-GE3", side=Side.YES), Leg(market_id="GOLD-A-GE2", side=Side.NO)),
)


def _evaluate(rel: Relationship, fx: dict[str, Any], cat: Catalog) -> Any:
    return evaluate(
        rel,
        YES_A_NO_B,
        markets=cat.markets_by_id(),
        books=books(fx, NOW_MS),
        now_ms=NOW_MS,
        fees=fee_calculator(),
        observed_duration_ms=5000,
    )


def test_yes_a_no_b_on_implication_worst_case_is_zero_via_evaluator() -> None:
    """YES(A) + NO(B) on A => B pays 1 in 00, 0 in 01, 1 in 11: worst case $0."""
    fx = _yes_a_no_b_books(load("A"))
    cat = catalog(fx)
    rel = _find(discover(cat, as_of=T0), RT.IMPLICATION, ("GOLD-A-GE3", "GOLD-A-GE2"))
    ev = _evaluate(rel, fx, cat)
    payoff = ev.certificate.payoff
    assert payoff is not None
    assert payoff.min_payoff_per_unit == 0
    assert {tuple(s.state.values()): s.payoff_per_unit for s in payoff.states or ()} == {
        (0, 0): 1,
        (0, 1): 0,
        (1, 1): 1,
    }
    assert ev.classification is Classification.NO_OPPORTUNITY
    assert "NO_GUARANTEED_PAYOFF" in ev.reason_codes


def test_tampered_scenario_spec_on_verified_relationship_is_invalid() -> None:
    """Replacing the spec on a VERIFIED relationship (bypassing review) must neither verify nor
    let the certificate claim a $1 guarantee."""
    fx = _yes_a_no_b_books(load("A"))
    cat = catalog(fx)
    rel = _find(discover(cat, as_of=T0), RT.IMPLICATION, ("GOLD-A-GE3", "GOLD-A-GE2"))
    tampered = rel.model_copy(update={"scenario_spec": _explicit((0, 0), (1, 1))})
    ev = _evaluate(tampered, fx, cat)
    assert ev.classification is Classification.INVALID_RELATIONSHIP
    assert REMOVES in ev.reason_codes
    assert ev.certificate.payoff is not None
    assert ev.certificate.payoff.min_payoff_per_unit == 0


def test_yes_a_no_b_review_path_never_guarantees_one_dollar() -> None:
    fx = _yes_a_no_b_books(load("A"))
    cat = catalog(fx)
    members = ("GOLD-A-GE3", "GOLD-A-GE2")
    rel = _find(
        _apply(cat, _review(cat, RT.IMPLICATION, members, spec=_explicit((0, 0), (1, 1)))),
        RT.IMPLICATION,
        members,
    )
    ev = _evaluate(rel, fx, cat)
    assert ev.classification is Classification.INVALID_RELATIONSHIP
    assert ev.certificate.payoff is not None
    assert ev.certificate.payoff.min_payoff_per_unit == 0


# ---------------------------------------------------------------- justified stronger claims
def _equivalent_pair() -> Catalog:
    return Catalog(markets=(_threshold("SYN-E1", "3.0"), _threshold("SYN-E2", "3.0")))


def test_override_justified_by_separately_verified_equivalence() -> None:
    cat = _equivalent_pair()
    rels = _apply(
        cat,
        _review(
            cat,
            RT.IMPLICATION,
            ("SYN-E1", "SYN-E2"),
            spec=_explicit((0, 0), (1, 1)),
            declared_removed_states=((0, 1),),
        ),
    )
    eq = _find(rels, RT.EQUIVALENT, ("SYN-E1", "SYN-E2"))
    assert eq.verification_status is V
    imp = _find(rels, RT.IMPLICATION, ("SYN-E1", "SYN-E2"))
    assert imp.verification_status is V
    assert imp.scenario_provenance.source == "overridden"
    assert imp.scenario_provenance.justified_by == (eq.relationship_id,)


def test_override_without_metadata_is_not_verified() -> None:
    cat = _equivalent_pair()
    rel = _find(
        _apply(
            cat,
            _review(
                cat,
                RT.IMPLICATION,
                ("SYN-E1", "SYN-E2"),
                spec=_explicit((0, 0), (1, 1)),
                metadata=False,
            ),
        ),
        RT.IMPLICATION,
        ("SYN-E1", "SYN-E2"),
    )
    assert rel.verification_status is not V
    assert "SCENARIO_OVERRIDE_METADATA_MISSING" in _codes(rel)


def test_override_with_wrong_declared_diff_is_not_verified() -> None:
    cat = _equivalent_pair()
    rel = _find(
        _apply(
            cat,
            _review(
                cat,
                RT.IMPLICATION,
                ("SYN-E1", "SYN-E2"),
                spec=_explicit((0, 0), (1, 1)),
                declared_removed_states=((1, 1),),
            ),
        ),
        RT.IMPLICATION,
        ("SYN-E1", "SYN-E2"),
    )
    assert rel.verification_status is not V
    assert "SCENARIO_OVERRIDE_DIFF_MISMATCH" in _codes(rel)


def test_superset_override_is_conservative_accepted_and_recorded() -> None:
    cat = catalog(load("A"))
    members = ("GOLD-A-GE3", "GOLD-A-GE2")
    rel = _find(
        _apply(
            cat,
            _review(
                cat,
                RT.IMPLICATION,
                members,
                spec=_explicit((0, 0), (0, 1), (1, 0), (1, 1)),
                declared_added_states=((1, 0),),
            ),
        ),
        RT.IMPLICATION,
        members,
    )
    assert rel.verification_status is V
    prov = rel.scenario_provenance
    assert prov.source == "overridden"
    assert prov.added_states == ((1, 0),)
    assert prov.removed_states == ()
    assert "SCENARIO_OVERRIDE_SUPERSET" in _codes(rel)


def test_equal_set_review_spec_stays_derived() -> None:
    """The Owls review supplies [[0,0],[0,1],[1,1]] for an implication: identical to the derived
    set, so it is recorded as derived (no stronger claim)."""
    cat = catalog(load("A"))
    members = ("GOLD-A-GE3", "GOLD-A-GE2")
    rel = _find(
        _apply(
            cat,
            _review(cat, RT.IMPLICATION, members, spec=_explicit((0, 0), (0, 1), (1, 1))),
        ),
        RT.IMPLICATION,
        members,
    )
    assert rel.verification_status is V
    assert rel.scenario_provenance.source == "derived"


# ---------------------------------------------------------------- other relationship types
def test_exclusive_group_cannot_drop_all_no_without_proven_exhaustiveness() -> None:
    cat = catalog(load("C"))
    members = ("GOLD-C-X", "GOLD-C-Y", "GOLD-C-Z")
    for review in (
        _review(cat, RT.MUTUALLY_EXCLUSIVE, members, exhaustive=True),
        _review(
            cat,
            RT.MUTUALLY_EXCLUSIVE,
            members,
            spec=ScenarioSpec(kind=ScenarioKind.CARDINALITY, min_yes=1, max_yes=1),
            declared_removed_states=((0, 0, 0),),
        ),
    ):
        rel = _find(_apply(cat, review), RT.MUTUALLY_EXCLUSIVE, members)
        assert rel.verification_status is not V
        assert REMOVES in _codes(rel)
        assert (0, 0, 0) in rel.scenario_provenance.removed_states


def _bins(exhaustive: bool) -> Catalog:
    lo = None if exhaustive else "0.1"
    hi = None if exhaustive else "0.4"
    return Catalog(
        markets=(
            _interval("SYN-B1", lo, "0.2"),
            _interval("SYN-B2", "0.2", "0.3"),
            _interval("SYN-B3", "0.3", hi),
        )
    )


def test_interval_override_cannot_drop_outside_all_bins_state() -> None:
    cat = _bins(exhaustive=False)
    members = ("SYN-B1", "SYN-B2", "SYN-B3")
    base = _find(discover(cat, as_of=T0), RT.DISJOINT_INTERVALS, members)
    assert base.verification_status is V
    assert not base.exhaustive
    rel = _find(
        _apply(
            cat,
            _review(
                cat,
                RT.DISJOINT_INTERVALS,
                base.members,
                spec=ScenarioSpec(kind=ScenarioKind.CARDINALITY, min_yes=1, max_yes=1),
                declared_removed_states=((0, 0, 0),),
            ),
        ),
        RT.DISJOINT_INTERVALS,
        members,
    )
    assert rel.verification_status is not V
    assert REMOVES in _codes(rel)


def test_interval_override_cannot_drop_a_bin_state() -> None:
    cat = _bins(exhaustive=True)
    members = ("SYN-B1", "SYN-B2", "SYN-B3")
    base = _find(discover(cat, as_of=T0), RT.DISJOINT_INTERVALS, members)
    assert base.exhaustive
    middle = tuple(1 if m == "SYN-B2" else 0 for m in base.members)
    rest = tuple(
        tuple(1 if j == i else 0 for j in range(3)) for i in range(3) if base.members[i] != "SYN-B2"
    )
    rel = _find(
        _apply(
            cat,
            _review(
                cat,
                RT.DISJOINT_INTERVALS,
                base.members,
                spec=_explicit(*rest),
                declared_removed_states=(middle,),
            ),
        ),
        RT.DISJOINT_INTERVALS,
        members,
    )
    assert rel.verification_status is not V
    assert REMOVES in _codes(rel)


def test_exhaustive_partition_cannot_drop_an_outcome_state() -> None:
    cat = catalog(load("B"))
    members = ("GOLD-B-X", "GOLD-B-Y", "GOLD-B-Z")
    rel = _find(
        _apply(
            cat,
            _review(
                cat,
                RT.EXHAUSTIVE_PARTITION,
                members,
                spec=_explicit((1, 0, 0), (0, 1, 0)),
                declared_removed_states=((0, 0, 1),),
            ),
        ),
        RT.EXHAUSTIVE_PARTITION,
        members,
    )
    assert rel.verification_status is not V
    assert REMOVES in _codes(rel)


def test_equivalence_override_cannot_drop_11() -> None:
    cat = _equivalent_pair()
    rel = _find(
        _apply(
            cat,
            _review(
                cat,
                RT.EQUIVALENT,
                ("SYN-E1", "SYN-E2"),
                spec=_explicit((0, 0)),
                declared_removed_states=((1, 1),),
            ),
        ),
        RT.EQUIVALENT,
        ("SYN-E1", "SYN-E2"),
    )
    assert rel.verification_status is not V
    assert REMOVES in _codes(rel)


def test_nested_threshold_override_cannot_drop_a_chain_state() -> None:
    cat = Catalog(
        markets=(
            _threshold("SYN-N3", "0.3"),
            _threshold("SYN-N2", "0.2"),
            _threshold("SYN-N1", "0.1"),
        )
    )
    base = _find(discover(cat, as_of=T0), RT.NESTED_THRESHOLDS, ("SYN-N1", "SYN-N2", "SYN-N3"))
    assert base.members == ("SYN-N3", "SYN-N2", "SYN-N1")
    rel = _find(
        _apply(
            cat,
            _review(
                cat,
                RT.NESTED_THRESHOLDS,
                base.members,
                spec=_explicit((0, 0, 1), (0, 1, 1), (1, 1, 1)),
                declared_removed_states=((0, 0, 0),),
            ),
        ),
        RT.NESTED_THRESHOLDS,
        base.members,
    )
    assert rel.verification_status is not V
    assert REMOVES in _codes(rel)


# ---------------------------------------------------------------- structural guarantees
def test_verified_relationship_with_unjustified_removal_cannot_be_constructed() -> None:
    from consistency_core.models.relationship import ScenarioProvenance

    cat = catalog(load("A"))
    rel = _find(discover(cat, as_of=T0), RT.IMPLICATION, ("GOLD-A-GE3", "GOLD-A-GE2"))
    data = rel.model_dump()
    data["scenario_spec"] = _explicit((0, 0), (1, 1)).model_dump()
    data["scenario_provenance"] = ScenarioProvenance(
        source="overridden", removed_states=((0, 1),)
    ).model_dump()
    with pytest.raises(ValueError, match="removes admissible states"):
        Relationship.model_validate(data)


def test_certificate_records_scenario_provenance() -> None:
    fx = load("A")
    cat = catalog(fx)
    rel = _find(discover(cat, as_of=T0), RT.IMPLICATION, ("GOLD-A-GE3", "GOLD-A-GE2"))
    pf = Portfolio(
        template=Template.IMPLICATION,
        legs=(
            Leg(market_id="GOLD-A-GE3", side=Side.NO),
            Leg(market_id="GOLD-A-GE2", side=Side.YES),
        ),
    )
    ev = evaluate(
        rel,
        pf,
        markets=cat.markets_by_id(),
        books=books(fx, NOW_MS),
        now_ms=NOW_MS,
        fees=fee_calculator(),
        observed_duration_ms=5000,
    )
    prov = ev.certificate.relationship.scenario_provenance
    assert prov.source == "derived"
    assert ev.certificate.payoff is not None
    assert ev.certificate.payoff.min_payoff_per_unit == Decimal(1)


def test_revalidate_demotes_override_when_justifier_is_no_longer_verified() -> None:
    from consistency_core.relationships.review import revalidate

    cat = _equivalent_pair()
    rels = _apply(
        cat,
        _review(
            cat,
            RT.IMPLICATION,
            ("SYN-E1", "SYN-E2"),
            spec=_explicit((0, 0), (1, 1)),
            declared_removed_states=((0, 1),),
        ),
    )
    eq = _find(rels, RT.EQUIVALENT, ("SYN-E1", "SYN-E2"))
    demoted = [
        r.invalidate("reviewer withdrew", T0, needs_review=False)
        if r.relationship_id == eq.relationship_id
        else r
        for r in rels
    ]
    out = revalidate(demoted, cat, as_of=T0)
    imp = _find(out, RT.IMPLICATION, ("SYN-E1", "SYN-E2"))
    assert imp.verification_status is not V
    assert imp.invalidation_reason is not None
    assert eq.relationship_id in imp.invalidation_reason

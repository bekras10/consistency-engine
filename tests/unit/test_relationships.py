"""Phase 5: relationship discovery, verification, manual review, scenarios (spec section 8)."""

from __future__ import annotations

import random
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

import pytest

from consistency_core.models.common import DataSourceKind, Provenance
from consistency_core.models.market import Catalog, Event, Market
from consistency_core.models.relationship import (
    EvidenceOutcome,
    Relationship,
    RelationshipType,
    ScenarioKind,
    ScenarioSpec,
    VerificationStatus,
)
from consistency_core.models.settlement import (
    Comparator,
    IntervalTerms,
    OutcomeTerms,
    PropositionTerms,
    RoundingConvention,
    RoundingMode,
    SettlementSpec,
    ThresholdTerms,
)
from consistency_core.money import dec
from consistency_core.relationships.discovery import discover
from consistency_core.relationships.review import (
    ManualReview,
    ReviewFile,
    apply_reviews,
    load_reviews,
    revalidate,
)
from consistency_core.relationships.scenarios import ScenarioSpace, ScenarioSpaceError
from consistency_simulation.families import FAMILY_BUILDERS, SIM_EPOCH, BuildContext
from tests.conftest import FIXTURES
from tests.factories import T0, market

V, C, R = (
    VerificationStatus.VERIFIED,
    VerificationStatus.CANDIDATE_REVIEW,
    VerificationStatus.REJECTED,
)
REVIEWS = FIXTURES / "relationships" / "manual-reviews.yaml"
PROV = Provenance(source_kind=DataSourceKind.SYNTHETIC, source_id="unit-test")


def synthetic_catalog() -> tuple[Catalog, list]:  # type: ignore[type-arg]
    fams = [b(BuildContext(seed=1, dataset_id="unit")) for b in FAMILY_BUILDERS.values()]
    cat = Catalog(
        series=tuple(s for f in fams for s in f.series),
        events=tuple(e for f in fams for e in f.events),
        markets=tuple(m.market for f in fams for m in f.markets),
    )
    return cat, [er for f in fams for er in f.expected_relationships]


def discover_and_review(cat: Catalog) -> list[Relationship]:
    rels = discover(cat, as_of=SIM_EPOCH)
    rels, _ = apply_reviews(rels, load_reviews(REVIEWS), cat, as_of=SIM_EPOCH)
    return rels


def find(rels: list[Relationship], rtype: RelationshipType, members: tuple[str, ...]):
    symmetric = rtype not in (RelationshipType.IMPLICATION, RelationshipType.NESTED_THRESHOLDS)
    for r in rels:
        if r.relationship_type is rtype and (
            set(r.members) == set(members) if symmetric else r.members == members
        ):
            return r
    return None


# ------------------------------------------------------------------ synthetic ground truth
def test_discovery_recovers_every_expected_relationship() -> None:
    cat, expected = synthetic_catalog()
    rels = discover_and_review(cat)
    assert len(expected) == 10
    for er in expected:
        r = find(rels, er.relationship_type, er.members)
        assert r is not None, er
        assert r.verification_status is er.expected_status, (er, r.evidence)
        assert r.exhaustive is er.exhaustive, er


def test_no_unexpected_verified_relationship() -> None:
    """Safety: discovery must never verify anything the ground truth does not contain."""
    cat, expected = synthetic_catalog()
    rels = discover_and_review(cat)
    verified_expected = {
        (er.relationship_type, frozenset(er.members)) for er in expected if er.expected_status is V
    }
    for r in rels:
        if r.is_verified:
            assert (r.relationship_type, frozenset(r.members)) in verified_expected, r


def test_G_rounding_mismatch_false_implication_is_rejected_with_witness() -> None:
    """Test G. "CPI >= 0.3%" (published to 1 dp, half-up) looks like it implies "CPI > 0.27%"
    (index-derived, 2 dp). Exact preimages on the raw value x:

        GE0.3 @ 0.1 half-up : reported >= 0.3  <=>  x >= 0.25           -> [0.25, inf)
        GT0.27 @ 0.01 half-up: reported >= 0.28 <=> x >= 0.275          -> [0.275, inf)

    x = 0.25 reports 0.3 (YES) and 0.25 (NO), so the implication is false."""
    cat, _ = synthetic_catalog()
    rels = discover(cat, as_of=SIM_EPOCH)
    pair = ("SYNCPI-26AUG-GE0.3", "SYNCPI2D-26AUG-GT0.27")
    r = find(rels, RelationshipType.IMPLICATION, pair)
    assert r is not None
    assert r.verification_status is R
    fail = [e for e in r.evidence if e.outcome is EvidenceOutcome.FAIL]
    assert [e.check for e in fail] == ["raw_preimage_subset"]
    assert "x=0.25" in fail[0].detail
    assert "reports 0.3 -> YES" in fail[0].detail
    assert "reports 0.25 -> NO" in fail[0].detail
    # the converse holds on the raw value but conventions differ: review, never auto-verified
    rev = find(rels, RelationshipType.IMPLICATION, (pair[1], pair[0]))
    assert rev is not None
    assert rev.verification_status is C


def test_false_equivalence_identical_titles_different_source_rejected() -> None:
    cat, _ = synthetic_catalog()
    hike, alt = cat.market("SYNFEDHIKE-26SEP-T"), cat.market("SYNFEDALT-26SEP-T")
    assert hike.title == alt.title
    rels = discover(cat, as_of=SIM_EPOCH)
    r = find(rels, RelationshipType.EQUIVALENT, (hike.market_id, alt.market_id))
    assert r is not None
    assert r.verification_status is R
    assert any(
        e.check == "settlement_source" and e.outcome is EvidenceOutcome.FAIL for e in r.evidence
    )


def test_propositions_are_never_auto_verified() -> None:
    cat, _ = synthetic_catalog()
    rels = discover(cat, as_of=SIM_EPOCH)
    r = find(rels, RelationshipType.EQUIVALENT, ("SYNFEDHIKE-26SEP-T", "SYNFEDUB-26SEP-T"))
    assert r is not None
    assert r.verification_status is C
    assert find(rels, RelationshipType.IMPLICATION, ("SYNCHAMP-26-OWLS", "SYNFINALS-26-OWLS")) is (
        None
    )


def test_discovery_is_order_independent_and_deterministic() -> None:
    cat, _ = synthetic_catalog()
    base = [r.model_dump(mode="json") for r in discover(cat, as_of=SIM_EPOCH)]
    rng = random.Random(42)
    for _ in range(3):
        ms, es = list(cat.markets), list(cat.events)
        rng.shuffle(ms)
        rng.shuffle(es)
        shuffled = Catalog(series=cat.series, events=tuple(es), markets=tuple(ms))
        assert [r.model_dump(mode="json") for r in discover(shuffled, as_of=SIM_EPOCH)] == base


def test_chain_is_ordered_narrowest_first_with_chain_scenarios() -> None:
    cat, _ = synthetic_catalog()
    rels = discover(cat, as_of=SIM_EPOCH)
    chain = [r for r in rels if r.relationship_type is RelationshipType.NESTED_THRESHOLDS]
    assert len(chain) == 1
    assert chain[0].members == tuple(f"SYNCPI-26AUG-GE{t}" for t in ("0.4", "0.3", "0.2", "0.1"))
    assert chain[0].scenario_spec.kind is ScenarioKind.CHAIN
    # pairwise implications inside the chain are not duplicated
    assert find(rels, RelationshipType.IMPLICATION, chain[0].members[:2]) is None


# ------------------------------------------------------------------ hand-built catalogs
def num_market(
    mid: str,
    terms: ThresholdTerms | IntervalTerms,
    *,
    rounding: RoundingConvention | None = None,
    source: str | None = "Bureau",
    event_id: str = "EV",
    domain: tuple[str | None, str | None] = (None, None),
    methodology: str = "published",
) -> Market:
    return market(
        mid,
        event_id=event_id,
        source=source or "Bureau",
        settlement=SettlementSpec(
            underlying_id="u1",
            measurement="m",
            location="l",
            observation_window="w",
            settlement_source=source,
            methodology=methodology,
            unit="pct",
            rounding=rounding,
            early_close_policy="none",
            exceptional_resolution=(),
            value_domain_lower=None if domain[0] is None else dec(domain[0]),
            value_domain_upper=None if domain[1] is None else dec(domain[1]),
            terms=terms,
        ),
    )


def th(cmp: Comparator, v: str) -> ThresholdTerms:
    return ThresholdTerms(comparator=cmp, value=dec(v))


HALF_UP_01 = RoundingConvention(mode=RoundingMode.HALF_UP, increment=dec("0.1"))


def cat_of(*markets: Market, events: tuple[Event, ...] = ()) -> Catalog:
    return Catalog(markets=markets, events=events)


def test_rounding_makes_different_thresholds_equivalent() -> None:
    """GE 0.3 and GT 0.2, both on a 1-dp half-up report: GT0.2 <=> report >= 0.3 <=> x >= 0.25.
    Same raw preimage [0.25, inf) -> EQUIVALENT, verified."""
    a = num_market("A", th(Comparator.GE, "0.3"), rounding=HALF_UP_01)
    b = num_market("B", th(Comparator.GT, "0.2"), rounding=HALF_UP_01)
    (r,) = discover(cat_of(a, b), as_of=T0)
    assert r.relationship_type is RelationshipType.EQUIVALENT
    assert r.verification_status is V


def test_strict_vs_inclusive_boundary_without_rounding() -> None:
    """No rounding: (0.3, inf) is a strict subset of [0.3, inf): GT => GE only."""
    gt = num_market("GT", th(Comparator.GT, "0.3"))
    ge = num_market("GE", th(Comparator.GE, "0.3"))
    rels = discover(cat_of(gt, ge), as_of=T0)
    assert [(r.relationship_type, r.members, r.verification_status) for r in rels] == [
        (RelationshipType.IMPLICATION, ("GT", "GE"), V)
    ]


def test_unknown_field_yields_candidate_review_never_verified() -> None:
    a = num_market("A", th(Comparator.GE, "0.4"), source=None)
    b = num_market("B", th(Comparator.GE, "0.3"))
    (r,) = discover(cat_of(a, b), as_of=T0)
    assert r.verification_status is C
    assert any(
        e.check == "settlement_source" and e.outcome is EvidenceOutcome.UNKNOWN for e in r.evidence
    )


def test_intervals_with_open_open_boundary_gap_are_not_exhaustive() -> None:
    lo = num_market("LO", IntervalTerms(lower=None, upper=dec("80"), upper_inclusive=False))
    hi = num_market("HI", IntervalTerms(lower=dec("80"), lower_inclusive=False, upper=None))
    (r,) = discover(cat_of(lo, hi), as_of=T0)
    assert r.relationship_type is RelationshipType.DISJOINT_INTERVALS
    assert r.verification_status is V
    assert r.exhaustive is False
    assert r.scenario_spec.min_yes == 0
    assert "raw value 80 is in no interval" in r.reasoning


def test_whole_degree_rounding_closes_the_gap() -> None:
    """Same bins on a whole-degree half-up report: LO <=> x < 79.5, HI (> 80) <=> x >= 80.5.
    Raw gap [79.5, 80.5) (report exactly 80) -> still not exhaustive. Closing HI at 80 fixes it."""
    r1 = RoundingConvention(mode=RoundingMode.HALF_UP, increment=dec("1"))
    lo = num_market("LO", IntervalTerms(upper=dec("80"), upper_inclusive=False), rounding=r1)
    hi_open = num_market("HI", IntervalTerms(lower=dec("80"), lower_inclusive=False), rounding=r1)
    (r,) = discover(cat_of(lo, hi_open), as_of=T0)
    assert r.exhaustive is False
    hi_closed = num_market("HI", IntervalTerms(lower=dec("80"), lower_inclusive=True), rounding=r1)
    (r,) = discover(cat_of(lo, hi_closed), as_of=T0)
    assert r.exhaustive is True
    assert r.scenario_spec.min_yes == 1


def test_bounded_value_domain_coverage() -> None:
    dom = ("0", "100")
    a = num_market("A", IntervalTerms(lower=dec("0"), upper=dec("50")), domain=dom)
    b = num_market(
        "B", IntervalTerms(lower=dec("50"), upper=dec("100"), upper_inclusive=True), domain=dom
    )
    (r,) = discover(cat_of(a, b), as_of=T0)
    assert r.exhaustive is True
    # without a declared domain the real line is not covered
    a2 = num_market("A", IntervalTerms(lower=dec("0"), upper=dec("50")))
    b2 = num_market("B", IntervalTerms(lower=dec("50"), upper=dec("100"), upper_inclusive=True))
    (r2,) = discover(cat_of(a2, b2), as_of=T0)
    assert r2.exhaustive is False


def outcome_market(mid: str, outcome: str, event_id: str = "EV") -> Market:
    return market(
        mid,
        event_id=event_id,
        source="Commission",
        settlement=SettlementSpec(
            underlying_id="winner",
            measurement="winner",
            location="x",
            observation_window="w",
            settlement_source="Commission",
            early_close_policy="none",
            exceptional_resolution=(),
            terms=OutcomeTerms(outcome_id=outcome),
        ),
    )


def event(*, me: bool | None, complete: bool | None, universe: tuple[str, ...] | None) -> Event:
    return Event(
        event_id="EV",
        series_id="S",
        title="t",
        mutually_exclusive=me,
        outcome_set_complete=complete,
        outcome_universe=universe,
        provenance=PROV,
    )


def test_partition_requires_listed_outcomes_to_equal_universe() -> None:
    ms = (outcome_market("X", "A"), outcome_market("Y", "B"))
    (r,) = discover(
        cat_of(*ms, events=(event(me=True, complete=True, universe=("A", "B")),)), as_of=T0
    )
    assert (r.relationship_type, r.exhaustive, r.verification_status) == (
        RelationshipType.EXHAUSTIVE_PARTITION,
        True,
        V,
    )
    (r,) = discover(
        cat_of(*ms, events=(event(me=True, complete=True, universe=("A", "B", "C")),)), as_of=T0
    )
    assert (r.relationship_type, r.exhaustive) == (RelationshipType.MUTUALLY_EXCLUSIVE, False)
    (r,) = discover(cat_of(*ms, events=(event(me=None, complete=None, universe=None),)), as_of=T0)
    assert r.verification_status is C
    assert (
        discover(cat_of(*ms, events=(event(me=False, complete=None, universe=None),)), as_of=T0)
        == []
    )


def test_title_similarity_is_candidate_review_at_most() -> None:
    a = market("A", settlement=SettlementSpec(terms=PropositionTerms(proposition_id="p")))
    b = market("B", settlement=SettlementSpec(terms=PropositionTerms(proposition_id="q")))
    b = b.model_copy(update={"title": a.title.upper() + "!"})
    rels = discover(cat_of(a, b), as_of=T0)
    assert len(rels) == 1
    assert rels[0].verification_status is C
    assert any(e.check == "title_similarity" for e in rels[0].evidence)


# ------------------------------------------------------------------ manual review safety
def test_stale_review_reopens_relationship() -> None:
    cat, _ = synthetic_catalog()
    reviews = load_reviews(REVIEWS)
    hike = cat.market("SYNFEDHIKE-26SEP-T")
    changed = hike.model_copy(update={"settlement_rules": hike.settlement_rules + " (amended)"})
    cat2 = Catalog(
        series=cat.series,
        events=cat.events,
        markets=tuple(changed if m.market_id == hike.market_id else m for m in cat.markets),
    )
    rels, notes = apply_reviews(discover(cat2, as_of=SIM_EPOCH), reviews, cat2, as_of=SIM_EPOCH)
    r = find(rels, RelationshipType.EQUIVALENT, ("SYNFEDHIKE-26SEP-T", "SYNFEDUB-26SEP-T"))
    assert r is not None
    assert r.verification_status is C
    assert r.invalidation_reason is not None
    assert "fed-hike-equals-upper-bound" in r.invalidation_reason
    assert any("stale" in n for n in notes)


def test_reviewer_cannot_override_failing_evidence() -> None:
    cat, _ = synthetic_catalog()
    hike, alt = cat.market("SYNFEDHIKE-26SEP-T"), cat.market("SYNFEDALT-26SEP-T")
    bad = ReviewFile(
        schema_version=1,
        reviews=(
            ManualReview(
                review_id="bad-approval",
                relationship_type=RelationshipType.EQUIVALENT,
                members=(hike.market_id, alt.market_id),
                decision=V,
                reviewer="someone",
                reviewed_at=T0,
                reasoning="looks the same",
                rules_hashes={hike.market_id: hike.rules_hash, alt.market_id: alt.rules_hash},
            ),
        ),
    )
    rels, notes = apply_reviews(discover(cat, as_of=SIM_EPOCH), bad, cat, as_of=SIM_EPOCH)
    r = find(rels, RelationshipType.EQUIVALENT, (hike.market_id, alt.market_id))
    assert r is not None
    assert r.verification_status is R
    assert any("cannot be overridden" in n for n in notes)


def test_review_with_unknown_members_is_skipped() -> None:
    cat, _ = synthetic_catalog()
    rf = ReviewFile(
        schema_version=1,
        reviews=(
            ManualReview(
                review_id="ghost",
                relationship_type=RelationshipType.IMPLICATION,
                members=("NOPE-1", "NOPE-2"),
                decision=V,
                reviewer="r",
                reviewed_at=T0,
                reasoning="x",
                rules_hashes={},
            ),
        ),
    )
    before = discover(cat, as_of=SIM_EPOCH)
    after, notes = apply_reviews(before, rf, cat, as_of=SIM_EPOCH)
    assert after == before
    assert notes == ["ghost: skipped, unknown members ['NOPE-1', 'NOPE-2']"]


def test_review_scenarios_and_record_attached() -> None:
    cat, _ = synthetic_catalog()
    rels = discover_and_review(cat)
    r = find(rels, RelationshipType.IMPLICATION, ("SYNCHAMP-26-OWLS", "SYNFINALS-26-OWLS"))
    assert r is not None
    assert r.scenario_spec.explicit_states == ((0, 0), (0, 1), (1, 1))
    assert r.reviewer == "synthetic-fixture-reviewer"
    assert r.reviews[0].source.endswith("#owls-champion-implies-finalist")
    assert all(e.outcome is EvidenceOutcome.PASS for e in r.evidence)


def test_revalidate_demotes_on_rules_change_and_removal() -> None:
    cat, _ = synthetic_catalog()
    rels = discover_and_review(cat)
    chain = next(r for r in rels if r.relationship_type is RelationshipType.NESTED_THRESHOLDS)
    m0 = cat.market(chain.members[0])
    changed = m0.model_copy(update={"settlement_rules": "rewritten"})
    cat2 = Catalog(
        events=cat.events,
        markets=tuple(changed if m.market_id == m0.market_id else m for m in cat.markets),
    )
    out = {r.relationship_id: r for r in revalidate(rels, cat2, as_of=SIM_EPOCH)}
    assert out[chain.relationship_id].verification_status is C
    assert "rules changed" in (out[chain.relationship_id].invalidation_reason or "")
    cat3 = Catalog(markets=tuple(m for m in cat.markets if m.market_id != m0.market_id))
    out3 = {r.relationship_id: r for r in revalidate(rels, cat3, as_of=SIM_EPOCH)}
    assert out3[chain.relationship_id].verification_status is C
    # rejected stays rejected
    rejected = [r for r in rels if r.verification_status is R]
    assert all(out3[r.relationship_id].verification_status is R for r in rejected)


def test_manual_review_file_hashes_match_current_synthetic_rules() -> None:
    cat, _ = synthetic_catalog()
    for review in load_reviews(REVIEWS).reviews:
        for mid, h in review.rules_hashes.items():
            assert cat.market(mid).rules_hash == h, (review.review_id, mid)


def test_review_file_path_exists() -> None:
    assert Path(REVIEWS).is_file()


# ------------------------------------------------------------------ scenario spaces
def test_scenario_space_counts_and_states() -> None:
    chain = ScenarioSpace(ScenarioSpec(kind=ScenarioKind.CHAIN), 3)
    assert list(chain.iter_states()) == [(0, 0, 0), (0, 0, 1), (0, 1, 1), (1, 1, 1)]
    assert chain.contains((0, 1, 1))
    assert not chain.contains((1, 0, 1))
    part = ScenarioSpace(ScenarioSpec(kind=ScenarioKind.CARDINALITY, min_yes=1, max_yes=1), 4)
    assert part.count() == 4
    assert not part.contains((0, 0, 0, 0))
    excl = ScenarioSpace(ScenarioSpec(kind=ScenarioKind.CARDINALITY, min_yes=0, max_yes=1), 4)
    assert excl.count() == 5
    assert (0, 0, 0, 0) in set(excl.iter_states())


def test_large_cardinality_space_uses_constraint_evaluation() -> None:
    """40 members, 0..20 YES: ~6.6e11 states; never enumerated, yet the minimum is exact."""
    sp = ScenarioSpace(ScenarioSpec(kind=ScenarioKind.CARDINALITY, min_yes=0, max_yes=20), 40)
    assert not sp.enumerable
    with pytest.raises(ScenarioSpaceError):
        next(sp.iter_states())
    w = [Decimal(i - 25) for i in range(40)]  # -25..14; 20 smallest are -25..-6
    v, s = sp.min_linear(Decimal(0), w)
    assert v == sum(Decimal(i) for i in range(-25, -5))
    assert sum(s) == 20


def test_empty_cardinality_space_rejected() -> None:
    with pytest.raises(ScenarioSpaceError):
        ScenarioSpace(ScenarioSpec(kind=ScenarioKind.CARDINALITY, min_yes=3, max_yes=3), 2)


def test_witness_fraction_formatting_is_exact() -> None:
    """Witnesses with non-terminating decimal expansions are printed as fractions."""
    from consistency_core.relationships.discovery import _fmt

    assert _fmt(Fraction(1, 4)) == "0.25"
    assert _fmt(Fraction(1, 3)) == "1/3"
    assert _fmt(Fraction(80)) == "80"

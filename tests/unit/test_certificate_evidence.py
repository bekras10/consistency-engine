"""Proof certificates carry the evidence behind every verdict, deterministically, and the worst-case
payoff and admissible set can be re-derived from the certificate JSON alone (hardening pass, P2)."""

from __future__ import annotations

import itertools
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from consistency_core.models.relationship import ReviewRecord, VerificationStatus
from consistency_core.serialization import sha256_of
from tests.golden.support import NOW_MS, load, run
from tests.unit.test_evaluator import run_a

D = Decimal
GATES = (
    "relationship_valid",
    "books_synchronized",
    "books_fresh",
    "markets_open_with_asks",
    "top_of_book_pre_fee_edge",
    "depth_supports_required_size",
    "pre_fee_edge_survives_depth",
    "fees_verified",
    "positive_after_fees",
    "execution_gates",
)


def _cert(ev: Any) -> dict[str, Any]:
    return json.loads(ev.certificate_json())  # type: ignore[no-any-return]


def test_certificate_version_is_bumped() -> None:
    assert run_a().certificate.certificate_version == "proof-certificate/2"


def test_verification_status_and_every_check_with_evidence() -> None:
    ev = run_a()
    v = _cert(ev)["verification"]
    assert v["status"] == "verified"
    assert v["checks"], "relationship evidence must be carried"
    for c in v["checks"]:
        assert set(c) >= {"check", "outcome", "detail"}
    assert v["discovered_by"]
    assert v["scenario_integrity"] == []


def test_reviewer_provenance_is_recorded() -> None:
    review = ReviewRecord(
        review_id="REV-42",
        reviewer="analyst@example.invalid",
        decision=VerificationStatus.VERIFIED,
        reasoning="Rules read side by side; GE3 implies GE2.",
        reviewed_at=datetime(2026, 7, 1, 12, tzinfo=UTC),
        source="fixtures/relationships/manual-reviews.yaml#REV-42",
    )
    ev = run_a(rel_update={"reviews": (review,), "reviewer": review.reviewer})
    v = _cert(ev)["verification"]
    assert v["reviewer"] == "analyst@example.invalid"
    [r] = v["reviews"]
    assert r == {
        "review_id": "REV-42",
        "reviewer": "analyst@example.invalid",
        "decision": "verified",
        "reviewed_at": "2026-07-01T12:00:00Z",
        "justification": "Rules read side by side; GE3 implies GE2.",
        "source": "fixtures/relationships/manual-reviews.yaml#REV-42",
    }


def test_rules_hashes_recorded_and_compared() -> None:
    ok = _cert(run_a())["verification"]["rules"]
    assert [r["matches"] for r in ok] == [True, True]
    assert all(r["recorded_hash"] == r["current_hash"] for r in ok)
    bad = run_a(markets_update={"GOLD-A-GE2": {"settlement_rules": "Rules were edited"}})
    rules = {r["market_id"]: r for r in _cert(bad)["verification"]["rules"]}
    assert rules["GOLD-A-GE2"]["matches"] is False
    assert rules["GOLD-A-GE2"]["recorded_hash"] != rules["GOLD-A-GE2"]["current_hash"]


def test_scenario_set_provenance_and_pricing_specs() -> None:
    c = _cert(run_a())
    assert c["relationship"]["scenario_provenance"]["source"] == "derived"
    assert c["payoff"]["scenario_specs"] == [c["relationship"]["scenario_spec"]]


def test_fee_schedule_identity_version_and_status() -> None:
    c = _cert(run_a())
    [s] = c["fee_schedules"]
    assert s["schedule_id"]
    assert s["version"].startswith("sha256:")
    assert s["verification_status"] == "FICTIONAL"
    assert s["effective_from"]
    assert sorted(s["markets"]) == ["GOLD-A-GE2", "GOLD-A-GE3"]
    assert s["resolutions_verified"] is True


def test_config_hash() -> None:
    ev = run_a()
    assert ev.certificate.config_hash == sha256_of(ev.certificate.config)
    other = run_a(config={"minimum_net_edge": "0.02"})
    assert other.certificate.config_hash != ev.certificate.config_hash


def test_search_method_and_exactness() -> None:
    ex = run_a().certificate.capacity
    assert ex is not None
    assert ex.method == "EXHAUSTIVE"
    assert ex.optimal_quantity_is_exact is True
    bp = run_a(config={"exhaustive_search_limit": 5}).certificate.capacity
    assert bp is not None
    assert bp.method == "BREAKPOINT_APPROXIMATE"
    assert bp.optimal_quantity_is_exact is False
    assert bp.reported_quantity_evaluation_is_exact is True


def test_validation_metadata_lists_evaluated_gates() -> None:
    v = _cert(run_a())["validation"]
    assert v["gates_evaluated"] == list(GATES)
    assert v["gates_not_reached"] == []
    assert v["first_failing_gate"] is None
    stale = _cert(run_a(config={"max_book_age_ms": 0, "max_cross_market_skew_ms": 0}))
    sv = stale["validation"]
    if stale["classification"] == "STALE_DATA":
        assert sv["gates_evaluated"] == list(GATES[:3])
        assert sv["gates_not_reached"] == list(GATES[3:])
        assert sv["first_failing_gate"] == "books_fresh"


def test_negative_age_never_reported_in_book_inputs() -> None:
    ev = run_a(book_update={"GOLD-A-GE3": {"confirmed_through_ms": NOW_MS + 50}})
    ages = {b.market_id: b.age_ms for b in ev.certificate.books}
    assert ages["GOLD-A-GE3"] is None
    assert ages["GOLD-A-GE2"] is not None and ages["GOLD-A-GE2"] >= 0


def test_evidence_certificate_is_deterministic() -> None:
    a, b = run_a(), run_a()
    assert a.certificate_json() == b.certificate_json()
    assert a.certificate_hash == b.certificate_hash


# ---------------------------------------------------------------- independent re-derivation
def _states(spec: dict[str, Any], n: int) -> set[tuple[int, ...]]:
    """Admissible states from the serialized ScenarioSpec semantics alone."""
    kind = spec["kind"]
    if kind == "explicit":
        return {tuple(s) for s in spec["explicit_states"]}
    if kind == "chain":  # members narrowest first: YES on member i implies YES on all j > i
        return {tuple([0] * k + [1] * (n - k)) for k in range(n + 1)}
    assert kind == "cardinality", kind
    lo, hi = spec["min_yes"], spec["max_yes"]
    return {s for s in itertools.product((0, 1), repeat=n) if lo <= sum(s) <= hi}


def _payoff(legs: list[dict[str, Any]], members: list[str], s: tuple[int, ...]) -> Decimal:
    state = dict(zip(members, s, strict=True))
    total = D(0)
    for leg in legs:
        yes = state[leg["market_id"]] == 1
        if (leg["side"] == "yes") == yes:
            total += D(leg["ratio"])
    return total


def _golden_cases() -> list[tuple[str, str]]:
    out = []
    for name in "ABCDEFGHIJ":
        for case, body in (load(name).get("cases") or {}).items():
            if isinstance(body, dict) and "relationship" in body and "portfolio" in body:
                out.append((name, case))
    return out


@pytest.mark.parametrize(("fixture", "case"), _golden_cases())
def test_min_payoff_and_admissible_set_rederivable_from_certificate(
    fixture: str, case: str
) -> None:
    fx = load(fixture)
    ev, _ = run(fx, fx["cases"][case])
    c = _cert(ev)
    payoff = c["payoff"]
    if payoff is None:
        pytest.skip("evaluation stopped before the payoff proof")
    members = c["relationship"]["members"]
    legs = c["portfolio"]["legs"]
    states: set[tuple[int, ...]] = set()
    for spec in payoff["scenario_specs"]:
        states |= _states(spec, len(members))
    assert len(states) == payoff["admissible_state_count"]
    if payoff["states"] is not None:
        listed = {tuple(s["state"][m] for m in members) for s in payoff["states"]}
        assert listed == states
    worst = min(_payoff(legs, members, s) for s in states)
    assert D(payoff["min_payoff_per_unit"]) == worst
    if c["evaluation"] is not None:
        q = D(c["evaluation"]["quantity"])
        assert D(c["evaluation"]["min_payoff"]) == worst * q

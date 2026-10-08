"""Evidence checks and relationship construction.

Verification policy (spec 8.2):

* ``FAIL`` evidence (a proven difference or a counterexample) -> REJECTED.
* otherwise any ``UNKNOWN`` evidence (missing field, differing convention, proposition terms,
  title-only match) -> CANDIDATE_REVIEW.
* only all-``PASS`` evidence -> VERIFIED.

Unknown never counts as a match, and a manual reviewer may resolve UNKNOWN evidence but can
never override FAIL evidence (see :mod:`consistency_core.relationships.review`).
"""

from __future__ import annotations

import itertools
from collections.abc import Sequence
from datetime import datetime

from consistency_core.models.market import Market
from consistency_core.models.relationship import (
    EvidenceCheck,
    EvidenceOutcome,
    Relationship,
    RelationshipType,
    ScenarioKind,
    ScenarioSpec,
    VerificationStatus,
)
from consistency_core.serialization import sha256_of

IDENTITY_FIELDS: tuple[str, ...] = (
    "underlying_id",
    "measurement",
    "location",
    "observation_window",
    "settlement_source",
    "early_close_policy",
)
"""Fields that must be identical for two contracts to refer to the same observation."""

NUMERIC_IDENTITY_FIELDS: tuple[str, ...] = (*IDENTITY_FIELDS, "unit")
"""Numeric contracts additionally need the same unit (not applicable to categorical ones)."""

CONTEXT_FIELDS: tuple[str, ...] = (
    "settlement_source",
    "observation_window",
    "early_close_policy",
)
"""Reduced set used for reviewer-asserted relationships between different propositions."""

PASS, FAIL, UNKNOWN = EvidenceOutcome.PASS, EvidenceOutcome.FAIL, EvidenceOutcome.UNKNOWN

SYMMETRIC_TYPES = frozenset(
    {
        RelationshipType.EQUIVALENT,
        RelationshipType.MUTUALLY_EXCLUSIVE,
        RelationshipType.EXHAUSTIVE_PARTITION,
        RelationshipType.DISJOINT_INTERVALS,
    }
)


def check(name: str, outcome: EvidenceOutcome, detail: str) -> EvidenceCheck:
    return EvidenceCheck(check=name, outcome=outcome, detail=detail)


def field_checks(markets: Sequence[Market], fields: Sequence[str]) -> list[EvidenceCheck]:
    out = []
    for f in fields:
        values = [getattr(m.settlement, f) for m in markets]
        missing = [m.market_id for m, v in zip(markets, values, strict=True) if v is None]
        if missing:
            out.append(check(f, UNKNOWN, f"{f} unknown for {', '.join(missing)}"))
        elif len(set(values)) == 1:
            out.append(check(f, PASS, f"{f} identical: {values[0]!r}"))
        else:
            pairs = "; ".join(f"{m.market_id}={v!r}" for m, v in zip(markets, values, strict=True))
            out.append(check(f, FAIL, f"{f} differs: {pairs}"))
    return out


def exceptional_check(markets: Sequence[Market]) -> EvidenceCheck:
    unknown = [m.market_id for m in markets if m.settlement.exceptional_resolution is None]
    if unknown:
        return check(
            "exceptional_resolution",
            UNKNOWN,
            "exceptional-resolution handling (void/withdrawal/postponement) unspecified for "
            + ", ".join(unknown),
        )
    special = sorted({x for m in markets for x in (m.settlement.exceptional_resolution or ())})
    if special:
        return check(
            "exceptional_resolution",
            UNKNOWN,
            f"exceptional outcomes {special} are not part of the binary scenario model",
        )
    return check("exceptional_resolution", PASS, "no exceptional resolution paths")


def convention_checks(markets: Sequence[Market]) -> list[EvidenceCheck]:
    """Rounding/methodology differences are not automatically contradictions, but they mean the
    contracts may not settle on the same reported figure, so they need review."""
    out = []
    for f in ("rounding", "methodology"):
        values = [getattr(m.settlement, f) for m in markets]
        if any(v is None for v in values) and f == "methodology":
            out.append(check(f, UNKNOWN, "methodology unknown"))
        elif len({repr(v) for v in values}) == 1:
            out.append(check(f, PASS, f"{f} identical"))
        else:
            pairs = "; ".join(f"{m.market_id}={v!r}" for m, v in zip(markets, values, strict=True))
            out.append(check(f, UNKNOWN, f"{f} differs (needs review): {pairs}"))
    return out


def status_from(evidence: Sequence[EvidenceCheck]) -> VerificationStatus:
    if any(e.outcome is FAIL for e in evidence):
        return VerificationStatus.REJECTED
    if any(e.outcome is UNKNOWN for e in evidence):
        return VerificationStatus.CANDIDATE_REVIEW
    return VerificationStatus.VERIFIED


def default_scenarios(rtype: RelationshipType, n: int, *, exhaustive: bool) -> ScenarioSpec:
    match rtype:
        case RelationshipType.IMPLICATION | RelationshipType.NESTED_THRESHOLDS:
            return ScenarioSpec(kind=ScenarioKind.CHAIN)
        case RelationshipType.EQUIVALENT:
            return ScenarioSpec(kind=ScenarioKind.EXPLICIT, explicit_states=((0, 0), (1, 1)))
        case RelationshipType.EXHAUSTIVE_PARTITION:
            return ScenarioSpec(kind=ScenarioKind.CARDINALITY, min_yes=1, max_yes=1)
        case RelationshipType.MUTUALLY_EXCLUSIVE:
            return ScenarioSpec(kind=ScenarioKind.CARDINALITY, min_yes=0, max_yes=1)
        case RelationshipType.DISJOINT_INTERVALS:
            lo = 1 if exhaustive else 0
            return ScenarioSpec(kind=ScenarioKind.CARDINALITY, min_yes=lo, max_yes=1)


def default_constraints(
    rtype: RelationshipType, members: Sequence[str], *, exhaustive: bool
) -> tuple[str, ...]:
    match rtype:
        case RelationshipType.IMPLICATION:
            a, b = members
            return (f"YES({a}) => YES({b})", f"P({a}) <= P({b})")
        case RelationshipType.NESTED_THRESHOLDS:
            return tuple(f"P({a}) <= P({b})" for a, b in itertools.pairwise(members))
        case RelationshipType.EQUIVALENT:
            a, b = members
            return (f"YES({a}) <=> YES({b})", f"P({a}) = P({b})")
        case RelationshipType.EXHAUSTIVE_PARTITION:
            return ("exactly one member settles YES", "sum P = 1")
        case RelationshipType.MUTUALLY_EXCLUSIVE:
            return ("at most one member settles YES", "sum P <= 1")
        case RelationshipType.DISJOINT_INTERVALS:
            if exhaustive:
                return ("pairwise disjoint and covering the value domain", "sum P = 1")
            return ("pairwise disjoint", "sum P <= 1")


def relationship_id(rtype: RelationshipType, members: Sequence[str]) -> str:
    """Stable across rules changes (so reviews can reference it); content changes show up in
    :func:`fingerprint`."""
    key = sorted(members) if rtype in SYMMETRIC_TYPES else list(members)
    return "rel-" + sha256_of({"type": rtype.value, "members": key}).removeprefix("sha256:")[:16]


def fingerprint(
    rtype: RelationshipType,
    members: Sequence[str],
    rules_hashes: dict[str, str],
    scenario: ScenarioSpec,
    exhaustive: bool,
) -> str:
    return sha256_of(
        {
            "type": rtype.value,
            "members": list(members),
            "rules_hashes": rules_hashes,
            "scenario_spec": scenario,
            "exhaustive": exhaustive,
        }
    )


def build_relationship(
    rtype: RelationshipType,
    markets: Sequence[Market],
    *,
    evidence: Sequence[EvidenceCheck],
    reasoning: str,
    as_of: datetime,
    discovered_by: str,
    exhaustive: bool = False,
    scenario: ScenarioSpec | None = None,
    status: VerificationStatus | None = None,
) -> Relationship:
    members = tuple(m.market_id for m in markets)
    spec = scenario or default_scenarios(rtype, len(members), exhaustive=exhaustive)
    hashes = {m.market_id: m.rules_hash for m in markets}
    return Relationship(
        relationship_id=relationship_id(rtype, members),
        relationship_type=rtype,
        members=members,
        constraints=default_constraints(rtype, members, exhaustive=exhaustive),
        scenario_spec=spec,
        exhaustive=exhaustive,
        verification_status=status or status_from(evidence),
        evidence=tuple(evidence),
        reasoning=reasoning,
        rules_hashes=hashes,
        discovered_by=discovered_by,
        fingerprint=fingerprint(rtype, members, hashes, spec, exhaustive),
        created_at=as_of,
        updated_at=as_of,
    )

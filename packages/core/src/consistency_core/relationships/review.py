"""Manual-review workflow (spec 8.4) backed by a version-controlled YAML file.

Safety rules:

* A review pins the members' ``rules_hash`` values. If any current hash differs, the review is
  stale: the relationship is (re)opened as CANDIDATE_REVIEW with an invalidation reason.
* A reviewer may resolve UNKNOWN evidence (e.g. free-text propositions, exceptional-resolution
  wording) but can never override FAIL evidence (a proven difference or counterexample).
* :func:`revalidate` must run whenever reference data refreshes: any rules change on a member
  demotes the relationship so it can no longer produce verified classifications.
* A supplied ``scenario_spec`` (or an ``exhaustive`` claim) is compared with the set derived
  from the relationship type (see ``scenario_integrity``). Equal sets stay ``derived``. Any
  difference is an override: it needs a justification, evidence references and declared
  removed/added states matching the computed diff. A superset is accepted (conservative); a
  removal keeps the relationship at CANDIDATE_REVIEW unless separately verified relationships
  with derived sets over the same members exclude every removed state.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Literal

import yaml
from pydantic import AwareDatetime

from consistency_core.models.common import FrozenModel
from consistency_core.models.market import Catalog
from consistency_core.models.relationship import (
    EvidenceCheck,
    EvidenceOutcome,
    Relationship,
    RelationshipType,
    ReviewRecord,
    ScenarioProvenance,
    ScenarioSpec,
    VerificationStatus,
)
from consistency_core.relationships.scenario_integrity import (
    DIFF_MISMATCH,
    JUSTIFIED,
    METADATA_MISSING,
    REMOVES_ADMISSIBLE_STATES,
    SUPERSET,
    UNCHECKABLE,
    derived_spec,
    diff_specs,
    excludes,
    proven_exhaustive,
    remap_spec,
    remap_state,
)
from consistency_core.relationships.verification import (
    CONTEXT_FIELDS,
    SYMMETRIC_TYPES,
    build_relationship,
    check,
    exceptional_check,
    field_checks,
    fingerprint,
)

REVIEW_DISCOVERED_BY = "manual-review"


class ManualReview(FrozenModel):
    review_id: str
    relationship_type: RelationshipType
    members: tuple[str, ...]
    decision: VerificationStatus
    reviewer: str
    reviewed_at: AwareDatetime
    reasoning: str
    rules_hashes: dict[str, str]
    scenario_spec: ScenarioSpec | None = None
    """States aligned with ``members`` as listed in this review."""
    exhaustive: bool = False
    scenario_justification: str | None = None
    """Required whenever the supplied set differs from the derived one."""
    scenario_evidence: tuple[str, ...] = ()
    """References (documents, tickets, rule clauses) backing a scenario override."""
    declared_removed_states: tuple[tuple[int, ...], ...] | None = None
    """Derived-admissible states the override removes; must equal the computed diff."""
    declared_added_states: tuple[tuple[int, ...], ...] | None = None


class ReviewFile(FrozenModel):
    schema_version: Literal[1]
    reviews: tuple[ManualReview, ...]


def load_reviews(path: Path) -> ReviewFile:
    with path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return ReviewFile.model_validate(data)


def _same(rel: Relationship, review: ManualReview) -> bool:
    if rel.relationship_type is not review.relationship_type:
        return False
    if rel.relationship_type in SYMMETRIC_TYPES:
        return set(rel.members) == set(review.members)
    return rel.members == review.members


def apply_reviews(
    relationships: Sequence[Relationship],
    reviews: ReviewFile,
    catalog: Catalog,
    *,
    as_of: datetime,
    source: str = "fixtures/relationships/manual-reviews.yaml",
) -> tuple[list[Relationship], list[str]]:
    """Returns the updated relationships (sorted by id) and human-readable notes."""
    by_id = {r.relationship_id: r for r in relationships}
    markets = catalog.markets_by_id()
    notes: list[str] = []
    pending: dict[str, ManualReview] = {}
    for review in reviews.reviews:
        missing = [m for m in review.members if m not in markets]
        if missing:
            notes.append(f"{review.review_id}: skipped, unknown members {missing}")
            continue
        members = [markets[m] for m in review.members]
        existing = next((r for r in by_id.values() if _same(r, review)), None)
        record = ReviewRecord(
            review_id=review.review_id,
            reviewer=review.reviewer,
            decision=review.decision,
            reasoning=review.reasoning,
            reviewed_at=review.reviewed_at,
            source=f"{source}#{review.review_id}",
        )
        if existing is None:
            initial = field_checks(members, CONTEXT_FIELDS)
            initial.append(exceptional_check(members))
            initial.append(
                check("reviewer_asserted_semantics", EvidenceOutcome.UNKNOWN, review.reasoning)
            )
            existing = build_relationship(
                review.relationship_type,
                members,
                evidence=initial,
                reasoning=review.reasoning,
                as_of=as_of,
                discovered_by=REVIEW_DISCOVERED_BY,
            )
        current = {m.market_id: m.rules_hash for m in members}
        stale = sorted(m for m in current if review.rules_hashes.get(m) != current[m])
        if stale:
            notes.append(f"{review.review_id}: stale (rules changed for {stale})")
            updated = existing.model_copy(
                update={
                    "verification_status": (
                        VerificationStatus.REJECTED
                        if existing.verification_status is VerificationStatus.REJECTED
                        else VerificationStatus.CANDIDATE_REVIEW
                    ),
                    "invalidation_reason": (
                        f"rules changed since review {review.review_id}: {', '.join(stale)}"
                    ),
                    "updated_at": as_of,
                }
            )
            by_id[updated.relationship_id] = updated
            continue
        scenario, exhaustive, provenance, scenario_checks, needs_justifier = _scenario_override(
            existing, review
        )
        failing = [e for e in existing.evidence if e.outcome is EvidenceOutcome.FAIL]
        evidence: tuple[EvidenceCheck, ...] = existing.evidence
        status = review.decision
        if review.decision is VerificationStatus.VERIFIED:
            if failing:
                status = VerificationStatus.REJECTED
                notes.append(
                    f"{review.review_id}: approval refused, failing evidence "
                    f"{[e.check for e in failing]} cannot be overridden"
                )
            else:
                evidence = (
                    *(
                        e
                        if e.outcome is EvidenceOutcome.PASS
                        else EvidenceCheck(
                            check=e.check,
                            outcome=EvidenceOutcome.PASS,
                            detail=f"resolved by manual review {review.review_id}: {e.detail}",
                        )
                        for e in existing.evidence
                    ),
                    check(
                        "manual_review",
                        EvidenceOutcome.PASS,
                        f"{review.reviewer} at {review.reviewed_at.isoformat()}",
                    ),
                )
        evidence = (*evidence, *scenario_checks)
        if status is VerificationStatus.VERIFIED and any(
            e.outcome is not EvidenceOutcome.PASS for e in evidence
        ):
            status = VerificationStatus.CANDIDATE_REVIEW
            open_checks = [
                e.check for e in scenario_checks if e.outcome is not EvidenceOutcome.PASS
            ]
            notes.append(
                f"{review.review_id}: approval held at candidate_review ({', '.join(open_checks)})"
            )
        if needs_justifier:
            pending[existing.relationship_id] = review
        updated = existing.model_copy(
            update={
                "verification_status": status,
                "evidence": evidence,
                "reviewer": review.reviewer,
                "reviews": (*existing.reviews, record),
                "scenario_spec": scenario,
                "scenario_provenance": provenance,
                "exhaustive": exhaustive,
                "fingerprint": fingerprint(
                    existing.relationship_type,
                    existing.members,
                    existing.rules_hashes,
                    scenario,
                    exhaustive,
                ),
                "updated_at": as_of,
            }
        )
        by_id[updated.relationship_id] = Relationship.model_validate(updated.model_dump())
        notes.append(f"{review.review_id}: applied ({status.value})")
    _justify_overrides(by_id, pending, notes)
    return [by_id[k] for k in sorted(by_id)], notes


def _scenario_override(
    existing: Relationship, review: ManualReview
) -> tuple[ScenarioSpec, bool, ScenarioProvenance, list[EvidenceCheck], bool]:
    """Validate a review's scenario set (and exhaustiveness claim) against the derived set.

    Returns ``(spec, exhaustive, provenance, checks, needs_justifier)``. ``needs_justifier`` is
    True when the override is well-formed but removes derived-admissible states, so it may only
    be verified by a separately verified relationship (resolved in :func:`_justify_overrides`).
    """
    rtype, members = existing.relationship_type, existing.members
    n = len(members)
    proven = proven_exhaustive(existing)
    claimed = review.exhaustive or existing.exhaustive
    derived = derived_spec(rtype, n, exhaustive=proven)
    supplied = (
        remap_spec(review.scenario_spec, review.members, members)
        if review.scenario_spec is not None
        else derived_spec(rtype, n, exhaustive=claimed)
    )
    diff = diff_specs(supplied, derived, n)
    if diff.identical:
        return supplied, proven, ScenarioProvenance(derived_exhaustive=proven), [], False

    def declared(states: tuple[tuple[int, ...], ...] | None) -> set[tuple[int, ...]]:
        return {remap_state(st, review.members, members) for st in states or ()}

    checks: list[EvidenceCheck] = []
    if not diff.exact:
        checks.append(
            check(UNCHECKABLE, EvidenceOutcome.UNKNOWN, "supplied and derived sets not comparable")
        )
    if not (review.scenario_justification and review.scenario_evidence):
        checks.append(
            check(
                METADATA_MISSING,
                EvidenceOutcome.UNKNOWN,
                "a scenario override needs a justification and evidence references",
            )
        )
    if diff.exact and (
        declared(review.declared_removed_states) != set(diff.removed)
        or declared(review.declared_added_states) != set(diff.added)
    ):
        checks.append(
            check(
                DIFF_MISMATCH,
                EvidenceOutcome.UNKNOWN,
                f"declared removed/added states do not match computed removed "
                f"{[list(x) for x in diff.removed]} added {[list(x) for x in diff.added]}",
            )
        )
    well_formed = not checks
    if diff.removed_count:
        checks.append(
            check(
                REMOVES_ADMISSIBLE_STATES,
                EvidenceOutcome.UNKNOWN,
                f"override removes {diff.removed_count} derived-admissible state(s) "
                f"{[list(x) for x in diff.removed]}; needs a separately verified relationship "
                "that excludes them",
            )
        )
    elif diff.added_count and well_formed:
        checks.append(
            check(
                SUPERSET,
                EvidenceOutcome.PASS,
                f"conservative superset: adds {[list(x) for x in diff.added]}",
            )
        )
    provenance = ScenarioProvenance(
        source="overridden",
        derived_exhaustive=proven,
        derived_spec=derived,
        removed_states=diff.removed,
        added_states=diff.added,
        removed_count=diff.removed_count,
        added_count=diff.added_count,
        comparison="exact" if diff.exact else "uncheckable",
        reviewer=review.reviewer,
        review_id=review.review_id,
        justification=review.scenario_justification,
        evidence_refs=review.scenario_evidence,
    )
    return supplied, claimed, provenance, checks, well_formed and diff.removed_count > 0


def _justify_overrides(
    by_id: dict[str, Relationship], pending: dict[str, ManualReview], notes: list[str]
) -> None:
    """Accept a state-removing override only if separately VERIFIED relationships over the same
    members, whose own scenario sets are *derived* (no chains of overrides), exclude every
    removed state."""
    for rel_id, review in sorted(pending.items()):
        rel = by_id[rel_id]
        prov = rel.scenario_provenance
        justifiers = sorted(
            (
                j
                for j in by_id.values()
                if j.relationship_id != rel_id
                and j.is_verified
                and j.scenario_provenance.source == "derived"
                and set(j.members) == set(rel.members)
            ),
            key=lambda j: j.relationship_id,
        )
        used: set[str] = set()
        covered = bool(prov.removed_states) and prov.removed_count == len(prov.removed_states)
        for state in prov.removed_states:
            hit = [j for j in justifiers if excludes(j, state, rel.members)]
            if not hit:
                covered = False
                break
            used.add(hit[0].relationship_id)
        if not covered:
            notes.append(f"{review.review_id}: override not justified by a verified relationship")
            continue
        evidence = tuple(
            check(
                JUSTIFIED,
                EvidenceOutcome.PASS,
                f"removed states excluded by verified {sorted(used)}",
            )
            if e.check == REMOVES_ADMISSIBLE_STATES
            else e
            for e in rel.evidence
        )
        status = rel.verification_status
        if review.decision is VerificationStatus.VERIFIED and all(
            e.outcome is EvidenceOutcome.PASS for e in evidence
        ):
            status = VerificationStatus.VERIFIED
        by_id[rel_id] = Relationship.model_validate(
            rel.model_copy(
                update={
                    "evidence": evidence,
                    "verification_status": status,
                    "scenario_provenance": prov.model_copy(
                        update={"justified_by": tuple(sorted(used))}
                    ),
                }
            ).model_dump()
        )
        notes.append(f"{review.review_id}: override justified by {sorted(used)}")


def revalidate(
    relationships: Sequence[Relationship], catalog: Catalog, *, as_of: datetime
) -> list[Relationship]:
    """Demote relationships whose members' rules changed or disappeared."""
    markets = catalog.markets_by_id()
    out = []
    for rel in relationships:
        if rel.verification_status is VerificationStatus.REJECTED:
            out.append(rel)
            continue
        gone = [m for m in rel.members if m not in markets]
        changed = [
            m for m in rel.members if m in markets and markets[m].rules_hash != rel.rules_hashes[m]
        ]
        if gone:
            rel = rel.invalidate(f"members no longer listed: {gone}", as_of)
        elif changed:
            rel = rel.invalidate(f"rules changed for {changed}", as_of)
        out.append(rel)
    verified = {r.relationship_id for r in out if r.is_verified}
    for i, rel in enumerate(out):
        lost = [j for j in rel.scenario_provenance.justified_by if j not in verified]
        if rel.is_verified and lost:
            out[i] = rel.invalidate(
                f"scenario override justification no longer verified: {lost}", as_of
            )
    return out

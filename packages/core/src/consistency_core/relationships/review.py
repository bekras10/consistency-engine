"""Manual-review workflow (spec 8.4) backed by a version-controlled YAML file.

Safety rules:

* A review pins the members' ``rules_hash`` values. If any current hash differs, the review is
  stale: the relationship is (re)opened as CANDIDATE_REVIEW with an invalidation reason.
* A reviewer may resolve UNKNOWN evidence (e.g. free-text propositions, exceptional-resolution
  wording) but can never override FAIL evidence (a proven difference or counterexample).
* :func:`revalidate` must run whenever reference data refreshes: any rules change on a member
  demotes the relationship so it can no longer produce verified classifications.
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
    ScenarioSpec,
    VerificationStatus,
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
    exhaustive: bool = False


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
    for review in reviews.reviews:
        missing = [m for m in review.members if m not in markets]
        if missing:
            notes.append(f"{review.review_id}: skipped, unknown members {missing}")
            continue
        members = [markets[m] for m in review.members]
        existing = next((r for r in by_id.values() if _same(r, review)), None)
        record = ReviewRecord(
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
                exhaustive=review.exhaustive,
                scenario=review.scenario_spec,
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
        scenario = review.scenario_spec or existing.scenario_spec
        exhaustive = review.exhaustive or existing.exhaustive
        updated = existing.model_copy(
            update={
                "verification_status": status,
                "evidence": evidence,
                "reviewer": review.reviewer,
                "reviews": (*existing.reviews, record),
                "scenario_spec": scenario,
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
    return [by_id[k] for k in sorted(by_id)], notes


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
    return out

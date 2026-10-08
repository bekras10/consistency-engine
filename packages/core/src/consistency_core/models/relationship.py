"""Relationships between markets and their verification state."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import AwareDatetime, Field, model_validator

from consistency_core.models.common import FrozenModel


class RelationshipType(StrEnum):
    IMPLICATION = "implication"
    """members = (A, B): A implies B.  P(A) <= P(B)."""
    MUTUALLY_EXCLUSIVE = "mutually_exclusive"
    """At most one member settles YES.  sum P <= 1.  Says nothing about at-least-one."""
    EXHAUSTIVE_PARTITION = "exhaustive_partition"
    """Exactly one member settles YES.  sum P = 1."""
    EQUIVALENT = "equivalent"
    """members = (A, B) settle identically in every admissible state.  P(A) = P(B)."""
    NESTED_THRESHOLDS = "nested_thresholds"
    """members ordered narrowest -> broadest: m0 => m1 => ... (a chain of implications)."""
    DISJOINT_INTERVALS = "disjoint_intervals"
    """Pairwise disjoint numeric intervals; exhaustive only if coverage is proven."""


class VerificationStatus(StrEnum):
    VERIFIED = "verified"
    CANDIDATE_REVIEW = "candidate_review"
    REJECTED = "rejected"


class EvidenceOutcome(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    UNKNOWN = "unknown"


class EvidenceCheck(FrozenModel):
    check: str
    outcome: EvidenceOutcome
    detail: str


class ScenarioKind(StrEnum):
    EXPLICIT = "explicit"
    """Enumerated admissible outcome vectors (truth table)."""
    CARDINALITY = "cardinality"
    """All vectors with min_yes <= (#YES) <= max_yes (exclusive / exhaustive groups)."""
    CHAIN = "chain"
    """Monotone chain m0 => m1 => ... => m_{n-1}: vectors of the form 0...01...1."""


class ScenarioSpec(FrozenModel):
    """Formal description of the admissible terminal states over ``Relationship.members``.

    States are binary vectors aligned with the member order (1 = settles YES).
    """

    kind: ScenarioKind
    explicit_states: tuple[tuple[int, ...], ...] | None = None
    min_yes: int | None = None
    max_yes: int | None = None

    @model_validator(mode="after")
    def _check(self) -> ScenarioSpec:
        if self.kind is ScenarioKind.EXPLICIT:
            if not self.explicit_states:
                raise ValueError("explicit scenario spec needs at least one state")
            for s in self.explicit_states:
                if any(b not in (0, 1) for b in s):
                    raise ValueError("scenario states are binary vectors")
            if len(set(self.explicit_states)) != len(self.explicit_states):
                raise ValueError("duplicate scenario state")
        elif self.kind is ScenarioKind.CARDINALITY:
            if (
                self.min_yes is None
                or self.max_yes is None
                or not (0 <= self.min_yes <= self.max_yes)
            ):
                raise ValueError("cardinality spec needs 0 <= min_yes <= max_yes")
        return self


class ScenarioProvenance(FrozenModel):
    """Where a relationship's admissible scenario set came from.

    ``derived``: the set implied by the relationship type (and *proven* exhaustiveness).
    ``overridden``: a reviewer supplied a different set; the diff against the derived set is
    recorded. Removing derived-admissible states asserts a stronger relationship and is only
    acceptable when ``justified_by`` names separately verified relationships that exclude them.
    """

    source: Literal["derived", "overridden"] = "derived"
    derived_exhaustive: bool | None = None
    """Exhaustiveness the derived set was computed with; None = ``Relationship.exhaustive``."""
    derived_spec: ScenarioSpec | None = None
    removed_states: tuple[tuple[int, ...], ...] = ()
    added_states: tuple[tuple[int, ...], ...] = ()
    removed_count: int = 0
    added_count: int = 0
    comparison: Literal["exact", "uncheckable"] = "exact"
    """``uncheckable``: the two sets could not be compared exactly (too large, mixed kinds)."""
    justified_by: tuple[str, ...] = ()
    reviewer: str | None = None
    review_id: str | None = None
    justification: str | None = None
    evidence_refs: tuple[str, ...] = ()

    @property
    def removes_states(self) -> bool:
        return bool(self.removed_states) or self.removed_count > 0


class ReviewRecord(FrozenModel):
    reviewer: str
    decision: VerificationStatus
    reasoning: str
    reviewed_at: AwareDatetime
    source: str
    """Where the review lives (e.g. ``fixtures/relationships/manual-reviews.yaml#id``)."""


class Relationship(FrozenModel):
    relationship_id: str
    relationship_type: RelationshipType
    members: tuple[str, ...]
    constraints: tuple[str, ...]
    """Human-readable formal constraints, e.g. ``P(A) <= P(B)``."""
    scenario_spec: ScenarioSpec
    scenario_provenance: ScenarioProvenance = Field(default_factory=ScenarioProvenance)
    exhaustive: bool = False
    """True only when exhaustiveness is independently proven (partitions, covered intervals)."""
    verification_status: VerificationStatus
    evidence: tuple[EvidenceCheck, ...] = ()
    reasoning: str
    rules_hashes: dict[str, str]
    reviewer: str | None = None
    reviews: tuple[ReviewRecord, ...] = ()
    discovered_by: str
    fingerprint: str
    created_at: AwareDatetime
    updated_at: AwareDatetime
    invalidation_reason: str | None = None

    @model_validator(mode="after")
    def _check(self) -> Relationship:
        n = len(self.members)
        if n < 2:
            raise ValueError("a relationship needs at least two members")
        if len(set(self.members)) != n:
            raise ValueError("duplicate relationship member")
        pairwise = (RelationshipType.IMPLICATION, RelationshipType.EQUIVALENT)
        if self.relationship_type in pairwise and n != 2:
            raise ValueError(f"{self.relationship_type} relates exactly two markets")
        if set(self.rules_hashes) != set(self.members):
            raise ValueError("rules_hashes must cover exactly the members")
        spec = self.scenario_spec
        if spec.explicit_states is not None and any(len(s) != n for s in spec.explicit_states):
            raise ValueError("scenario state length must equal member count")
        if self.verification_status is VerificationStatus.VERIFIED and any(
            e.outcome is not EvidenceOutcome.PASS for e in self.evidence
        ):
            raise ValueError("a VERIFIED relationship cannot carry failing/unknown evidence")
        prov = self.scenario_provenance
        if self.verification_status is VerificationStatus.VERIFIED:
            if prov.removes_states and not prov.justified_by:
                raise ValueError(
                    "a VERIFIED relationship's scenario override removes admissible states "
                    "without a separately verified justification"
                )
            if prov.comparison == "uncheckable":
                raise ValueError("a VERIFIED relationship needs an exactly checked scenario set")
        if self.updated_at < self.created_at:
            raise ValueError("updated_at precedes created_at")
        return self

    @property
    def is_verified(self) -> bool:
        return self.verification_status is VerificationStatus.VERIFIED

    def invalidate(
        self, reason: str, at: AwareDatetime, *, needs_review: bool = True
    ) -> Relationship:
        """Return a copy that can no longer produce verified classifications.

        ``needs_review=True`` (e.g. rules changed) -> CANDIDATE_REVIEW; otherwise REJECTED.
        """
        status = (
            VerificationStatus.CANDIDATE_REVIEW if needs_review else VerificationStatus.REJECTED
        )
        return self.model_copy(
            update={
                "verification_status": status,
                "invalidation_reason": reason,
                "updated_at": at,
            }
        )

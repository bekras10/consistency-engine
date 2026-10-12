"""Enter and exit intervals for a monitored market and relationship set.

A rules-hash change is an exit of the old fingerprint and an entrance of the
new one. The caller persists those intervals. This module does not contact
the exchange.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from consistency_core.models.market import Catalog
from consistency_core.models.relationship import Relationship
from consistency_core.relationships.discovery import discover
from consistency_core.relationships.review import revalidate


@dataclass(frozen=True)
class MembershipChange:
    subject_kind: str
    subject_id: str
    action: str
    at_ms: int
    fingerprint: str
    verification_status: str


def diff_membership(
    previous: Mapping[str, str],
    current: Mapping[str, str],
    *,
    subject_kind: str,
    at_ms: int,
    verification: Mapping[str, str] | None = None,
) -> list[MembershipChange]:
    """``previous`` and ``current`` map subject id to fingerprint."""
    status_of = verification or {}
    changes: list[MembershipChange] = []
    for subject_id in sorted(set(previous) | set(current)):
        before = previous.get(subject_id)
        after = current.get(subject_id)
        if before == after:
            continue
        if before is not None:
            changes.append(
                MembershipChange(
                    subject_kind=subject_kind,
                    subject_id=subject_id,
                    action="exit",
                    at_ms=at_ms,
                    fingerprint=before,
                    verification_status=status_of.get(subject_id, ""),
                )
            )
        if after is not None:
            changes.append(
                MembershipChange(
                    subject_kind=subject_kind,
                    subject_id=subject_id,
                    action="enter",
                    at_ms=at_ms,
                    fingerprint=after,
                    verification_status=status_of.get(subject_id, ""),
                )
            )
    return changes


def revise_relationships(
    catalog: Catalog,
    current: Sequence[Relationship],
    *,
    as_of: object,
) -> list[Relationship]:
    """Re-run the strict verifier after a catalog refresh. Titles do not verify."""
    from datetime import datetime

    if not isinstance(as_of, datetime):
        raise TypeError("as_of must be a datetime")
    demoted = revalidate(current, catalog, as_of=as_of)
    discovered = discover(catalog, as_of=as_of)
    by_id = {item.relationship_id: item for item in discovered}
    for item in demoted:
        if item.relationship_id not in by_id or not item.is_verified:
            by_id[item.relationship_id] = item
    return [by_id[key] for key in sorted(by_id)]

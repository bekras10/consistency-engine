"""Complete event families and one batched relationship review.

Markets are kept or dropped as a whole event. A later event that does not fit
the remaining budget is skipped entirely. Title text is not settlement evidence.
A proposal records the rules text and rules hash the strict verifier already
uses. Applying the proposal file does not mark a relationship VERIFIED.
Approved settlement fields go through :func:`apply_settlement_overlay` and then
the existing discovery and manual-review functions.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime

from consistency_core.models.market import Catalog, Market
from consistency_core.models.relationship import Relationship
from consistency_core.models.settlement import SettlementSpec
from consistency_core.relationships.discovery import discover

SETTLEMENT_KEYS: tuple[str, ...] = (
    "underlying_id",
    "measurement",
    "location",
    "observation_window",
    "settlement_source",
    "methodology",
    "unit",
    "early_close_policy",
    "exceptional_resolution",
    "rounding",
    "terms",
    "value_domain_lower",
    "value_domain_upper",
)


def select_complete_families(
    markets: Sequence[Market], *, limit: int
) -> tuple[tuple[Market, ...], list[str]]:
    """Deterministic whole-event selection. Never returns a partial event."""
    if limit < 1:
        raise ValueError("limit must be positive")
    grouped: dict[str, list[Market]] = defaultdict(list)
    for market in markets:
        grouped[market.event_id].append(market)
    chosen: list[Market] = []
    skipped: list[str] = []
    for event_id in sorted(grouped):
        group = sorted(grouped[event_id], key=lambda item: item.market_id)
        remaining = limit - len(chosen)
        if len(group) > remaining:
            skipped.append(
                f"{event_id}: family of {len(group)} exceeds remaining budget {remaining}"
            )
            continue
        chosen.extend(group)
    return tuple(chosen), skipped


def apply_settlement_overlay(
    catalog: Catalog, overlay: Mapping[str, Mapping[str, object]]
) -> Catalog:
    """Copy markets with operator-supplied settlement fields. Titles are ignored."""
    updated: list[Market] = []
    for market in catalog.markets:
        patch = overlay.get(market.market_id)
        if not isinstance(patch, Mapping):
            updated.append(market)
            continue
        current = market.settlement.model_dump(mode="json")
        for key in SETTLEMENT_KEYS:
            if key in patch:
                current[key] = patch[key]
        spec = SettlementSpec.model_validate(current)
        source = spec.settlement_source or market.settlement_source
        updated.append(market.model_copy(update={"settlement": spec, "settlement_source": source}))
    return catalog.model_copy(update={"markets": tuple(updated)})


def relationship_proposals(catalog: Catalog, *, as_of: datetime) -> list[dict[str, object]]:
    """One proposal per discovered relationship, with rule excerpts and hashes."""
    markets = catalog.markets_by_id()
    proposals: list[dict[str, object]] = []
    for rel in discover(catalog, as_of=as_of):
        proposals.append(_proposal(rel, markets))
    return proposals


def _proposal(rel: Relationship, markets: Mapping[str, Market]) -> dict[str, object]:
    members: list[dict[str, object]] = []
    for market_id in rel.members:
        market = markets[market_id]
        members.append(
            {
                "market_id": market_id,
                "rules_hash": market.rules_hash,
                "rules_version": market.rules_version,
                "rules_excerpt": market.settlement_rules,
                "settlement_source": market.settlement.settlement_source,
            }
        )
    edge_cases = [check.detail for check in rel.evidence if check.outcome.value != "pass"]
    return {
        "relationship_id": rel.relationship_id,
        "relationship_type": rel.relationship_type.value,
        "verification": rel.verification_status.value,
        "members": members,
        "constraints": list(rel.constraints),
        "proposed_logic": rel.reasoning,
        "evidence": [check.model_dump(mode="json") for check in rel.evidence],
        "edge_cases": edge_cases,
        "rules_hashes": dict(rel.rules_hashes),
        "grants_verified": False,
    }

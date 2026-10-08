"""Scenario-set integrity: every admissible scenario set is checked against the set derived from
the relationship semantics.

The *derived* set is what the relationship type itself guarantees (with exhaustiveness only when
proven): A => B admits 00, 01, 11; A <=> B admits 00, 11; a chain admits the monotone vectors;
an exclusive group (or non-covering interval bins) admits "at most one YES"; a proven partition
admits "exactly one YES".

* A supplied set that is a superset of the derived set is conservative (more states can only
  lower a worst case) and is accepted, but recorded.
* A supplied set that removes derived-admissible states asserts a stronger relationship. It can
  only back a VERIFIED relationship when separately verified relationships with *derived*
  scenario sets exclude every removed state (e.g. a verified equivalence excludes 01 and 10).
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

from consistency_core.models.relationship import (
    Relationship,
    RelationshipType,
    ScenarioKind,
    ScenarioSpec,
)
from consistency_core.relationships.scenarios import ScenarioSpace, State
from consistency_core.relationships.verification import default_scenarios

REMOVES_ADMISSIBLE_STATES = "SCENARIO_OVERRIDE_REMOVES_ADMISSIBLE_STATES"
METADATA_MISSING = "SCENARIO_OVERRIDE_METADATA_MISSING"
DIFF_MISMATCH = "SCENARIO_OVERRIDE_DIFF_MISMATCH"
UNCHECKABLE = "SCENARIO_SET_UNCHECKABLE"
SUPERSET = "SCENARIO_OVERRIDE_SUPERSET"
JUSTIFIED = "SCENARIO_OVERRIDE_JUSTIFIED"


def derived_spec(rtype: RelationshipType, n: int, *, exhaustive: bool) -> ScenarioSpec:
    return default_scenarios(rtype, n, exhaustive=exhaustive)


def derived_states(rtype: RelationshipType, n: int, *, exhaustive: bool) -> set[State]:
    return set(ScenarioSpace(derived_spec(rtype, n, exhaustive=exhaustive), n).iter_states())


@dataclass(frozen=True)
class SpecDiff:
    removed: tuple[State, ...]
    """Derived-admissible states the supplied set omits (listed when enumerable)."""
    added: tuple[State, ...]
    removed_count: int
    added_count: int
    exact: bool

    @property
    def identical(self) -> bool:
        return self.exact and self.removed_count == 0 and self.added_count == 0


def _card_range(spec: ScenarioSpec, n: int) -> range:
    assert spec.min_yes is not None and spec.max_yes is not None
    return range(spec.min_yes, min(spec.max_yes, n) + 1)


def diff_specs(supplied: ScenarioSpec, derived: ScenarioSpec, n: int) -> SpecDiff:
    a, b = ScenarioSpace(supplied, n), ScenarioSpace(derived, n)
    if a.enumerable and b.enumerable:
        sa, sb = set(a.iter_states()), set(b.iter_states())
        removed, added = tuple(sorted(sb - sa)), tuple(sorted(sa - sb))
        return SpecDiff(removed, added, len(removed), len(added), exact=True)
    if supplied.kind is ScenarioKind.CARDINALITY and derived.kind is ScenarioKind.CARDINALITY:
        ra, rb = set(_card_range(supplied, n)), set(_card_range(derived, n))
        return SpecDiff(
            (),
            (),
            sum(math.comb(n, k) for k in rb - ra),
            sum(math.comb(n, k) for k in ra - rb),
            exact=True,
        )
    if supplied.kind is ScenarioKind.CHAIN and derived.kind is ScenarioKind.CHAIN:
        return SpecDiff((), (), 0, 0, exact=True)
    return SpecDiff((), (), 0, 0, exact=False)


def remap_state(state: Sequence[int], src: Sequence[str], dst: Sequence[str]) -> State:
    by = dict(zip(src, state, strict=True))
    return tuple(by[m] for m in dst)


def remap_spec(spec: ScenarioSpec, src: Sequence[str], dst: Sequence[str]) -> ScenarioSpec:
    """Re-align an explicit truth table from member order ``src`` to ``dst``."""
    if spec.kind is not ScenarioKind.EXPLICIT or tuple(src) == tuple(dst):
        return spec
    assert spec.explicit_states is not None
    return spec.model_copy(
        update={"explicit_states": tuple(remap_state(s, src, dst) for s in spec.explicit_states)}
    )


def proven_exhaustive(rel: Relationship) -> bool:
    prov = rel.scenario_provenance
    return rel.exhaustive if prov.derived_exhaustive is None else prov.derived_exhaustive


def relationship_diff(rel: Relationship) -> SpecDiff:
    n = len(rel.members)
    derived = derived_spec(rel.relationship_type, n, exhaustive=proven_exhaustive(rel))
    return diff_specs(rel.scenario_spec, derived, n)


def excludes(justifier: Relationship, state: State, members: Sequence[str]) -> bool:
    """True if ``justifier`` (same member set) proves ``state`` cannot occur."""
    mapped = remap_state(state, members, justifier.members)
    return not ScenarioSpace(justifier.scenario_spec, len(justifier.members)).contains(mapped)


def integrity_reasons(rel: Relationship) -> list[str]:
    """Re-check a relationship's scenario set independently of how it was built (the evaluator
    calls this, so a tampered or stale ``scenario_spec`` can never price as VERIFIED)."""
    diff = relationship_diff(rel)
    prov = rel.scenario_provenance
    if not diff.exact:
        return [UNCHECKABLE]
    if diff.removed_count == 0:
        return []
    recorded_ok = (
        prov.source == "overridden"
        and prov.justified_by
        and prov.removed_count == diff.removed_count
        and set(prov.removed_states) == set(diff.removed)
    )
    return [] if recorded_ok else [REMOVES_ADMISSIBLE_STATES]


def pricing_specs(rel: Relationship) -> list[ScenarioSpec]:
    """Scenario sets a payoff must be minimised over. When integrity fails, the derived set is
    included as well, so a payoff never relies on unjustifiably removed states."""
    if not integrity_reasons(rel):
        return [rel.scenario_spec]
    n = len(rel.members)
    return [rel.scenario_spec, derived_spec(rel.relationship_type, n, exhaustive=False)]

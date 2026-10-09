"""Payoff over every admissible terminal state (spec 6.3 / 5.6).

A portfolio of binary legs pays, per basket unit, in state ``s`` (s_i = 1 if member i is YES):

    payoff(s) = sum_{YES legs on i} r * s_i  +  sum_{NO legs on i} r * (1 - s_i)
              = constant + sum_i w_i * s_i

with ``constant = sum of NO-leg ratios`` and ``w_i = (YES ratio on i) - (NO ratio on i)``.
The guaranteed (worst-case) payoff is the exact minimum over the relationship's admissible
states — enumerated for small spaces, constraint-based for large cardinality groups. It is never
assumed from the template.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from consistency_core.models.common import FrozenModel, Side
from consistency_core.models.relationship import Relationship, ScenarioSpec
from consistency_core.money import ZERO, Dec
from consistency_core.pricing.portfolio import Portfolio
from consistency_core.relationships.scenarios import ScenarioSpace

LISTED_STATES_LIMIT = 256
"""Certificates list every state's payoff up to this many states."""


class StatePayoff(FrozenModel):
    state: dict[str, int]
    payoff_per_unit: Dec


class PayoffAnalysis(FrozenModel):
    method: str
    """``enumerated`` (every admissible state listed) or ``constraint`` (exact linear minimum)."""
    admissible_state_count: int
    scenario_sets: int = 1
    """>1 when the derived set was added because the relationship's own set failed integrity."""
    scenario_specs: tuple[ScenarioSpec, ...] = ()
    """The specs whose union was priced (members order): the admissible set is re-derivable."""
    constant: Dec
    weights: dict[str, Dec]
    min_payoff_per_unit: Dec
    worst_state: dict[str, int]
    max_payoff_per_unit: Dec
    states: tuple[StatePayoff, ...] | None


class PortfolioNotInRelationshipError(ValueError):
    pass


def payoff_coefficients(
    portfolio: Portfolio, members: tuple[str, ...]
) -> tuple[Decimal, list[Decimal]]:
    index = {m: i for i, m in enumerate(members)}
    constant = ZERO
    weights = [ZERO] * len(members)
    for leg in portfolio.legs:
        if leg.market_id not in index:
            raise PortfolioNotInRelationshipError(leg.market_id)
        i = index[leg.market_id]
        if leg.side is Side.YES:
            weights[i] += leg.ratio
        else:
            constant += leg.ratio
            weights[i] -= leg.ratio
    return constant, weights


def analyse_payoff(
    relationship: Relationship,
    portfolio: Portfolio,
    specs: Sequence[ScenarioSpec] | None = None,
) -> PayoffAnalysis:
    """Exact worst case over the union of ``specs`` (default: the relationship's own set)."""
    members = relationship.members
    constant, weights = payoff_coefficients(portfolio, members)
    used = tuple(specs or [relationship.scenario_spec])
    spaces = [ScenarioSpace(sp, len(members)) for sp in used]
    lows = [sp.min_linear(constant, weights) for sp in spaces]
    lo, worst = min(lows, key=lambda t: t[0])
    hi = max(sp.max_linear(constant, weights)[0] for sp in spaces)
    union: list[tuple[int, ...]] | None = None
    if all(sp.enumerable for sp in spaces):
        union = list(dict.fromkeys(s for sp in spaces for s in sp.iter_states()))
    count = len(union) if union is not None else sum(sp.count() for sp in spaces)
    states: tuple[StatePayoff, ...] | None = None
    if union is not None and len(union) <= LISTED_STATES_LIMIT:
        states = tuple(
            StatePayoff(
                state=dict(zip(members, s, strict=True)),
                payoff_per_unit=constant
                + sum((w for w, b in zip(weights, s, strict=True) if b), ZERO),
            )
            for s in union
        )
        assert min(sp.payoff_per_unit for sp in states) == lo
    return PayoffAnalysis(
        method="enumerated" if states is not None else "constraint",
        admissible_state_count=count,
        scenario_sets=len(spaces),
        scenario_specs=used,
        constant=constant,
        weights=dict(zip(members, weights, strict=True)),
        min_payoff_per_unit=lo,
        worst_state=dict(zip(members, worst, strict=True)),
        max_payoff_per_unit=hi,
        states=states,
    )

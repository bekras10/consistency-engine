"""Portfolios: legs, canonical templates, and stable strategy identifiers."""

from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum

from consistency_core.models.common import FrozenModel, Side
from consistency_core.models.relationship import Relationship, RelationshipType
from consistency_core.money import ONE, Dec


class Template(StrEnum):
    IMPLICATION = "implication"
    """A => B: buy NO(A) + YES(B). Pays >= 1 in every admissible state."""
    YES_BASKET = "yes_basket"
    """Buy YES on every member. Pays exactly 1 only for a proven exhaustive partition."""
    NO_BASKET = "no_basket"
    """Buy NO on every member of an exclusive group. Pays >= N - 1."""
    EQUIVALENT_YES_A_NO_B = "equivalent_yes_a_no_b"
    EQUIVALENT_NO_A_YES_B = "equivalent_no_a_yes_b"
    CUSTOM = "custom"


class Leg(FrozenModel):
    market_id: str
    side: Side
    ratio: Dec = ONE
    """Contracts of this leg per basket unit."""


class Portfolio(FrozenModel):
    template: Template
    legs: tuple[Leg, ...]

    @property
    def strategy_id(self) -> str:
        return strategy_id(self.template, self.legs)


def strategy_id(template: Template, legs: Sequence[Leg]) -> str:
    """``<template>|<SIDE>:<market>[xratio],...`` — stable, human-readable, order-preserving."""
    parts = []
    for leg in legs:
        suffix = "" if leg.ratio == ONE else f"x{leg.ratio}"
        parts.append(f"{leg.side.value.upper()}:{leg.market_id}{suffix}")
    return f"{template.value}|{','.join(parts)}"


def _yes(m: str) -> Leg:
    return Leg(market_id=m, side=Side.YES)


def _no(m: str) -> Leg:
    return Leg(market_id=m, side=Side.NO)


def canonical_portfolios(rel: Relationship) -> list[Portfolio]:
    """Canonical constructions per relationship type (spec 6.2).

    The YES basket is offered only when exhaustiveness is proven (``rel.exhaustive``); every
    portfolio's guarantee is nevertheless re-derived by enumerating admissible states, never
    assumed from the template.
    """
    m = rel.members
    out: list[Portfolio] = []
    match rel.relationship_type:
        case RelationshipType.IMPLICATION:
            out.append(Portfolio(template=Template.IMPLICATION, legs=(_no(m[0]), _yes(m[1]))))
        case RelationshipType.NESTED_THRESHOLDS:
            # members narrow -> broad; every ordered pair i < j is an implication m_i => m_j
            for i in range(len(m)):
                for j in range(i + 1, len(m)):
                    out.append(
                        Portfolio(template=Template.IMPLICATION, legs=(_no(m[i]), _yes(m[j])))
                    )
        case RelationshipType.EQUIVALENT:
            out.append(
                Portfolio(template=Template.EQUIVALENT_YES_A_NO_B, legs=(_yes(m[0]), _no(m[1])))
            )
            out.append(
                Portfolio(template=Template.EQUIVALENT_NO_A_YES_B, legs=(_no(m[0]), _yes(m[1])))
            )
        case (
            RelationshipType.MUTUALLY_EXCLUSIVE
            | RelationshipType.EXHAUSTIVE_PARTITION
            | RelationshipType.DISJOINT_INTERVALS
        ):
            if rel.exhaustive:
                out.append(Portfolio(template=Template.YES_BASKET, legs=tuple(_yes(x) for x in m)))
            out.append(Portfolio(template=Template.NO_BASKET, legs=tuple(_no(x) for x in m)))
    return out

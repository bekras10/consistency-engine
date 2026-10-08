"""Turning fair probabilities into displayed bid ladders and incremental deltas.

For fair YES probability p and half-spread h, the market maker bids YES at grid prices
<= p - h and NO at grid prices <= (1 - p) - h. Hence YES ask = 1 - NO bid >= p + h: the
baseline never offers a book-implied arbitrage, and the book is never crossed
(best YES bid + best NO bid <= 1 - 2h < 1).
"""

from __future__ import annotations

import functools
import json
import random
from bisect import bisect_right
from dataclasses import dataclass, field, replace
from decimal import Decimal
from fractions import Fraction
from typing import Any

from consistency_core.models.common import Side
from consistency_core.money import ZERO
from consistency_core.ticks import parse_price_grid


@dataclass(frozen=True)
class GridCache:
    prices: list[Decimal]
    fracs: list[Fraction]

    @staticmethod
    def of(metadata: dict[str, Any]) -> GridCache:
        return _grid_cache(json.dumps(metadata, sort_keys=True))

    def best_index_at_or_below(self, ceiling: Fraction) -> int:
        return bisect_right(self.fracs, ceiling) - 1


@functools.lru_cache(maxsize=64)
def _grid_cache(key: str) -> GridCache:
    prices = parse_price_grid(json.loads(key)).valid_prices()
    return GridCache(prices=prices, fracs=[Fraction(p) for p in prices])


@dataclass(frozen=True)
class LadderSpec:
    half_spread: Decimal
    levels: int
    qty_lo: int
    qty_hi: int
    fractional: bool
    gaps: tuple[int, ...]
    """Grid-index gaps between successive levels (fixed per market to avoid flicker)."""


@dataclass(frozen=True)
class ThinTop:
    """Depth-limited top level on one bid side (used by the depth-eliminates scenario)."""

    side: Side
    quantity: Decimal
    depth_fair_yes: Fraction


@dataclass
class RestingBook:
    yes: dict[Decimal, Decimal] = field(default_factory=dict)
    no: dict[Decimal, Decimal] = field(default_factory=dict)

    def side(self, side: Side) -> dict[Decimal, Decimal]:
        return self.yes if side is Side.YES else self.no

    def levels(self, side: Side) -> list[tuple[Decimal, Decimal]]:
        return sorted(self.side(side).items(), key=lambda kv: kv[0], reverse=True)


def target_prices(
    fair: Fraction, spec: LadderSpec, grid: GridCache, *, below: Decimal | None = None
) -> list[Decimal]:
    """Bid ladder: best level = highest grid price <= fair - h (and < ``below`` if given)."""
    idx = grid.best_index_at_or_below(fair - Fraction(spec.half_spread))
    if below is not None:
        idx = min(idx, grid.best_index_at_or_below(Fraction(below)) - 1)
    out: list[Decimal] = []
    k = 0
    while idx >= 0 and len(out) < spec.levels:
        out.append(grid.prices[idx])
        idx -= spec.gaps[k % len(spec.gaps)]
        k += 1
    return out


def draw_quantity(rng: random.Random, spec: LadderSpec) -> Decimal:
    q = Decimal(rng.randint(spec.qty_lo, spec.qty_hi))
    if spec.fractional:
        q += Decimal(rng.randint(0, 99)) / 100
    return q


def requote(
    book: RestingBook,
    fair_yes: Fraction,
    spec: LadderSpec,
    grid: GridCache,
    rng: random.Random,
    *,
    thin: ThinTop | None = None,
    churn_permille: int = 0,
) -> list[tuple[Side, Decimal, Decimal]]:
    """Move ``book`` to its new target ladders; return deltas (side, price, signed change).

    Deltas are ordered all-decreases-first, then increases, each group sorted by (side, price),
    so every intermediate state's price set is a subset of the (uncrossed) final state's.
    """
    deltas: list[tuple[Side, Decimal, Decimal]] = []
    for side in (Side.YES, Side.NO):
        fair = fair_yes if side is Side.YES else 1 - fair_yes
        current = book.side(side)
        new: dict[Decimal, Decimal] = {}
        if thin is not None and thin.side is side:
            top = target_prices(fair, spec, grid)[:1]
            if top:
                new[top[0]] = thin.quantity
                depth_fair = thin.depth_fair_yes if side is Side.YES else 1 - thin.depth_fair_yes
                deeper = target_prices(
                    depth_fair, replace(spec, levels=spec.levels - 1), grid, below=top[0]
                )
                for p in deeper:
                    new[p] = current.get(p) or draw_quantity(rng, spec)
        else:
            for p in target_prices(fair, spec, grid):
                q = current.get(p)
                new[p] = q if q is not None else draw_quantity(rng, spec)
            if churn_permille and new and rng.randint(0, 999) < churn_permille:
                p = rng.choice(sorted(new))
                change = Decimal(rng.randint(-(spec.qty_hi // 4), spec.qty_hi // 4))
                if change != ZERO and new[p] + change > ZERO:
                    new[p] = new[p] + change
        for p in sorted(set(current) | set(new)):
            d = new.get(p, ZERO) - current.get(p, ZERO)
            if d != ZERO:
                deltas.append((side, p, d))
        current.clear()
        current.update(new)
    deltas.sort(key=lambda d: (0 if d[2] < ZERO else 1, d[0].value, d[1]))
    return deltas

"""Latent settlement-state models.

Each family starts from a valid probability distribution over terminal states and derives every
contract's fair probability from it, so the baseline is coherent by construction. All state is
integer/rational (``Fraction``): no transcendental floats, hence bit-identical output across
platforms for a given seed.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field
from fractions import Fraction


@dataclass
class CategoricalLatent:
    """Exactly-one-of categorical outcome (election winner, match result)."""

    weights: dict[str, int]
    min_weight: int = 8

    def step(self, rng: random.Random, vol: int) -> None:
        key = rng.choice(sorted(self.weights))
        self.weights[key] = max(self.min_weight, self.weights[key] + rng.randint(-vol, vol))

    def prob(self, outcome: str) -> Fraction:
        return Fraction(self.weights[outcome], sum(self.weights.values()))


@dataclass
class GridLatent:
    """Distribution of a raw numeric value on an exact lattice ``start + k * step``.

    Weights are a triangular kernel around ``center`` plus a floor weight of 1 everywhere, so
    every state has positive probability (no contract is ever priced at exactly 0 or 1).
    """

    start: Fraction
    step_size: Fraction
    n_states: int
    center: int
    half_width: int
    _weights: list[int] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self._recompute()

    def _recompute(self) -> None:
        self._weights = [
            1 + max(0, self.half_width - abs(k - self.center)) * 4 for k in range(self.n_states)
        ]
        self._prefix = [0]
        for w in self._weights:
            self._prefix.append(self._prefix[-1] + w)

    def prob_range(self, k_lo: int, k_hi: int) -> Fraction:
        """P(k_lo <= k <= k_hi) — contract YES sets are lattice intervals."""
        if k_hi < k_lo:
            return Fraction(0)
        return Fraction(self._prefix[k_hi + 1] - self._prefix[k_lo], self._prefix[-1])

    def step(self, rng: random.Random, vol: int) -> None:
        move = rng.randint(-vol, vol)
        self.center = min(self.n_states - 1, max(0, self.center + move))
        self._recompute()

    def value(self, k: int) -> Fraction:
        return self.start + k * self.step_size

    def prob_where(self, predicate: Callable[[Fraction], bool]) -> Fraction:
        total = sum(self._weights)
        hit = sum(w for k, w in enumerate(self._weights) if predicate(self.value(k)))
        return Fraction(hit, total)


@dataclass
class BernoulliLatent:
    permille: int
    lo: int = 30
    hi: int = 970

    def step(self, rng: random.Random, vol: int) -> None:
        self.permille = min(self.hi, max(self.lo, self.permille + rng.randint(-vol, vol)))

    def prob(self) -> Fraction:
        return Fraction(self.permille, 1000)


@dataclass
class ChainLatent:
    """P(B) = a, P(A | B) = b, A => B (e.g. wins title => reaches final)."""

    a_permille: int
    b_permille: int

    def step(self, rng: random.Random, vol: int) -> None:
        self.a_permille = min(950, max(100, self.a_permille + rng.randint(-vol, vol)))
        self.b_permille = min(900, max(100, self.b_permille + rng.randint(-vol, vol)))

    def prob_b(self) -> Fraction:
        return Fraction(self.a_permille, 1000)

    def prob_a(self) -> Fraction:
        return Fraction(self.a_permille * self.b_permille, 1_000_000)


@dataclass
class CompositeLatent:
    """Independent sub-models evolving together (e.g. two matches in one family)."""

    parts: list[CategoricalLatent | GridLatent | BernoulliLatent | ChainLatent]

    def step(self, rng: random.Random, vol: int) -> None:
        for p in self.parts:
            p.step(rng, vol)


LatentModel = CategoricalLatent | GridLatent | BernoulliLatent | ChainLatent | CompositeLatent

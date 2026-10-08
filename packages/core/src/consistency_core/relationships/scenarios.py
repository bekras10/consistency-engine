"""Admissible terminal states of a relationship (spec 8.3).

A state is a binary vector aligned with ``Relationship.members`` (1 = settles YES).

* EXPLICIT: an enumerated truth table.
* CHAIN: ``m0 => m1 => ... => m_{n-1}``; exactly the ``n + 1`` vectors ``0..0 1..1``.
* CARDINALITY: every vector with ``min_yes <= #YES <= max_yes``. For large groups the space is
  never enumerated: a *linear* payoff ``c + sum w_i s_i`` is minimized exactly by taking the
  ``k`` smallest weights for each admissible ``k`` (constraint-based evaluation).

All portfolio payoffs in this engine are linear in the state vector, so :meth:`min_linear` is an
exact worst case over every admissible state, whether or not the space is enumerable.
"""

from __future__ import annotations

import itertools
import math
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from decimal import Decimal

from consistency_core.models.relationship import ScenarioKind, ScenarioSpec

ENUMERATION_LIMIT = 4096
"""Spaces up to this size are listed state-by-state in proof certificates."""

State = tuple[int, ...]


class ScenarioSpaceError(ValueError):
    pass


@dataclass(frozen=True)
class ScenarioSpace:
    spec: ScenarioSpec
    n: int

    def __post_init__(self) -> None:
        if self.n < 1:
            raise ScenarioSpaceError("scenario space needs at least one member")
        spec = self.spec
        if spec.kind is ScenarioKind.EXPLICIT:
            assert spec.explicit_states is not None
            if any(len(s) != self.n for s in spec.explicit_states):
                raise ScenarioSpaceError("explicit state length != member count")
        elif spec.kind is ScenarioKind.CARDINALITY:
            assert spec.min_yes is not None
            if spec.min_yes > self.n:
                raise ScenarioSpaceError("cardinality space is empty (min_yes > n)")

    # ------------------------------------------------------------------ size / membership
    @property
    def _bounds(self) -> tuple[int, int]:
        assert self.spec.min_yes is not None
        assert self.spec.max_yes is not None
        return self.spec.min_yes, min(self.spec.max_yes, self.n)

    def count(self) -> int:
        match self.spec.kind:
            case ScenarioKind.EXPLICIT:
                assert self.spec.explicit_states is not None
                return len(self.spec.explicit_states)
            case ScenarioKind.CHAIN:
                return self.n + 1
            case ScenarioKind.CARDINALITY:
                lo, hi = self._bounds
                return sum(math.comb(self.n, k) for k in range(lo, hi + 1))

    @property
    def enumerable(self) -> bool:
        return self.count() <= ENUMERATION_LIMIT

    def contains(self, state: Sequence[int]) -> bool:
        s = tuple(state)
        if len(s) != self.n or any(b not in (0, 1) for b in s):
            return False
        match self.spec.kind:
            case ScenarioKind.EXPLICIT:
                assert self.spec.explicit_states is not None
                return s in self.spec.explicit_states
            case ScenarioKind.CHAIN:
                return all(a <= b for a, b in itertools.pairwise(s))
            case ScenarioKind.CARDINALITY:
                lo, hi = self._bounds
                return lo <= sum(s) <= hi

    def iter_states(self) -> Iterator[State]:
        """Deterministic order. Refuses spaces larger than :data:`ENUMERATION_LIMIT`."""
        if not self.enumerable:
            raise ScenarioSpaceError(
                f"{self.count()} states exceed the enumeration limit; use min_linear"
            )
        match self.spec.kind:
            case ScenarioKind.EXPLICIT:
                assert self.spec.explicit_states is not None
                yield from self.spec.explicit_states
            case ScenarioKind.CHAIN:
                for k in range(self.n, -1, -1):  # k leading zeros
                    yield tuple([0] * k + [1] * (self.n - k))
            case ScenarioKind.CARDINALITY:
                lo, hi = self._bounds
                for k in range(lo, hi + 1):
                    for ones in itertools.combinations(range(self.n), k):
                        yield tuple(1 if i in ones else 0 for i in range(self.n))

    # ------------------------------------------------------------------ linear optimisation
    def min_linear(self, constant: Decimal, weights: Sequence[Decimal]) -> tuple[Decimal, State]:
        """Exact ``min over admissible s of constant + sum_i weights[i] * s_i`` and an argmin
        (ties broken by the deterministic state order)."""
        if len(weights) != self.n:
            raise ScenarioSpaceError("weights length != member count")
        if self.spec.kind is ScenarioKind.CARDINALITY:
            lo, hi = self._bounds
            order = sorted(range(self.n), key=lambda i: (weights[i], i))
            best: tuple[Decimal, State] | None = None
            prefix = constant
            for k in range(hi + 1):
                if k > 0:
                    prefix += weights[order[k - 1]]
                if k < lo:
                    continue
                if best is None or prefix < best[0]:
                    chosen = set(order[:k])
                    best = (prefix, tuple(1 if i in chosen else 0 for i in range(self.n)))
            assert best is not None
            return best
        best_val: Decimal | None = None
        best_state: State = ()
        for s in self.iter_states():
            v = constant + sum((w for w, b in zip(weights, s, strict=True) if b), Decimal(0))
            if best_val is None or v < best_val:
                best_val, best_state = v, s
        assert best_val is not None
        return best_val, best_state

    def max_linear(self, constant: Decimal, weights: Sequence[Decimal]) -> tuple[Decimal, State]:
        v, s = self.min_linear(-constant, [-w for w in weights])
        return -v, s

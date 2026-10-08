"""Exact numeric settlement semantics.

A threshold/interval contract settles on a *reported* observation ``y = R(x)``, where ``x`` is
the raw measured value and ``R`` is the settlement source's rounding step (or the identity).
For relationship proofs we map every contract to the set of raw values ``{x : R(x) in I_y}``.
Because ``R`` is monotone and maps onto the lattice ``r * Z``, that preimage is a single
interval, computed exactly with :class:`fractions.Fraction`:

=========  ===========================================
mode       preimage of lattice point ``k * r``
=========  ===========================================
half_up    ``[(k - 1/2) r, (k + 1/2) r)``
half_down  ``((k - 1/2) r, (k + 1/2) r]``
floor      ``[k r, (k + 1) r)``
ceil       ``((k - 1) r, k r]``
=========  ===========================================

This is what makes boundary mismatches (``>`` vs ``>=``, different rounding increments)
provable rather than heuristic.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction

from consistency_core.models.settlement import (
    Comparator,
    IntervalTerms,
    RoundingConvention,
    RoundingMode,
    ThresholdTerms,
)


def frac(value: Decimal | int) -> Fraction:
    return Fraction(value)


@dataclass(frozen=True, slots=True)
class Interval:
    """Real interval with optional (None = infinite) Fraction endpoints."""

    lower: Fraction | None
    lower_closed: bool
    upper: Fraction | None
    upper_closed: bool

    @staticmethod
    def everything() -> Interval:
        return Interval(None, False, None, False)

    @property
    def is_empty(self) -> bool:
        if self.lower is None or self.upper is None:
            return False
        if self.lower > self.upper:
            return True
        return self.lower == self.upper and not (self.lower_closed and self.upper_closed)

    def contains(self, x: Fraction) -> bool:
        if self.lower is not None and (
            x < self.lower or (x == self.lower and not self.lower_closed)
        ):
            return False
        return not (
            self.upper is not None
            and (x > self.upper or (x == self.upper and not self.upper_closed))
        )

    def intersect(self, other: Interval) -> Interval:
        lo, lo_c = _max_lower(self, other)
        hi, hi_c = _min_upper(self, other)
        return Interval(lo, lo_c, hi, hi_c)

    def is_subset_of(self, other: Interval) -> bool:
        if self.is_empty:
            return True
        # lower bound of self must be >= lower bound of other
        if other.lower is not None:
            if self.lower is None or self.lower < other.lower:
                return False
            if self.lower == other.lower and self.lower_closed and not other.lower_closed:
                return False
        if other.upper is not None:
            if self.upper is None or self.upper > other.upper:
                return False
            if self.upper == other.upper and self.upper_closed and not other.upper_closed:
                return False
        return True

    def disjoint_from(self, other: Interval) -> bool:
        return self.intersect(other).is_empty

    def witness_outside(self, other: Interval) -> Fraction | None:
        """A point of ``self`` not in ``other`` (counterexample to self ⊆ other), if any."""
        if self.is_subset_of(other):
            return None
        candidates: list[Fraction] = []
        for p in (self.lower, self.upper, other.lower, other.upper):
            if p is not None:
                candidates.extend((p, p - Fraction(1, 10**9), p + Fraction(1, 10**9)))
        if self.lower is not None and self.upper is not None:
            candidates.append((self.lower + self.upper) / 2)
        if self.lower is None:
            candidates.append((other.lower if other.lower is not None else Fraction(0)) - 10**9)
        if self.upper is None:
            candidates.append((other.upper if other.upper is not None else Fraction(0)) + 10**9)
        for c in sorted(set(candidates)):
            if self.contains(c) and not other.contains(c):
                return c
        return None  # pragma: no cover - unreachable for well-formed intervals

    def describe(self) -> str:
        lo = "-inf" if self.lower is None else str(self.lower)
        hi = "+inf" if self.upper is None else str(self.upper)
        return f"{'[' if self.lower_closed else '('}{lo}, {hi}{']' if self.upper_closed else ')'}"


def _max_lower(a: Interval, b: Interval) -> tuple[Fraction | None, bool]:
    if a.lower is None:
        return b.lower, b.lower_closed
    if b.lower is None:
        return a.lower, a.lower_closed
    if a.lower > b.lower:
        return a.lower, a.lower_closed
    if b.lower > a.lower:
        return b.lower, b.lower_closed
    return a.lower, a.lower_closed and b.lower_closed


def _min_upper(a: Interval, b: Interval) -> tuple[Fraction | None, bool]:
    if a.upper is None:
        return b.upper, b.upper_closed
    if b.upper is None:
        return a.upper, a.upper_closed
    if a.upper < b.upper:
        return a.upper, a.upper_closed
    if b.upper < a.upper:
        return b.upper, b.upper_closed
    return a.upper, a.upper_closed and b.upper_closed


# --------------------------------------------------------------------------- terms -> intervals
def reported_interval(terms: ThresholdTerms | IntervalTerms) -> Interval:
    """The YES set expressed on the *reported* value y."""
    if isinstance(terms, ThresholdTerms):
        v = frac(terms.value)
        match terms.comparator:
            case Comparator.GT:
                return Interval(v, False, None, False)
            case Comparator.GE:
                return Interval(v, True, None, False)
            case Comparator.LT:
                return Interval(None, False, v, False)
            case Comparator.LE:
                return Interval(None, False, v, True)
    lo = None if terms.lower is None else frac(terms.lower)
    hi = None if terms.upper is None else frac(terms.upper)
    return Interval(lo, terms.lower_inclusive, hi, terms.upper_inclusive)


def _lattice_preimage(k: int, r: Fraction, mode: RoundingMode) -> Interval:
    half = r / 2
    match mode:
        case RoundingMode.HALF_UP:
            return Interval(k * r - half, True, k * r + half, False)
        case RoundingMode.HALF_DOWN:
            return Interval(k * r - half, False, k * r + half, True)
        case RoundingMode.FLOOR:
            return Interval(k * r, True, (k + 1) * r, False)
        case RoundingMode.CEIL:
            return Interval((k - 1) * r, False, k * r, True)
        case RoundingMode.NONE:  # pragma: no cover - handled by caller
            raise ValueError("NONE has no lattice")


def raw_interval(
    terms: ThresholdTerms | IntervalTerms, rounding: RoundingConvention | None
) -> Interval:
    """Exact set of raw values x for which the contract settles YES."""
    y_set = reported_interval(terms)
    if rounding is None or rounding.mode is RoundingMode.NONE:
        return y_set
    assert rounding.increment is not None
    r = frac(rounding.increment)
    # lattice indices k with k*r in y_set
    if y_set.lower is None:
        k_lo: int | None = None
    else:
        q = y_set.lower / r
        k_lo = math.ceil(q) if y_set.lower_closed else math.floor(q) + 1
    if y_set.upper is None:
        k_hi: int | None = None
    else:
        q = y_set.upper / r
        k_hi = math.floor(q) if y_set.upper_closed else math.ceil(q) - 1
    if k_lo is not None and k_hi is not None and k_lo > k_hi:
        return Interval(Fraction(0), False, Fraction(0), False)  # empty
    if k_lo is None:
        lower, lower_closed = None, False
    else:
        pre = _lattice_preimage(k_lo, r, rounding.mode)
        lower, lower_closed = pre.lower, pre.lower_closed
    if k_hi is None:
        upper, upper_closed = None, False
    else:
        pre = _lattice_preimage(k_hi, r, rounding.mode)
        upper, upper_closed = pre.upper, pre.upper_closed
    return Interval(lower, lower_closed, upper, upper_closed)


def apply_rounding(x: Fraction, rounding: RoundingConvention | None) -> Fraction:
    """The settlement source's reported value for raw value x."""
    if rounding is None or rounding.mode is RoundingMode.NONE:
        return x
    assert rounding.increment is not None
    r = frac(rounding.increment)
    q = x / r
    match rounding.mode:
        case RoundingMode.HALF_UP:
            k = math.floor(q + Fraction(1, 2))
        case RoundingMode.HALF_DOWN:
            k = math.ceil(q - Fraction(1, 2))
        case RoundingMode.FLOOR:
            k = math.floor(q)
        case RoundingMode.CEIL:
            k = math.ceil(q)
    return k * r


def settles_yes(
    terms: ThresholdTerms | IntervalTerms, rounding: RoundingConvention | None, x: Fraction
) -> bool:
    """Direct evaluation (used by the simulator and to cross-check :func:`raw_interval`)."""
    return reported_interval(terms).contains(apply_rounding(x, rounding))


def covers(intervals: Sequence[Interval], domain: Interval) -> tuple[bool, Fraction | None]:
    """Whether the union of ``intervals`` contains ``domain``; else a witness point not covered.

    Boundary handling is exact: two pieces meeting at a point cover it only if at least one of
    them is closed there.
    """
    pieces = [p for p in (iv.intersect(domain) for iv in intervals) if not p.is_empty]
    unbounded_below = [p for p in pieces if p.lower is None]
    bounded = sorted(
        (p for p in pieces if p.lower is not None),
        key=lambda p: (p.lower, 0 if p.lower_closed else 1),
    )

    # Invariant: every domain point strictly below `frontier` is covered; `included` says whether
    # the point `frontier` itself is covered (or not required). frontier None == +infinity.
    frontier: Fraction | None
    if domain.lower is None:
        if not unbounded_below:
            first = bounded[0].lower if bounded else None
            return False, (first if first is not None else Fraction(0)) - 1
        frontier, included = None, False
        for p in unbounded_below:
            frontier, included = _advance_from(frontier, included, p, start=True)
            if frontier is None:
                return True, None
    else:
        frontier, included = domain.lower, not domain.lower_closed
        for p in unbounded_below:  # only possible if domain itself is unbounded; defensive
            frontier, included = _advance_from(frontier, included, p, start=False)

    for p in bounded:
        if frontier is None:
            return True, None
        assert p.lower is not None
        if p.lower > frontier:
            return False, frontier if not included else (frontier + p.lower) / 2
        if p.lower == frontier and not included and not p.lower_closed:
            return False, frontier
        covers_frontier_point = p.lower == frontier and p.lower_closed
        frontier, included = _advance_from(
            frontier, included or covers_frontier_point, p, start=False
        )

    if frontier is None:
        return True, None
    if domain.upper is None:
        return False, frontier if not included else frontier + 1
    if frontier > domain.upper:
        return True, None
    if frontier == domain.upper:
        if included or not domain.upper_closed:
            return True, None
        return False, frontier
    return False, frontier if not included else (frontier + domain.upper) / 2


def _advance_from(
    frontier: Fraction | None, included: bool, p: Interval, *, start: bool
) -> tuple[Fraction | None, bool]:
    """Extend the covered prefix with piece ``p`` (which starts at or before the frontier)."""
    if p.upper is None:
        return None, True
    if start and frontier is None:
        return p.upper, p.upper_closed
    assert frontier is not None
    if p.upper > frontier:
        return p.upper, p.upper_closed
    if p.upper == frontier:
        return frontier, included or p.upper_closed
    return frontier, included

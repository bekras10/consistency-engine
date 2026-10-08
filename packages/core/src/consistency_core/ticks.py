"""Market-dependent price grids.

A grid is a list of tick ranges ``[start, end]`` with a ``step``; a price is valid if it lies
strictly inside (0, 1) and on the step lattice of at least one range that contains it. This
models Kalshi-style ``price_ranges`` (e.g. finer ticks near the extremes) as well as the legacy
uniform one-cent grid. Parsing never rounds: an off-grid price is an error, not a snap.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from itertools import pairwise
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator

from consistency_core.money import CENT, ONE, ZERO, Dec, dec, is_multiple


class TickRange(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    start: Dec
    end: Dec
    step: Dec

    @model_validator(mode="after")
    def _check(self) -> TickRange:
        if not (ZERO <= self.start < self.end <= ONE):
            raise ValueError(f"tick range must satisfy 0 <= start < end <= 1: {self}")
        if self.step <= ZERO:
            raise ValueError("tick step must be positive")
        if not is_multiple(self.end - self.start, self.step):
            raise ValueError(f"range [{self.start}, {self.end}] is not a whole number of steps")
        return self

    def contains(self, price: Decimal) -> bool:
        return self.start <= price <= self.end and is_multiple(price - self.start, self.step)


class PriceGrid(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    ranges: tuple[TickRange, ...]

    @model_validator(mode="after")
    def _check(self) -> PriceGrid:
        if not self.ranges:
            raise ValueError("price grid needs at least one tick range")
        ordered = sorted(self.ranges, key=lambda r: r.start)
        for prev, nxt in pairwise(ordered):
            if nxt.start < prev.end:
                raise ValueError("tick ranges overlap")
        return self

    @classmethod
    def uniform(cls, step: Decimal = CENT) -> PriceGrid:
        return cls(ranges=(TickRange(start=ZERO, end=ONE, step=step),))

    def is_valid_price(self, price: Decimal) -> bool:
        if not (ZERO < price < ONE):
            return False
        return any(r.contains(price) for r in self.ranges)

    def require_valid(self, price: Decimal) -> Decimal:
        if not self.is_valid_price(price):
            raise ValueError(f"price {price} is not on the market's tick grid")
        return price

    def valid_prices(self) -> list[Decimal]:
        """All tradable prices, ascending (used by the simulator; grids are small)."""
        out: set[Decimal] = set()
        for r in self.ranges:
            p = r.start
            while p <= r.end:
                if ZERO < p < ONE:
                    out.add(p)
                p += r.step
        return sorted(out)

    @property
    def min_step(self) -> Decimal:
        return min(r.step for r in self.ranges)


def parse_price_grid(metadata: Mapping[str, Any]) -> PriceGrid:
    """Build a grid from exchange metadata.

    Supported shapes (field names are best-knowledge assumptions; see docs/data-contracts.md):

    * ``{"price_ranges": [{"start": "0.0000", "end": "0.1000", "step": "0.0010"}, ...]}``
    * ``{"tick_size": 1}`` — legacy integer cents per tick
    * ``{"tick_size_dollars": "0.01"}``
    """
    ranges_raw = metadata.get("price_ranges")
    if ranges_raw is not None:
        if not isinstance(ranges_raw, Sequence) or isinstance(ranges_raw, str | bytes):
            raise ValueError("price_ranges must be a list")
        ranges: list[TickRange] = []
        for item in ranges_raw:
            if not isinstance(item, Mapping):
                raise ValueError("each price range must be an object")
            ranges.append(
                TickRange(start=dec(item["start"]), end=dec(item["end"]), step=dec(item["step"]))
            )
        return PriceGrid(ranges=tuple(ranges))
    if "tick_size_dollars" in metadata:
        return PriceGrid.uniform(dec(metadata["tick_size_dollars"]))
    if "tick_size" in metadata:
        raw = metadata["tick_size"]
        if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
            raise ValueError("legacy tick_size must be a positive integer number of cents")
        return PriceGrid.uniform(Decimal(raw) * CENT)
    raise ValueError("market metadata does not describe a price grid")

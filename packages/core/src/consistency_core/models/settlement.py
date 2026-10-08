"""Structured settlement terms.

These are the facts the relationship verifier compares. ``None`` always means *unknown*, which
can never be treated as a match: an unknown field yields CANDIDATE_REVIEW, not VERIFIED.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import Field, model_validator

from consistency_core.models.common import FrozenModel
from consistency_core.money import ZERO, Dec


class Comparator(StrEnum):
    GT = ">"
    GE = ">="
    LT = "<"
    LE = "<="


class RoundingMode(StrEnum):
    """How the settlement source rounds the raw observation before comparing.

    ``NONE`` means the comparison is applied to the raw reported value with no rounding step.
    """

    NONE = "none"
    HALF_UP = "half_up"
    HALF_DOWN = "half_down"
    FLOOR = "floor"
    CEIL = "ceil"


class RoundingConvention(FrozenModel):
    mode: RoundingMode
    increment: Dec | None = None

    @model_validator(mode="after")
    def _check(self) -> RoundingConvention:
        if self.mode is RoundingMode.NONE:
            if self.increment is not None:
                raise ValueError("rounding mode NONE takes no increment")
        elif self.increment is None or self.increment <= ZERO:
            raise ValueError("rounding increment must be positive")
        return self


class ThresholdTerms(FrozenModel):
    """YES iff ``observation <comparator> value`` (after the settlement rounding step)."""

    kind: Literal["threshold"] = "threshold"
    comparator: Comparator
    value: Dec


class IntervalTerms(FrozenModel):
    """YES iff observation lies in the interval. ``None`` bounds are unbounded."""

    kind: Literal["interval"] = "interval"
    lower: Dec | None = None
    lower_inclusive: bool = True
    upper: Dec | None = None
    upper_inclusive: bool = False

    @model_validator(mode="after")
    def _check(self) -> IntervalTerms:
        if self.lower is None and self.upper is None:
            raise ValueError("interval needs at least one bound")
        if self.lower is not None and self.upper is not None:
            if self.lower > self.upper:
                raise ValueError("interval lower bound exceeds upper bound")
            if self.lower == self.upper and not (self.lower_inclusive and self.upper_inclusive):
                raise ValueError("empty interval")
        return self


class OutcomeTerms(FrozenModel):
    """YES iff the categorical event resolves to ``outcome_id`` (e.g. a candidate)."""

    kind: Literal["outcome"] = "outcome"
    outcome_id: str


class PropositionTerms(FrozenModel):
    """YES iff an arbitrary proposition holds. Only manual review can relate these."""

    kind: Literal["proposition"] = "proposition"
    proposition_id: str


ContractTerms = ThresholdTerms | IntervalTerms | OutcomeTerms | PropositionTerms


class SettlementSpec(FrozenModel):
    """Machine-comparable settlement facts for one market."""

    underlying_id: str | None = None
    measurement: str | None = None
    location: str | None = None
    observation_window: str | None = None
    settlement_source: str | None = None
    methodology: str | None = None
    unit: str | None = None
    rounding: RoundingConvention | None = None
    early_close_policy: str | None = None
    exceptional_resolution: tuple[str, ...] | None = None
    """Known exceptional outcomes (e.g. ``"void_refund"``). ``()`` = none; ``None`` = unknown."""
    value_domain_lower: Dec | None = None
    """Inclusive lower bound of admissible raw values (None = unbounded)."""
    value_domain_upper: Dec | None = None
    """Inclusive upper bound of admissible raw values (None = unbounded)."""
    terms: ContractTerms | None = Field(default=None, discriminator="kind")

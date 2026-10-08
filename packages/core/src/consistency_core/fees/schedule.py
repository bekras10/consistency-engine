"""Versioned fee schedules loaded from ``fixtures/fees/*.yaml``.

A schedule is selected by (venue, timestamp): exactly one schedule may be effective for a venue
at any instant (``effective_from`` inclusive, ``effective_to`` exclusive, ``None`` = open-ended).
Series exceptions match the series ticker EXACTLY — never by prefix.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal

import yaml
from pydantic import AwareDatetime, model_validator

from consistency_core.fees.rounding import MemberClass
from consistency_core.models.common import FrozenModel
from consistency_core.money import ZERO, Dec


class Venue(StrEnum):
    KALSHI = "kalshi"
    SYNTHETIC = "synthetic"


class Completeness(StrEnum):
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"


class ScheduleVerification(StrEnum):
    VERIFIED_AGAINST_PUBLISHED_SCHEDULE = "VERIFIED_AGAINST_PUBLISHED_SCHEDULE"
    FICTIONAL = "FICTIONAL"
    UNVERIFIED = "UNVERIFIED"


class ExceptionRow(FrozenModel):
    series: str
    maker_multiplier: Dec
    taker_multiplier: Dec
    combo_classification: str | None = None
    """For combo series (KXMVE): the combo class this row applies to."""
    note: str = ""


class FeeSchedule(FrozenModel):
    schedule_id: str
    venue: Venue
    label: str
    effective_from: AwareDatetime
    effective_to: AwareDatetime | None = None
    source_urls: tuple[str, ...] = ()
    taker_coefficient: Dec
    maker_coefficient: Dec
    default_taker_multiplier: Dec
    default_maker_multiplier: Dec
    settlement_fee: Dec = ZERO
    exceptions: tuple[ExceptionRow, ...] = ()
    combo_series: tuple[str, ...] = ()
    """Series whose multiplier depends on a combo classification that must be supplied."""
    completeness: Completeness
    verification_status: ScheduleVerification
    unlisted_series: Literal["default_multipliers", "unverified"]
    default_member_class: MemberClass | None = None
    """Set only for the fictional venue, where the member class is part of the definition."""
    notes: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _check(self) -> FeeSchedule:
        if self.effective_to is not None and self.effective_to <= self.effective_from:
            raise ValueError("effective_to must be after effective_from")
        if self.settlement_fee != ZERO:
            raise ValueError("settlement fees are not part of the event-contract model")
        keys = [(r.series, r.combo_classification) for r in self.exceptions]
        if len(set(keys)) != len(keys):
            raise ValueError("duplicate exception row")
        if (self.verification_status is ScheduleVerification.FICTIONAL) != (
            self.venue is Venue.SYNTHETIC
        ):
            raise ValueError("FICTIONAL schedules are exactly the synthetic-venue schedules")
        if self.completeness is Completeness.PARTIAL and self.unlisted_series != "unverified":
            raise ValueError("a PARTIAL exception table cannot default unlisted series")
        return self

    def is_effective(self, at: datetime) -> bool:
        return self.effective_from <= at and (self.effective_to is None or at < self.effective_to)

    def rows_for(self, series: str) -> list[ExceptionRow]:
        return [r for r in self.exceptions if r.series == series]


def load_schedule(path: Path) -> FeeSchedule:
    with path.open(encoding="utf-8") as fh:
        return FeeSchedule.model_validate(yaml.safe_load(fh))


class FeeScheduleRegistry:
    def __init__(self, schedules: Iterable[FeeSchedule]) -> None:
        self.schedules = sorted(schedules, key=lambda s: (s.venue.value, s.effective_from))
        ids = [s.schedule_id for s in self.schedules]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate schedule_id")
        for a, b in zip(self.schedules, self.schedules[1:], strict=False):
            if a.venue is b.venue and (a.effective_to is None or a.effective_to > b.effective_from):
                raise ValueError(f"overlapping schedules {a.schedule_id} / {b.schedule_id}")

    @classmethod
    def from_directory(cls, directory: Path) -> FeeScheduleRegistry:
        return cls(load_schedule(p) for p in sorted(directory.glob("*.yaml")))

    def select(self, venue: Venue, at: datetime) -> FeeSchedule | None:
        for s in self.schedules:
            if s.venue is venue and s.is_effective(at):
                return s
        return None

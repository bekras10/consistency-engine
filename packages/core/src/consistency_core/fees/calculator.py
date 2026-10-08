"""FeeCalculator: schedule selection, override resolution, and the live-fee verification gate.

A real (Kalshi) market's fee is VERIFIED only when all of the following are known:

1. the exact series ticker, matched exactly against the listed exception rows (the table is
   PARTIAL, so an unlisted series cannot be assumed to use the defaults);
2. resolved overrides (combo series such as KXMVE need a supplied combo classification that
   matches a listed row);
3. a schedule effective at the evaluation timestamp;
4. the member classification (DIRECT / NON_DIRECT) — supplied, not defaulted;
5. intermediary/FCM fees either accounted for or explicitly excluded;
6. confirmation that the fee-schedule landing page was checked for later revisions.

Otherwise the resolution is unverified (-> FEE_UNVERIFIED), although an *estimate* is still
computed when a schedule exists, using the conservative NON_DIRECT precision.

The synthetic venue uses a fictional, fully specified schedule; it is "verified by construction"
and always labelled FICTIONAL.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import Field

from consistency_core.fees.model import Role
from consistency_core.fees.rounding import MemberClass, OrderFees, buy_order_fees
from consistency_core.fees.schedule import FeeScheduleRegistry, ScheduleVerification, Venue
from consistency_core.models.common import DataSourceKind, FrozenModel
from consistency_core.models.market import Market
from consistency_core.money import Dec


class IntermediaryFees(StrEnum):
    ACCOUNTED = "accounted"
    EXPLICITLY_EXCLUDED = "explicitly_excluded"
    UNKNOWN = "unknown"


class FeeAssumptions(FrozenModel):
    member_class: MemberClass | None = None
    """None = unknown: fees are estimated at NON_DIRECT ($0.01, the coarser and therefore more
    expensive precision) and the resolution is unverified for real venues."""
    intermediary_fees: IntermediaryFees = IntermediaryFees.UNKNOWN
    schedule_revisions_checked: bool = False
    combo_classifications: dict[str, str] = Field(default_factory=dict)
    """market_id -> combo class (e.g. ``"specified"``) for combo series such as KXMVE."""


class FeeReason(StrEnum):
    UNKNOWN_VENUE = "UNKNOWN_VENUE"
    NO_SCHEDULE_EFFECTIVE = "NO_SCHEDULE_EFFECTIVE"
    SCHEDULE_UNVERIFIED = "SCHEDULE_UNVERIFIED"
    SERIES_NOT_IN_PARTIAL_TABLE = "SERIES_NOT_IN_PARTIAL_TABLE"
    COMBO_CLASSIFICATION_UNKNOWN = "COMBO_CLASSIFICATION_UNKNOWN"
    COMBO_CLASSIFICATION_NOT_LISTED = "COMBO_CLASSIFICATION_NOT_LISTED"
    AMBIGUOUS_EXCEPTION_ROWS = "AMBIGUOUS_EXCEPTION_ROWS"
    MEMBER_CLASS_UNKNOWN = "MEMBER_CLASS_UNKNOWN"
    INTERMEDIARY_FEES_UNKNOWN = "INTERMEDIARY_FEES_UNKNOWN"
    SCHEDULE_REVISIONS_NOT_CHECKED = "SCHEDULE_REVISIONS_NOT_CHECKED"


class FeeResolution(FrozenModel):
    market_id: str
    series_id: str
    venue: Venue | None
    role: Role
    at: datetime
    schedule_id: str | None
    schedule_label: str | None
    fictional: bool
    verified: bool
    reasons: tuple[FeeReason, ...]
    coefficient: Dec | None
    multiplier: Dec | None
    matched_exception: str | None
    member_class: MemberClass
    member_class_assumed: bool

    @property
    def can_estimate(self) -> bool:
        return self.coefficient is not None and self.multiplier is not None


def venue_of(market: Market) -> Venue | None:
    match market.provenance.source_kind:
        case DataSourceKind.SYNTHETIC:
            return Venue.SYNTHETIC
        case DataSourceKind.KALSHI_AUTHORIZED:
            return Venue.KALSHI
        case _:
            return None


class FeeCalculator:
    def __init__(self, registry: FeeScheduleRegistry) -> None:
        self.registry = registry

    def resolve(
        self,
        market: Market,
        at: datetime,
        assumptions: FeeAssumptions | None = None,
        role: Role = Role.TAKER,
    ) -> FeeResolution:
        a = assumptions or FeeAssumptions()
        reasons: list[FeeReason] = []
        venue = venue_of(market)
        schedule = None if venue is None else self.registry.select(venue, at)
        if venue is None:
            reasons.append(FeeReason.UNKNOWN_VENUE)
        elif schedule is None:
            reasons.append(FeeReason.NO_SCHEDULE_EFFECTIVE)

        coefficient: Decimal | None = None
        multiplier: Decimal | None = None
        matched: str | None = None
        member = a.member_class
        assumed = False
        if schedule is not None:
            coefficient = (
                schedule.taker_coefficient if role is Role.TAKER else schedule.maker_coefficient
            )
            default_mult = (
                schedule.default_taker_multiplier
                if role is Role.TAKER
                else schedule.default_maker_multiplier
            )
            rows = schedule.rows_for(market.series_id)  # exact ticker match only
            if market.series_id in schedule.combo_series:
                combo = a.combo_classifications.get(market.market_id)
                if combo is None:
                    reasons.append(FeeReason.COMBO_CLASSIFICATION_UNKNOWN)
                    rows = []
                else:
                    rows = [r for r in rows if r.combo_classification == combo]
                    if not rows:
                        reasons.append(FeeReason.COMBO_CLASSIFICATION_NOT_LISTED)
            if len(rows) > 1:
                reasons.append(FeeReason.AMBIGUOUS_EXCEPTION_ROWS)
                multiplier = max(
                    r.taker_multiplier if role is Role.TAKER else r.maker_multiplier for r in rows
                )
            elif rows:
                row = rows[0]
                matched = row.series + (
                    f"[{row.combo_classification}]" if row.combo_classification else ""
                )
                multiplier = row.taker_multiplier if role is Role.TAKER else row.maker_multiplier
            else:
                if (
                    schedule.unlisted_series == "unverified"
                    and market.series_id not in schedule.combo_series
                ):
                    reasons.append(FeeReason.SERIES_NOT_IN_PARTIAL_TABLE)
                multiplier = default_mult
            if schedule.verification_status is ScheduleVerification.UNVERIFIED:
                reasons.append(FeeReason.SCHEDULE_UNVERIFIED)
            if member is None and schedule.default_member_class is not None:
                member = schedule.default_member_class
            if venue is Venue.KALSHI:
                if a.intermediary_fees is IntermediaryFees.UNKNOWN:
                    reasons.append(FeeReason.INTERMEDIARY_FEES_UNKNOWN)
                if not a.schedule_revisions_checked:
                    reasons.append(FeeReason.SCHEDULE_REVISIONS_NOT_CHECKED)
        if member is None:
            member = MemberClass.NON_DIRECT
            assumed = True
            reasons.append(FeeReason.MEMBER_CLASS_UNKNOWN)
        return FeeResolution(
            market_id=market.market_id,
            series_id=market.series_id,
            venue=venue,
            role=role,
            at=at,
            schedule_id=None if schedule is None else schedule.schedule_id,
            schedule_label=None if schedule is None else schedule.label,
            fictional=schedule is not None
            and schedule.verification_status is ScheduleVerification.FICTIONAL,
            verified=not reasons,
            reasons=tuple(dict.fromkeys(reasons)),
            coefficient=coefficient,
            multiplier=multiplier,
            matched_exception=matched,
            member_class=member,
            member_class_assumed=assumed,
        )

    @staticmethod
    def order_fees(
        resolution: FeeResolution, fills: Sequence[tuple[Decimal, Decimal]]
    ) -> OrderFees | None:
        """Fees for one buy order; ``None`` if no estimate is possible (no schedule)."""
        if resolution.coefficient is None or resolution.multiplier is None:
            return None
        return buy_order_fees(
            fills,
            coefficient=resolution.coefficient,
            multiplier=resolution.multiplier,
            member_class=resolution.member_class,
            role=resolution.role,
        )

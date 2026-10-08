"""Fee engine: (a) model formulas, (b) exact rounding layer, versioned schedules, gate."""

from consistency_core.fees.calculator import (
    FeeAssumptions,
    FeeCalculator,
    FeeReason,
    FeeResolution,
    IntermediaryFees,
)
from consistency_core.fees.model import Role, model_fee
from consistency_core.fees.rounding import (
    FillFee,
    MemberClass,
    OrderFeeAccumulator,
    OrderFees,
    buy_order_fees,
)
from consistency_core.fees.schedule import (
    Completeness,
    ExceptionRow,
    FeeSchedule,
    FeeScheduleRegistry,
    ScheduleVerification,
    Venue,
    load_schedule,
)

__all__ = [
    "Completeness",
    "ExceptionRow",
    "FeeAssumptions",
    "FeeCalculator",
    "FeeReason",
    "FeeResolution",
    "FeeSchedule",
    "FeeScheduleRegistry",
    "FillFee",
    "IntermediaryFees",
    "MemberClass",
    "OrderFeeAccumulator",
    "OrderFees",
    "Role",
    "ScheduleVerification",
    "Venue",
    "buy_order_fees",
    "load_schedule",
    "model_fee",
]

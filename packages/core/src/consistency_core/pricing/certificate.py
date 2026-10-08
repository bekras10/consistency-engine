"""Proof certificate: a deterministic, JSON-serializable record of one evaluation.

Every Decimal is serialized as a fixed-point string. The certificate contains no wall-clock
telemetry, so identical inputs always produce a byte-identical certificate and hash.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from consistency_core.fees.calculator import FeeAssumptions, FeeResolution
from consistency_core.fees.model import Role
from consistency_core.fees.rounding import OrderFees
from consistency_core.models.common import FrozenModel, MarketStatus, Side, SyncStatus
from consistency_core.models.detection import Classification
from consistency_core.models.relationship import (
    RelationshipType,
    ScenarioProvenance,
    ScenarioSpec,
    VerificationStatus,
)
from consistency_core.money import Dec, dec
from consistency_core.pricing.depth import DepthWalkResult
from consistency_core.pricing.payoff import PayoffAnalysis
from consistency_core.pricing.portfolio import Leg, Template

CERTIFICATE_VERSION = "proof-certificate/1"
ENGINE_VERSION = "consistency-engine/0.1.0"

DISCLAIMER = (
    "Independently observed order books do not establish an atomic multi-market execution "
    "opportunity. Displayed depth may be gone before any order arrives; no order was placed."
)


class EvaluationConfig(FrozenModel):
    """Execution thresholds (spec 6.6). Defaults are deliberately cautious assumptions."""

    config_version: str = "evaluation-config/1"
    max_book_age_ms: int = 2000
    max_cross_market_skew_ms: int = 500
    minimum_net_edge: Dec = dec("0.01")
    """Required execution-adjusted profit per basket unit (dollars)."""
    minimum_available_quantity: Dec = dec("10")
    """Smallest basket size worth reporting; the quantity domain starts here."""
    assumed_extra_slippage_per_leg: Dec = dec("0.005")
    """Dollars per contract per leg subtracted in the execution-adjusted estimate."""
    minimum_candidate_duration_ms: int = 1000
    target_quantity: Dec | None = None
    """If set, evaluate exactly this basket size instead of optimizing."""
    fee_buffer_per_leg: Dec | None = None
    """Dollars per leg; None = one balance-precision increment of the leg's member class."""
    execution_role: Role = Role.TAKER
    exhaustive_search_limit: int = 2000
    fee_assumptions: FeeAssumptions = Field(default_factory=FeeAssumptions)


class RelationshipRef(FrozenModel):
    relationship_id: str
    relationship_type: RelationshipType
    members: tuple[str, ...]
    verification_status: VerificationStatus
    fingerprint: str
    rules_hashes: dict[str, str]
    exhaustive: bool
    scenario_spec: ScenarioSpec
    scenario_provenance: ScenarioProvenance
    constraints: tuple[str, ...]
    invalidation_reason: str | None


class PortfolioRef(FrozenModel):
    strategy_id: str
    template: Template
    legs: tuple[Leg, ...]


class BookInput(FrozenModel):
    market_id: str
    side_bought: Side
    market_status: MarketStatus | None
    sync_status: SyncStatus | None
    source: str | None
    source_sequence: int | None
    subscription_id: str | None
    connection_id: str | None
    exchange_ts_ms: int | None
    received_ts_ms: int | None
    confirmed_through_ms: int | None
    observed_ts_ms: int | None
    age_ms: int | None
    book_hash: str | None
    asks: tuple[tuple[Dec, Dec], ...]
    """The executable ask curve for the side bought: (price, displayed quantity), cheapest first."""


class TimingInfo(FrozenModel):
    now_ms: int
    max_book_age_ms: int | None
    cross_market_skew_ms: int | None
    observed_duration_ms: int | None


class TopOfBook(FrozenModel):
    best_asks: dict[str, Dec]
    premium_per_unit: Dec
    min_payoff_per_unit: Dec
    pre_fee_edge_per_unit: Dec


class LegExecution(FrozenModel):
    market_id: str
    side: Side
    ratio: Dec
    quantity: Dec
    walk: DepthWalkResult
    fees: OrderFees | None
    leg_cost: Dec | None
    """Premium plus net fees (cash out) for this leg's order."""


class QuantityEvaluation(FrozenModel):
    quantity: Dec
    fully_filled: bool
    legs: tuple[LegExecution, ...]
    min_payoff: Dec
    total_premium: Dec
    gross_profit: Dec
    """Worst-case payoff minus premium (before fees)."""
    total_net_fees: Dec | None
    total_cost: Dec | None
    net_profit: Dec | None
    """Worst-case theoretical profit after fees."""
    slippage_buffer: Dec
    fee_buffer: Dec | None
    execution_adjusted_profit: Dec | None
    per_unit_gross: Dec | None
    per_unit_net: Dec | None
    per_unit_execution_adjusted: Dec | None
    """Per-unit figures are informational (floored to 1e-8); gates compare totals exactly."""


class QuantitySearch(FrozenModel):
    quantity_step: Dec
    domain_lower: Dec
    max_supported_quantity: Dec
    """Largest basket on the quantity grid fully covered by displayed depth on every leg."""
    method: Literal["exhaustive", "breakpoints", "target", "none"]
    points_evaluated: int
    best_gross_quantity: Dec | None
    best_net_quantity: Dec | None
    best_execution_quantity: Dec | None
    """Max execution-adjusted profit among quantities meeting the minimum-edge gate (over all
    evaluated quantities when none does); ties resolve to the smaller quantity."""
    max_profitable_quantity: Dec | None
    """Largest evaluated quantity with net profit > 0 (exact when method is exhaustive)."""
    edge_qualifying_points: int | None = None
    """Evaluated quantities whose execution-adjusted profit >= minimum_net_edge x quantity."""


class TraceStep(FrozenModel):
    step: int
    check: str
    outcome: Literal["pass", "fail", "not_reached"]
    detail: str


class ProofCertificate(FrozenModel):
    certificate_version: str = CERTIFICATE_VERSION
    engine_version: str = ENGINE_VERSION
    relationship: RelationshipRef
    portfolio: PortfolioRef
    config: EvaluationConfig
    books: tuple[BookInput, ...]
    timing: TimingInfo
    payoff: PayoffAnalysis | None
    top_of_book: TopOfBook | None
    capacity: QuantitySearch | None
    evaluation: QuantityEvaluation | None
    fees: tuple[FeeResolution, ...] | None
    fee_schedule_ids: tuple[str, ...]
    fictional_fees: bool
    classification: Classification
    reason_codes: tuple[str, ...]
    trace: tuple[TraceStep, ...]
    notes: tuple[str, ...] = (DISCLAIMER,)

"""Detections and the classification vocabulary."""

from __future__ import annotations

from enum import StrEnum

from pydantic import AwareDatetime, JsonValue

from consistency_core.models.common import FrozenModel
from consistency_core.money import Dec


class Classification(StrEnum):
    """Exactly one primary status per evaluation. Declaration order == decision precedence."""

    INVALID_RELATIONSHIP = "INVALID_RELATIONSHIP"
    UNSYNCHRONIZED_DATA = "UNSYNCHRONIZED_DATA"
    STALE_DATA = "STALE_DATA"
    INSUFFICIENT_LIQUIDITY = "INSUFFICIENT_LIQUIDITY"
    NO_OPPORTUNITY = "NO_OPPORTUNITY"
    FEE_UNVERIFIED = "FEE_UNVERIFIED"
    THEORETICAL_ONLY = "THEORETICAL_ONLY"
    DEPTH_SUPPORTED = "DEPTH_SUPPORTED"
    FEE_ADJUSTED_CANDIDATE = "FEE_ADJUSTED_CANDIDATE"


class SnapshotRef(FrozenModel):
    market_id: str
    source: str
    source_sequence: int | None
    subscription_id: str | None
    received_ts_ms: int
    exchange_ts_ms: int | None
    book_hash: str


class Detection(FrozenModel):
    """Persistable result of one strategy evaluation (pipeline/persistence: milestone 2)."""

    detection_id: str
    relationship_id: str
    strategy_id: str
    detected_at: AwareDatetime
    snapshot_refs: tuple[SnapshotRef, ...]
    theoretical_deviation: Dec | None
    gross_portfolio_edge: Dec | None
    total_estimated_fees: Dec | None
    net_theoretical_edge: Dec | None
    max_depth_supported_quantity: Dec | None
    worst_case_payoff: Dec | None
    timestamp_skew_ms: int | None
    data_freshness_ms: int | None
    classification: Classification
    reason_codes: tuple[str, ...]
    proof_certificate: dict[str, JsonValue]
    simulation_metadata: dict[str, JsonValue] | None = None

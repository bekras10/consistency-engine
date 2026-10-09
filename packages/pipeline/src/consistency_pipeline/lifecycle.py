"""Detection lifecycle vocabulary and records (spec 7.2).

A *detection* is one continuous run of signals for the same (relationship, strategy). The
strategy id encodes the portfolio direction (e.g. both equivalence directions are distinct
strategies), so "same relationship and portfolio direction" is the strategy key.

Statuses: ``OPEN`` (first signal), ``UPDATED`` (classification / reason codes changed, or a
tracked maximum grew), and the terminal ``RESOLVED`` (the inconsistency disappeared on trusted,
fresh data), ``EXPIRED`` (it can no longer be assessed: stale data, market not open, no asks,
end of session) and ``INVALIDATED`` (its inputs became untrusted or its proof basis lapsed:
unsynchronized/interrupted books, relationship invalidated or changed, market removed, restart).

A *signal* is an evaluation that passed the top-of-book worst-case edge gate (step 5): the
apparent inconsistency survives bid/ask prices on synchronized, fresh books of a verified
relationship. Every later classification (INSUFFICIENT_LIQUIDITY at steps 6-7, FEE_UNVERIFIED,
THEORETICAL_ONLY, DEPTH_SUPPORTED, FEE_ADJUSTED_CANDIDATE) is a signal; earlier failures are not.
"""

from __future__ import annotations

import hashlib
from enum import StrEnum
from typing import Literal

from consistency_core.models.common import FrozenModel
from consistency_core.models.detection import Classification
from consistency_core.money import Dec
from consistency_core.pricing.evaluator import Evaluation, Reason

SIGNAL_GATE = "top_of_book_pre_fee_edge"
DURATION_REASONS = frozenset({Reason.DURATION_UNKNOWN.value, Reason.DURATION_BELOW_MINIMUM.value})


class DetectionStatus(StrEnum):
    OPEN = "OPEN"
    UPDATED = "UPDATED"
    EXPIRED = "EXPIRED"
    RESOLVED = "RESOLVED"
    INVALIDATED = "INVALIDATED"

    @property
    def active(self) -> bool:
        return self in (DetectionStatus.OPEN, DetectionStatus.UPDATED)


class EventKind(StrEnum):
    OPENED = "OPENED"
    UPDATED = "UPDATED"
    EXPIRED = "EXPIRED"
    RESOLVED = "RESOLVED"
    INVALIDATED = "INVALIDATED"


class CloseReason(StrEnum):
    NO_PRE_FEE_EDGE = "NO_PRE_FEE_EDGE"
    STALE_DATA = "STALE_DATA"
    MARKET_NOT_OPEN = "MARKET_NOT_OPEN"
    NO_ASKS = "NO_ASKS"
    SESSION_ENDED = "SESSION_ENDED"
    BOOK_UNSYNCHRONIZED = "BOOK_UNSYNCHRONIZED"
    BOOK_INTERRUPTED = "BOOK_INTERRUPTED"
    RELATIONSHIP_INVALIDATED = "RELATIONSHIP_INVALIDATED"
    RELATIONSHIP_CHANGED = "RELATIONSHIP_CHANGED"
    MARKET_REMOVED = "MARKET_REMOVED"
    SERVICE_RESTART = "SERVICE_RESTART"


CLOSE_STATUS: dict[CloseReason, DetectionStatus] = {
    CloseReason.NO_PRE_FEE_EDGE: DetectionStatus.RESOLVED,
    CloseReason.STALE_DATA: DetectionStatus.EXPIRED,
    CloseReason.MARKET_NOT_OPEN: DetectionStatus.EXPIRED,
    CloseReason.NO_ASKS: DetectionStatus.EXPIRED,
    CloseReason.SESSION_ENDED: DetectionStatus.EXPIRED,
    CloseReason.BOOK_UNSYNCHRONIZED: DetectionStatus.INVALIDATED,
    CloseReason.BOOK_INTERRUPTED: DetectionStatus.INVALIDATED,
    CloseReason.RELATIONSHIP_INVALIDATED: DetectionStatus.INVALIDATED,
    CloseReason.RELATIONSHIP_CHANGED: DetectionStatus.INVALIDATED,
    CloseReason.MARKET_REMOVED: DetectionStatus.INVALIDATED,
    CloseReason.SERVICE_RESTART: DetectionStatus.INVALIDATED,
}

EVENT_FOR_STATUS: dict[DetectionStatus, EventKind] = {
    DetectionStatus.OPEN: EventKind.OPENED,
    DetectionStatus.UPDATED: EventKind.UPDATED,
    DetectionStatus.EXPIRED: EventKind.EXPIRED,
    DetectionStatus.RESOLVED: EventKind.RESOLVED,
    DetectionStatus.INVALIDATED: EventKind.INVALIDATED,
}


def is_signal(ev: Evaluation) -> bool:
    return SIGNAL_GATE in ev.certificate.validation.gates_passed


def passes_all_but_duration(ev: Evaluation) -> bool:
    """The duration-gate condition (mathematical-model §6.3, golden I): every gate passed
    except, possibly, the minimum-duration gate."""
    if ev.classification is Classification.FEE_ADJUSTED_CANDIDATE:
        return True
    return (
        ev.classification is Classification.DEPTH_SUPPORTED
        and bool(ev.reason_codes)
        and set(ev.reason_codes) <= DURATION_REASONS
    )


def close_reason_for(ev: Evaluation) -> CloseReason:
    """Map a non-signal evaluation to the reason an active detection ends."""
    reasons = set(ev.reason_codes)
    match ev.classification:
        case Classification.UNSYNCHRONIZED_DATA:
            return CloseReason.BOOK_UNSYNCHRONIZED
        case Classification.STALE_DATA:
            return CloseReason.STALE_DATA
        case Classification.INVALID_RELATIONSHIP:
            if Reason.MARKET_UNKNOWN.value in reasons:
                return CloseReason.MARKET_REMOVED
            return CloseReason.RELATIONSHIP_INVALIDATED
        case Classification.INSUFFICIENT_LIQUIDITY:
            if Reason.MARKET_NOT_OPEN.value in reasons:
                return CloseReason.MARKET_NOT_OPEN
            return CloseReason.NO_ASKS
        case _:
            return CloseReason.NO_PRE_FEE_EDGE


class TimingMeta(FrozenModel):
    """Spec 4.5 timing for the evaluation behind an event.

    ``exchange_ts_ms`` / ``received_ts_ms`` / ``source_sequence`` / ids come from the stream and
    are deterministic for a recording. ``processing_started_ns`` / ``detection_completed_ns``
    are local ``perf_counter_ns`` telemetry: they differ between runs and are excluded from
    every determinism comparison. Source latency (``received - exchange``) is network/exchange
    latency; internal latency (``completed - started``) is attributable to this application.
    """

    trigger: Literal[
        "book_update",
        "sweep",
        "probe",
        "relationship_change",
        "session_end",
        "restart",
    ]
    market_id: str | None = None
    connection_id: str | None = None
    subscription_id: str | None = None
    source_sequence: int | None = None
    exchange_ts_ms: int | None = None
    received_ts_ms: int | None = None
    processing_started_ns: int | None = None
    detection_completed_ns: int | None = None

    @property
    def source_latency_ms(self) -> int | None:
        if self.exchange_ts_ms is None or self.received_ts_ms is None:
            return None
        return self.received_ts_ms - self.exchange_ts_ms

    @property
    def internal_latency_ns(self) -> int | None:
        if self.processing_started_ns is None or self.detection_completed_ns is None:
            return None
        return self.detection_completed_ns - self.processing_started_ns

    def deterministic(self) -> dict[str, object]:
        return self.model_dump(
            mode="json", exclude={"processing_started_ns", "detection_completed_ns"}
        )


class DetectionMetrics(FrozenModel):
    """Figures of the evaluation the detection currently reports (all exact Decimals)."""

    theoretical_deviation: Dec | None = None
    """Top-of-book worst-case edge per basket unit (pre-fee)."""
    gross_edge: Dec | None = None
    total_fees: Dec | None = None
    net_edge: Dec | None = None
    execution_adjusted_edge: Dec | None = None
    reported_quantity: Dec | None = None
    depth_supported_quantity: Dec | None = None
    worst_case_payoff: Dec | None = None
    timestamp_skew_ms: int | None = None
    data_freshness_ms: int | None = None
    observed_duration_ms: int | None = None

    @classmethod
    def of(cls, ev: Evaluation) -> DetectionMetrics:
        c = ev.certificate
        q = c.evaluation
        return cls(
            theoretical_deviation=None
            if c.top_of_book is None
            else c.top_of_book.pre_fee_edge_per_unit,
            gross_edge=None if q is None else q.gross_profit,
            total_fees=None if q is None else q.total_net_fees,
            net_edge=None if q is None else q.net_profit,
            execution_adjusted_edge=None if q is None else q.execution_adjusted_profit,
            reported_quantity=None if q is None else q.quantity,
            depth_supported_quantity=None
            if c.capacity is None
            else c.capacity.max_supported_quantity,
            worst_case_payoff=None if q is None else q.min_payoff,
            timestamp_skew_ms=c.timing.cross_market_skew_ms,
            data_freshness_ms=c.timing.max_book_age_ms,
            observed_duration_ms=c.timing.observed_duration_ms,
        )


class DetectionRecord(FrozenModel):
    """Current state of one detection (what the ``detections`` row holds)."""

    detection_id: str
    session_id: str
    relationship_id: str
    strategy_id: str
    template: str
    ordinal: int
    """1-based count of detections opened for this strategy in this session."""
    status: DetectionStatus
    classification: Classification
    reason_codes: tuple[str, ...]
    peak_classification: Classification
    """Furthest classification reached (later in precedence order = more gates passed)."""
    first_observed_ms: int
    last_observed_ms: int
    first_position: int
    last_position: int
    closed_at_ms: int | None = None
    close_reason: CloseReason | None = None
    close_detail: str | None = None
    max_deviation: Dec | None = None
    max_capacity: Dec | None = None
    max_net_edge: Dec | None = None
    max_candidate_duration_ms: int | None = None
    """Longest continuous run of 'every gate except duration passed' within this detection."""
    update_count: int = 0
    event_count: int = 1
    certificate_hash: str
    """Certificate of the evaluation behind the latest OPENED / UPDATED event."""
    metrics: DetectionMetrics
    timing: TimingMeta

    @property
    def signal_duration_ms(self) -> int:
        return self.last_observed_ms - self.first_observed_ms


class DetectionEvent(FrozenModel):
    """One lifecycle transition. ``evaluation`` is the evaluation that caused it (None for
    forced closures: interrupted books, relationship changes, session end, restart)."""

    detection_id: str
    seq: int
    kind: EventKind
    at_ms: int
    position: int
    classification: Classification | None
    reason_codes: tuple[str, ...]
    close_reason: CloseReason | None = None
    close_detail: str | None = None
    certificate_hash: str | None
    record: DetectionRecord
    evaluation: Evaluation | None = None
    timing: TimingMeta

    def comparable(self) -> dict[str, object]:
        """Deterministic identity of the event (wall-clock telemetry excluded)."""
        return {
            "detection_id": self.detection_id,
            "seq": self.seq,
            "kind": self.kind.value,
            "at_ms": self.at_ms,
            "position": self.position,
            "classification": None if self.classification is None else self.classification.value,
            "reason_codes": list(self.reason_codes),
            "close_reason": None if self.close_reason is None else self.close_reason.value,
            "certificate_hash": self.certificate_hash,
            "relationship_id": self.record.relationship_id,
            "strategy_id": self.record.strategy_id,
        }


class StrategyState(FrozenModel):
    """Latest evaluation of one strategy (the relationship explorer's live constraint state)."""

    key: str
    relationship_id: str
    strategy_id: str
    classification: Classification
    reason_codes: tuple[str, ...]
    signal: bool
    evaluated_at_ms: int
    position: int
    certificate_hash: str
    streak_since_ms: int | None
    active_detection_id: str | None


def strategy_key(relationship_id: str, strategy_id: str) -> str:
    return f"{relationship_id}::{strategy_id}"


def detection_id(
    session_id: str, relationship_id: str, strategy_id: str, first_position: int, ordinal: int
) -> str:
    raw = f"{session_id}|{relationship_id}|{strategy_id}|{first_position}|{ordinal}"
    return "det-" + hashlib.sha256(raw.encode()).hexdigest()[:24]

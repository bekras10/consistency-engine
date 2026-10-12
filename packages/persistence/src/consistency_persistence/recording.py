"""Journal batches, detection transactions, and checkpoints.

Book rows are inserted in bounded batches (one transaction per flush). A detection batch
writes the detection, its legs, its scenarios, the certificate v2 JSON, and one
notification-outbox row in one transaction.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, insert, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from consistency_core.events import OrderBookSnapshotEvent
from consistency_core.pricing.certificate import ProofCertificate
from consistency_core.serialization import canonical_json
from consistency_persistence.dashboard import json_ready
from consistency_persistence.outbox import reserve_outbox_id
from consistency_persistence.quote_retention import redact_detection_event
from consistency_persistence.schema import (
    DetectionLegRow,
    DetectionRow,
    DetectionScenarioRow,
    NotificationOutboxRow,
    OrderbookSnapshotRow,
    OrderbookUpdateRow,
    SessionCheckpointRow,
)
from consistency_persistence.timeutil import ms_to_dt, now_utc
from consistency_pipeline.checkpoint import Checkpoint
from consistency_pipeline.journal import JournalEntry
from consistency_pipeline.lifecycle import DetectionEvent, DetectionRecord
from consistency_pipeline.serialize import DETECTION_TOPIC, detection_event_payload


class VersionStamp:
    def __init__(
        self,
        *,
        fee_schedule_version: str,
        relationship_version: str,
        rules_hashes: dict[str, str],
    ) -> None:
        self.fee_schedule_version = fee_schedule_version
        self.relationship_version = relationship_version
        self.rules_hashes = rules_hashes


def _market_id(entry: JournalEntry) -> str | None:
    if entry.message is None:
        return None
    value = getattr(entry.message.event, "market_id", None)
    return value if isinstance(value, str) else None


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _dec(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, str):
        return Decimal(value)
    raise TypeError("money values must be Decimal or fixed-point strings")


class BatchJournal:
    """``JournalSink``: buffers entries and flushes them in one transaction."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        session_id: str,
        versions: VersionStamp,
        *,
        batch_size: int,
        enabled: bool,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self._sessions = sessions
        self._session_id = session_id
        self._versions = versions
        self._batch_size = batch_size
        self._enabled = enabled
        self._buf: list[JournalEntry] = []
        self._flush_lock = asyncio.Lock()

    def append(self, entry: JournalEntry) -> None:
        if self._enabled:
            self._buf.append(entry)

    async def wait_capacity(self) -> None:
        if len(self._buf) >= self._batch_size:
            await self.flush()

    async def flush(self) -> None:
        # One flush at a time, including the buffer read, the insert transaction, and the
        # prefix delete. Overlapping flushes would insert the same ordinals twice. Appends
        # are synchronous and only land at the await inside the transaction; they stay
        # behind the prefix that was sent. The buffer is dropped only after commit.
        async with self._flush_lock:
            if not self._buf:
                return
            batch = list(self._buf)
            async with self._sessions() as session, session.begin():
                await _insert_journal(session, self._session_id, self._versions, batch)
            if self._buf[: len(batch)] != batch:
                raise RuntimeError("journal buffer changed during flush")
            del self._buf[: len(batch)]


async def _insert_journal(
    session: AsyncSession,
    session_id: str,
    versions: VersionStamp,
    entries: Sequence[JournalEntry],
) -> None:
    updates: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []
    for entry in entries:
        payload = entry.model_dump(mode="json")
        message = entry.message
        event = None if message is None else message.event
        kind = entry.kind
        market_id = _market_id(entry)
        received_ms = entry.now_ms
        exchange_ms = _optional_int(getattr(event, "exchange_ts_ms", None)) if event else None
        row: dict[str, Any] = {
            "session_id": session_id,
            "ordinal": entry.ordinal,
            "kind": kind,
            "market_id": market_id,
            "connection_id": _optional_str(getattr(event, "connection_id", None))
            if event
            else None,
            "subscription_id": _optional_str(getattr(event, "sid", None)) if event else None,
            "source_sequence": _optional_int(getattr(event, "seq", None)) if event else None,
            "exchange_at": None if exchange_ms is None else ms_to_dt(exchange_ms),
            "received_at": ms_to_dt(received_ms),
            "received_ts_ms": received_ms,
            "price": _dec(getattr(event, "price", None)) if event else None,
            "quantity_delta": _dec(getattr(event, "delta", None)) if event else None,
            "market_rules_hash": None
            if market_id is None
            else versions.rules_hashes.get(market_id),
            "fee_schedule_version": versions.fee_schedule_version,
            "relationship_version": versions.relationship_version,
            "entry_json": payload,
        }
        updates.append(row)
        if isinstance(event, OrderBookSnapshotEvent):
            dumped = event.model_dump(mode="json")
            snapshots.append(
                {
                    "session_id": session_id,
                    "ordinal": entry.ordinal,
                    "market_id": event.market_id,
                    "connection_id": event.connection_id,
                    "subscription_id": event.sid,
                    "source_sequence": event.seq,
                    "exchange_at": None
                    if event.exchange_ts_ms is None
                    else ms_to_dt(event.exchange_ts_ms),
                    "received_at": ms_to_dt(received_ms),
                    "received_ts_ms": received_ms,
                    "market_rules_hash": versions.rules_hashes.get(event.market_id),
                    "fee_schedule_version": versions.fee_schedule_version,
                    "relationship_version": versions.relationship_version,
                    "levels": {"yes_bids": dumped["yes_bids"], "no_bids": dumped["no_bids"]},
                }
            )
    if updates:
        await session.execute(insert(OrderbookUpdateRow), updates)
    if snapshots:
        await session.execute(insert(OrderbookSnapshotRow), snapshots)


async def load_journal(session: AsyncSession, session_id: str) -> list[JournalEntry]:
    rows = (
        await session.execute(
            select(OrderbookUpdateRow.entry_json)
            .where(OrderbookUpdateRow.session_id == session_id)
            .order_by(OrderbookUpdateRow.ordinal)
        )
    ).scalars()
    return [JournalEntry.model_validate(payload) for payload in rows]


class DetectionWriter:
    """``DetectionSink``: one transaction per batch (detection + legs + scenarios + certificate)."""

    def __init__(
        self, sessions: async_sessionmaker[AsyncSession], *, retain_quote_levels: bool = True
    ) -> None:
        self._sessions = sessions
        self.retain_quote_levels = retain_quote_levels
        self.fault: Exception | None = None

    async def write(self, events: Sequence[DetectionEvent]) -> None:
        if not events:
            return
        prepared = (
            events if self.retain_quote_levels else [redact_detection_event(ev) for ev in events]
        )
        async with self._sessions() as session, session.begin():
            for ev in prepared:
                await _apply_event(session, ev)
            if self.fault is not None:
                raise self.fault


def _require_proof_hash(rec: DetectionRecord) -> None:
    """Refuse a write whose stored detection disagrees with its qualifying proof.

    The hash and the certificate bytes must be the same snapshot, and the
    identity columns copied from that proof (classification, relationship,
    strategy, template, reason codes, certificate version, and the quoted
    metrics) must match it. A new qualifying detection has no row until that
    check passes. An update that fails raises before any column changes, so
    the transaction rolls the whole batch back.
    """
    if rec.certificate_json is None:
        raise ValueError(f"{rec.detection_id} qualifying certificate JSON is missing")
    digest = "sha256:" + hashlib.sha256(rec.certificate_json.encode()).hexdigest()
    if digest != rec.certificate_hash:
        raise ValueError(f"{rec.detection_id} certificate hash does not match certificate JSON")
    closing_json = rec.closing_certificate_json
    closing_hash = rec.closing_certificate_hash
    if (closing_json is None) ^ (closing_hash is None):
        raise ValueError(f"{rec.detection_id} closing certificate is missing half of its pair")
    if closing_json is not None and closing_hash is not None:
        closing = "sha256:" + hashlib.sha256(closing_json.encode()).hexdigest()
        if closing != closing_hash:
            raise ValueError(f"{rec.detection_id} closing certificate hash does not match its JSON")
    _require_certificate_identity(rec, rec.certificate_json)


def _same_figure(stored: object, expected: object) -> bool:
    if stored is None and expected is None:
        return True
    if stored is None or expected is None:
        return False
    if isinstance(stored, int) and isinstance(expected, int):
        return stored == expected
    try:
        return Decimal(str(stored)) == Decimal(str(expected))
    except (ArithmeticError, ValueError):
        return False


def _require_certificate_identity(rec: DetectionRecord, document: str) -> None:
    """The detection row quotes these fields from the qualifying certificate."""
    try:
        cert = ProofCertificate.model_validate_json(document)
    except ValueError as exc:
        raise ValueError(f"{rec.detection_id} qualifying certificate is not a proof") from exc
    if cert.classification != rec.classification:
        raise ValueError(
            f"{rec.detection_id} classification {rec.classification.value} "
            f"contradicts the certificate classification {cert.classification.value}"
        )
    if cert.relationship.relationship_id != rec.relationship_id:
        raise ValueError(
            f"{rec.detection_id} relationship {rec.relationship_id} "
            f"contradicts the certificate relationship {cert.relationship.relationship_id}"
        )
    if cert.portfolio.strategy_id != rec.strategy_id:
        raise ValueError(
            f"{rec.detection_id} strategy {rec.strategy_id} "
            f"contradicts the certificate strategy {cert.portfolio.strategy_id}"
        )
    if cert.portfolio.template.value != rec.template:
        raise ValueError(
            f"{rec.detection_id} template {rec.template} "
            f"contradicts the certificate template {cert.portfolio.template.value}"
        )
    if tuple(cert.reason_codes) != tuple(rec.reason_codes):
        raise ValueError(f"{rec.detection_id} reason codes contradict the certificate")
    if rec.certificate_version is not None and rec.certificate_version != cert.certificate_version:
        raise ValueError(f"{rec.detection_id} certificate version contradicts the certificate")
    evaluation = cert.evaluation
    top = cert.top_of_book
    capacity = cert.capacity
    quoted: dict[str, object] = {
        "theoretical_deviation": None if top is None else top.pre_fee_edge_per_unit,
        "gross_edge": None if evaluation is None else evaluation.gross_profit,
        "total_fees": None if evaluation is None else evaluation.total_net_fees,
        "net_edge": None if evaluation is None else evaluation.net_profit,
        "execution_adjusted_edge": None
        if evaluation is None
        else evaluation.execution_adjusted_profit,
        "reported_quantity": None if evaluation is None else evaluation.quantity,
        "depth_supported_quantity": None if capacity is None else capacity.max_supported_quantity,
        "worst_case_payoff": None if evaluation is None else evaluation.min_payoff,
        "timestamp_skew_ms": cert.timing.cross_market_skew_ms,
        "data_freshness_ms": cert.timing.max_book_age_ms,
        "observed_duration_ms": cert.timing.observed_duration_ms,
    }
    for key, expected in quoted.items():
        actual = getattr(rec.metrics, key)
        if not _same_figure(actual, expected):
            raise ValueError(f"{rec.detection_id} {key} contradicts the certificate")


async def _apply_event(session: AsyncSession, ev: DetectionEvent) -> None:
    rec = ev.record
    _require_proof_hash(rec)
    existing = await session.get(DetectionRow, rec.detection_id)
    values = _detection_values(rec)
    proof = rec.certificate_json
    if existing is None:
        session.add(DetectionRow(**values))
        await session.flush()
        if proof is not None:
            await replace_certificate_projection(session, rec.detection_id, proof)
    else:
        previous = existing.certificate_json
        for key, value in values.items():
            if key == "certificate_version" and value is None:
                continue
            setattr(existing, key, value)
        if proof is not None and previous != proof:
            await replace_certificate_projection(session, rec.detection_id, proof)
    payload = json_ready(detection_event_payload(ev))
    if not isinstance(payload, dict):
        raise TypeError("outbox payload must be an object")
    session.add(
        NotificationOutboxRow(
            id=await reserve_outbox_id(session),
            topic=DETECTION_TOPIC,
            session_id=rec.session_id,
            payload=payload,
            created_at=now_utc(),
        )
    )


async def replace_certificate_projection(
    session: AsyncSession, detection_id: str, certificate_json: str
) -> None:
    """Replace legs and scenarios so they match ``certificate_json`` exactly."""
    cert = ProofCertificate.model_validate_json(certificate_json)
    await session.execute(
        delete(DetectionLegRow).where(DetectionLegRow.detection_id == detection_id)
    )
    await session.execute(
        delete(DetectionScenarioRow).where(DetectionScenarioRow.detection_id == detection_id)
    )
    executed = list(cert.evaluation.legs) if cert.evaluation is not None else []
    for index, leg in enumerate(cert.portfolio.legs):
        walked = executed[index] if index < len(executed) else None
        session.add(
            DetectionLegRow(
                detection_id=detection_id,
                leg_index=index,
                market_id=leg.market_id,
                side=leg.side.value,
                ratio=leg.ratio,
                quantity=None if walked is None else walked.quantity,
                premium=None if walked is None else walked.walk.total_premium,
                leg_cost=None if walked is None else walked.leg_cost,
            )
        )
    payoff = cert.payoff
    if payoff is not None and payoff.states:
        for index, state in enumerate(payoff.states):
            session.add(
                DetectionScenarioRow(
                    detection_id=detection_id,
                    scenario_index=index,
                    state_json=canonical_json(state.state),
                    payoff_per_unit=state.payoff_per_unit,
                    is_worst=state.state == payoff.worst_state,
                )
            )
    elif payoff is not None:
        session.add(
            DetectionScenarioRow(
                detection_id=detection_id,
                scenario_index=0,
                state_json=canonical_json(payoff.worst_state),
                payoff_per_unit=payoff.min_payoff_per_unit,
                is_worst=True,
            )
        )


def _detection_values(rec: DetectionRecord) -> dict[str, Any]:
    """Persist the record's qualifying proof. The closing evaluation is a separate column."""
    certificate_json = rec.certificate_json
    version = rec.certificate_version
    closing = None if rec.closing_classification is None else rec.closing_classification.value
    return {
        "detection_id": rec.detection_id,
        "session_id": rec.session_id,
        "relationship_id": rec.relationship_id,
        "strategy_id": rec.strategy_id,
        "template": rec.template,
        "ordinal": rec.ordinal,
        "status": rec.status.value,
        "classification": rec.classification.value,
        "peak_classification": rec.peak_classification.value,
        "reason_codes": list(rec.reason_codes),
        "first_observed_at": ms_to_dt(rec.first_observed_ms),
        "last_observed_at": ms_to_dt(rec.last_observed_ms),
        "first_observed_ms": rec.first_observed_ms,
        "last_observed_ms": rec.last_observed_ms,
        "first_position": rec.first_position,
        "last_position": rec.last_position,
        "closed_at": None if rec.closed_at_ms is None else ms_to_dt(rec.closed_at_ms),
        "close_reason": None if rec.close_reason is None else rec.close_reason.value,
        "close_detail": rec.close_detail,
        "max_deviation": rec.max_deviation,
        "max_capacity": rec.max_capacity,
        "max_net_edge": rec.max_net_edge,
        "event_count": rec.event_count,
        "certificate_hash": rec.certificate_hash,
        "certificate_json": certificate_json,
        "certificate_version": version,
        "closing_classification": closing,
        "closing_certificate_hash": rec.closing_certificate_hash,
        "closing_certificate_json": rec.closing_certificate_json,
        "proof_status": "certified",
        "record_json": rec.model_dump(mode="json"),
    }


class CheckpointWriter:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def save(self, checkpoint: Checkpoint) -> None:
        payload = checkpoint.model_dump(mode="json")
        async with self._sessions() as session, session.begin():
            session.add(
                SessionCheckpointRow(
                    session_id=checkpoint.session_id,
                    ordinal=checkpoint.ordinal,
                    now_ms=checkpoint.now_ms,
                    source_position=checkpoint.source_position,
                    state_digest=checkpoint.state_digest,
                    checkpoint_json=payload,
                    created_at=now_utc(),
                )
            )


async def latest_checkpoint(session: AsyncSession, session_id: str) -> Checkpoint | None:
    row = (
        await session.execute(
            select(SessionCheckpointRow)
            .where(SessionCheckpointRow.session_id == session_id)
            .order_by(SessionCheckpointRow.ordinal.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    return Checkpoint.model_validate(row.checkpoint_json)


async def checkpoints_at_or_before(
    session: AsyncSession, session_id: str, ordinal: int
) -> list[Checkpoint]:
    rows = (
        await session.execute(
            select(SessionCheckpointRow.checkpoint_json)
            .where(
                SessionCheckpointRow.session_id == session_id,
                SessionCheckpointRow.ordinal <= ordinal,
            )
            .order_by(SessionCheckpointRow.ordinal)
        )
    ).scalars()
    return [Checkpoint.model_validate(payload) for payload in rows]

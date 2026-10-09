"""Relational schema for spec section 11.

Money columns are ``NUMERIC`` (never float). Timestamps are ``timestamptz`` (UTC).
The proof certificate is stored as canonical JSON text plus its hash so the bytes that
were hashed survive a round trip. ``session_checkpoints`` is the extra table spec 12.2
needs in order to restore a pipeline position; every spec 11 table is present as well.

High-frequency book rows live in ``orderbook_snapshots`` and ``orderbook_updates``.
The update table is the ordered event log (snapshots, deltas, heartbeats, lifecycle,
control): replay order is ``(session_id, ordinal)``.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

SPEC_TABLES: tuple[str, ...] = (
    "data_sources",
    "series",
    "events",
    "markets",
    "market_rules",
    "market_price_ranges",
    "relationships",
    "relationship_members",
    "relationship_reviews",
    "fee_schedules",
    "ingestion_sessions",
    "orderbook_snapshots",
    "orderbook_updates",
    "detections",
    "detection_legs",
    "detection_scenarios",
    "replay_sessions",
    "system_health",
    "configuration_versions",
    "notification_outbox",
)
CHECKPOINT_TABLE = "session_checkpoints"


class Base(DeclarativeBase):
    pass


class DataSourceRow(Base):
    __tablename__ = "data_sources"

    data_source_id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SeriesRow(Base):
    __tablename__ = "series"

    series_id: Mapped[str] = mapped_column(Text, primary_key=True)
    data_source_id: Mapped[str] = mapped_column(
        ForeignKey("data_sources.data_source_id"), nullable=False
    )
    title: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    provenance: Mapped[str] = mapped_column(Text, nullable=False)
    document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)


class EventRow(Base):
    __tablename__ = "events"
    __table_args__ = (Index("ix_events_ticker", "ticker"),)

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    ticker: Mapped[str] = mapped_column(Text, nullable=False)
    series_id: Mapped[str] = mapped_column(ForeignKey("series.series_id"), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    mutually_exclusive: Mapped[bool | None] = mapped_column(Boolean)
    outcome_set_complete: Mapped[bool | None] = mapped_column(Boolean)
    provenance: Mapped[str] = mapped_column(Text, nullable=False)
    document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)


class MarketRow(Base):
    __tablename__ = "markets"
    __table_args__ = (Index("ix_markets_ticker", "ticker"),)

    market_id: Mapped[str] = mapped_column(Text, primary_key=True)
    ticker: Mapped[str] = mapped_column(Text, nullable=False)
    event_id: Mapped[str] = mapped_column(ForeignKey("events.event_id"), nullable=False)
    series_id: Mapped[str] = mapped_column(ForeignKey("series.series_id"), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    open_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    close_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expiration_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    quantity_increment: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    rules_version: Mapped[str] = mapped_column(Text, nullable=False)
    rules_hash: Mapped[str] = mapped_column(Text, nullable=False)
    provenance: Mapped[str] = mapped_column(Text, nullable=False)
    document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)


class MarketRulesRow(Base):
    __tablename__ = "market_rules"
    __table_args__ = (UniqueConstraint("market_id", "rules_hash", name="uq_market_rules_hash"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.market_id"), nullable=False)
    rules_version: Mapped[str] = mapped_column(Text, nullable=False)
    rules_hash: Mapped[str] = mapped_column(Text, nullable=False)
    rules_text: Mapped[str] = mapped_column(Text, nullable=False)
    settlement: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class MarketPriceRangeRow(Base):
    __tablename__ = "market_price_ranges"
    __table_args__ = (UniqueConstraint("market_id", "range_index", name="uq_market_price_range"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.market_id"), nullable=False)
    range_index: Mapped[int] = mapped_column(Integer, nullable=False)
    start_price: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    end_price: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    step: Mapped[Decimal] = mapped_column(Numeric, nullable=False)


class RelationshipRow(Base):
    __tablename__ = "relationships"

    relationship_id: Mapped[str] = mapped_column(Text, primary_key=True)
    relationship_type: Mapped[str] = mapped_column(Text, nullable=False)
    verification_status: Mapped[str] = mapped_column(Text, nullable=False)
    exhaustive: Mapped[bool] = mapped_column(Boolean, nullable=False)
    fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)


class RelationshipMemberRow(Base):
    __tablename__ = "relationship_members"
    __table_args__ = (
        Index("ix_relationship_members_market", "market_id"),
        UniqueConstraint("relationship_id", "market_id", name="uq_relationship_member"),
    )

    relationship_id: Mapped[str] = mapped_column(
        ForeignKey("relationships.relationship_id", ondelete="CASCADE"), primary_key=True
    )
    position: Mapped[int] = mapped_column(Integer, primary_key=True)
    market_id: Mapped[str] = mapped_column(Text, nullable=False)


class RelationshipReviewRow(Base):
    __tablename__ = "relationship_reviews"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    relationship_id: Mapped[str] = mapped_column(
        ForeignKey("relationships.relationship_id", ondelete="CASCADE"), nullable=False
    )
    review_id: Mapped[str | None] = mapped_column(Text)
    reviewer: Mapped[str] = mapped_column(Text, nullable=False)
    decision: Mapped[str] = mapped_column(Text, nullable=False)
    reasoning: Mapped[str] = mapped_column(Text, nullable=False)
    reviewed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)


class FeeScheduleRow(Base):
    __tablename__ = "fee_schedules"

    schedule_id: Mapped[str] = mapped_column(Text, primary_key=True)
    venue: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    effective_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    taker_coefficient: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    maker_coefficient: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)


class ConfigurationVersionRow(Base):
    __tablename__ = "configuration_versions"

    version_id: Mapped[str] = mapped_column(Text, primary_key=True)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    document: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)


class IngestionSessionRow(Base):
    __tablename__ = "ingestion_sessions"
    __table_args__ = (Index("ix_ingestion_sessions_fingerprint", "fingerprint", "status"),)

    session_id: Mapped[str] = mapped_column(Text, primary_key=True)
    data_source_id: Mapped[str] = mapped_column(
        ForeignKey("data_sources.data_source_id"), nullable=False
    )
    status: Mapped[str] = mapped_column(Text, nullable=False)
    fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    source_label: Mapped[str] = mapped_column(Text, nullable=False)
    deterministic: Mapped[bool] = mapped_column(Boolean, nullable=False)
    pinned: Mapped[bool] = mapped_column(Boolean, nullable=False)
    raw_persisted: Mapped[bool] = mapped_column(Boolean, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    detail: Mapped[str | None] = mapped_column(Text)
    config_version_id: Mapped[str | None] = mapped_column(Text)


class OrderbookSnapshotRow(Base):
    __tablename__ = "orderbook_snapshots"
    __table_args__ = (
        Index("ix_orderbook_snapshots_lookup", "session_id", "market_id", "received_at"),
        Index("ix_orderbook_snapshots_order", "session_id", "ordinal", unique=True),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(
        ForeignKey("ingestion_sessions.session_id", ondelete="CASCADE"), nullable=False
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    market_id: Mapped[str] = mapped_column(Text, nullable=False)
    connection_id: Mapped[str | None] = mapped_column(Text)
    subscription_id: Mapped[str | None] = mapped_column(Text)
    source_sequence: Mapped[int | None] = mapped_column(BigInteger)
    exchange_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_ts_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    market_rules_hash: Mapped[str | None] = mapped_column(Text)
    fee_schedule_version: Mapped[str] = mapped_column(Text, nullable=False)
    relationship_version: Mapped[str] = mapped_column(Text, nullable=False)
    levels: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)


class OrderbookUpdateRow(Base):
    """Ordered event log. ``ordinal`` is the deterministic replay order (spec 12.1)."""

    __tablename__ = "orderbook_updates"
    __table_args__ = (
        Index("ix_orderbook_updates_replay_order", "session_id", "ordinal", unique=True),
        Index(
            "ix_orderbook_updates_session_sequence",
            "session_id",
            "subscription_id",
            "source_sequence",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(
        ForeignKey("ingestion_sessions.session_id", ondelete="CASCADE"), nullable=False
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    market_id: Mapped[str | None] = mapped_column(Text)
    connection_id: Mapped[str | None] = mapped_column(Text)
    subscription_id: Mapped[str | None] = mapped_column(Text)
    source_sequence: Mapped[int | None] = mapped_column(BigInteger)
    exchange_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_ts_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    price: Mapped[Decimal | None] = mapped_column(Numeric)
    quantity_delta: Mapped[Decimal | None] = mapped_column(Numeric)
    market_rules_hash: Mapped[str | None] = mapped_column(Text)
    fee_schedule_version: Mapped[str] = mapped_column(Text, nullable=False)
    relationship_version: Mapped[str] = mapped_column(Text, nullable=False)
    entry_json: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)


class DetectionRow(Base):
    __tablename__ = "detections"
    __table_args__ = (
        Index("ix_detections_classification", "classification"),
        Index("ix_detections_first_observed", "first_observed_at"),
        Index("ix_detections_last_observed", "last_observed_at"),
        Index("ix_detections_session", "session_id"),
    )

    detection_id: Mapped[str] = mapped_column(Text, primary_key=True)
    session_id: Mapped[str] = mapped_column(
        ForeignKey("ingestion_sessions.session_id", ondelete="CASCADE"), nullable=False
    )
    relationship_id: Mapped[str] = mapped_column(Text, nullable=False)
    strategy_id: Mapped[str] = mapped_column(Text, nullable=False)
    template: Mapped[str] = mapped_column(Text, nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    classification: Mapped[str] = mapped_column(Text, nullable=False)
    peak_classification: Mapped[str] = mapped_column(Text, nullable=False)
    reason_codes: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    first_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    first_observed_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_observed_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    first_position: Mapped[int] = mapped_column(Integer, nullable=False)
    last_position: Mapped[int] = mapped_column(Integer, nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    close_reason: Mapped[str | None] = mapped_column(Text)
    close_detail: Mapped[str | None] = mapped_column(Text)
    max_deviation: Mapped[Decimal | None] = mapped_column(Numeric)
    max_capacity: Mapped[Decimal | None] = mapped_column(Numeric)
    max_net_edge: Mapped[Decimal | None] = mapped_column(Numeric)
    event_count: Mapped[int] = mapped_column(Integer, nullable=False)
    certificate_hash: Mapped[str] = mapped_column(Text, nullable=False)
    certificate_json: Mapped[str | None] = mapped_column(Text)
    certificate_version: Mapped[str | None] = mapped_column(Text)
    record_json: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)


class DetectionLegRow(Base):
    __tablename__ = "detection_legs"

    detection_id: Mapped[str] = mapped_column(
        ForeignKey("detections.detection_id", ondelete="CASCADE"), primary_key=True
    )
    leg_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    market_id: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)
    ratio: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    quantity: Mapped[Decimal | None] = mapped_column(Numeric)
    premium: Mapped[Decimal | None] = mapped_column(Numeric)
    leg_cost: Mapped[Decimal | None] = mapped_column(Numeric)


class DetectionScenarioRow(Base):
    __tablename__ = "detection_scenarios"

    detection_id: Mapped[str] = mapped_column(
        ForeignKey("detections.detection_id", ondelete="CASCADE"), primary_key=True
    )
    scenario_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    state_json: Mapped[str] = mapped_column(Text, nullable=False)
    payoff_per_unit: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    is_worst: Mapped[bool] = mapped_column(Boolean, nullable=False)


class SessionCheckpointRow(Base):
    __tablename__ = CHECKPOINT_TABLE
    __table_args__ = (
        UniqueConstraint("session_id", "ordinal", name="uq_session_checkpoint_ordinal"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(
        ForeignKey("ingestion_sessions.session_id", ondelete="CASCADE"), nullable=False
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    now_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_position: Mapped[int | None] = mapped_column(Integer)
    state_digest: Mapped[str] = mapped_column(Text, nullable=False)
    checkpoint_json: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ReplaySessionRow(Base):
    __tablename__ = "replay_sessions"

    replay_id: Mapped[str] = mapped_column(Text, primary_key=True)
    source_session_id: Mapped[str | None] = mapped_column(
        ForeignKey("ingestion_sessions.session_id")
    )
    status: Mapped[str] = mapped_column(Text, nullable=False)
    speed: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    position_ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    position_ms: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class NotificationOutboxRow(Base):
    """Append-only detection tail. ``id`` is assigned at insert, not at commit."""

    __tablename__ = "notification_outbox"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    session_id: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SystemHealthRow(Base):
    __tablename__ = "system_health"
    __table_args__ = (Index("ix_system_health_checked", "checked_at"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    component: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    detail: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)

"""Initial schema (spec section 11 plus session checkpoints).

Revision ID: 0001_initial
Revises:
Create Date: 2026-10-09

These operations are a frozen copy of the schema as of this revision. They do not
read the ORM metadata, so a later model change cannot rewrite this history.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES: tuple[str, ...] = (
    "configuration_versions",
    "data_sources",
    "fee_schedules",
    "relationships",
    "system_health",
    "ingestion_sessions",
    "relationship_members",
    "relationship_reviews",
    "series",
    "detections",
    "events",
    "orderbook_snapshots",
    "orderbook_updates",
    "replay_sessions",
    "session_checkpoints",
    "detection_legs",
    "detection_scenarios",
    "markets",
    "market_price_ranges",
    "market_rules",
)


def upgrade() -> None:
    op.create_table(
        "configuration_versions",
        sa.Column("version_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("document", postgresql.JSONB(), nullable=False),
    )
    op.create_table(
        "data_sources",
        sa.Column("data_source_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "fee_schedules",
        sa.Column("schedule_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("venue", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("effective_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("taker_coefficient", sa.Numeric(), nullable=False),
        sa.Column("maker_coefficient", sa.Numeric(), nullable=False),
        sa.Column("document", postgresql.JSONB(), nullable=False),
    )
    op.create_table(
        "relationships",
        sa.Column("relationship_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("relationship_type", sa.Text(), nullable=False),
        sa.Column("verification_status", sa.Text(), nullable=False),
        sa.Column("exhaustive", sa.Boolean(), nullable=False),
        sa.Column("fingerprint", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("document", postgresql.JSONB(), nullable=False),
    )
    op.create_table(
        "system_health",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("component", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("detail", postgresql.JSONB(), nullable=False),
    )
    op.create_table(
        "ingestion_sessions",
        sa.Column("session_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("data_source_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("fingerprint", sa.Text(), nullable=False),
        sa.Column("source_label", sa.Text(), nullable=False),
        sa.Column("deterministic", sa.Boolean(), nullable=False),
        sa.Column("pinned", sa.Boolean(), nullable=False),
        sa.Column("raw_persisted", sa.Boolean(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("config_version_id", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["data_source_id"], ["data_sources.data_source_id"]),
    )
    op.create_table(
        "relationship_members",
        sa.Column("relationship_id", sa.Text(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("relationship_id", "position"),
        sa.UniqueConstraint("relationship_id", "market_id", name="uq_relationship_member"),
        sa.ForeignKeyConstraint(
            ["relationship_id"], ["relationships.relationship_id"], ondelete="CASCADE"
        ),
    )
    op.create_table(
        "relationship_reviews",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("relationship_id", sa.Text(), nullable=False),
        sa.Column("review_id", sa.Text(), nullable=True),
        sa.Column("reviewer", sa.Text(), nullable=False),
        sa.Column("decision", sa.Text(), nullable=False),
        sa.Column("reasoning", sa.Text(), nullable=False),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("document", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["relationship_id"], ["relationships.relationship_id"], ondelete="CASCADE"
        ),
    )
    op.create_table(
        "series",
        sa.Column("series_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("data_source_id", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("provenance", sa.Text(), nullable=False),
        sa.Column("document", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(["data_source_id"], ["data_sources.data_source_id"]),
    )
    op.create_table(
        "detections",
        sa.Column("detection_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("relationship_id", sa.Text(), nullable=False),
        sa.Column("strategy_id", sa.Text(), nullable=False),
        sa.Column("template", sa.Text(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("classification", sa.Text(), nullable=False),
        sa.Column("peak_classification", sa.Text(), nullable=False),
        sa.Column("reason_codes", postgresql.JSONB(), nullable=False),
        sa.Column("first_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_observed_ms", sa.BigInteger(), nullable=False),
        sa.Column("last_observed_ms", sa.BigInteger(), nullable=False),
        sa.Column("first_position", sa.Integer(), nullable=False),
        sa.Column("last_position", sa.Integer(), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("close_reason", sa.Text(), nullable=True),
        sa.Column("close_detail", sa.Text(), nullable=True),
        sa.Column("max_deviation", sa.Numeric(), nullable=True),
        sa.Column("max_capacity", sa.Numeric(), nullable=True),
        sa.Column("max_net_edge", sa.Numeric(), nullable=True),
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column("certificate_hash", sa.Text(), nullable=False),
        sa.Column("certificate_json", sa.Text(), nullable=True),
        sa.Column("certificate_version", sa.Text(), nullable=True),
        sa.Column("record_json", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["session_id"], ["ingestion_sessions.session_id"], ondelete="CASCADE"
        ),
    )
    op.create_table(
        "events",
        sa.Column("event_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("ticker", sa.Text(), nullable=False),
        sa.Column("series_id", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("mutually_exclusive", sa.Boolean(), nullable=True),
        sa.Column("outcome_set_complete", sa.Boolean(), nullable=True),
        sa.Column("provenance", sa.Text(), nullable=False),
        sa.Column("document", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(["series_id"], ["series.series_id"]),
    )
    op.create_table(
        "orderbook_snapshots",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("connection_id", sa.Text(), nullable=True),
        sa.Column("subscription_id", sa.Text(), nullable=True),
        sa.Column("source_sequence", sa.BigInteger(), nullable=True),
        sa.Column("exchange_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_ts_ms", sa.BigInteger(), nullable=False),
        sa.Column("market_rules_hash", sa.Text(), nullable=True),
        sa.Column("fee_schedule_version", sa.Text(), nullable=False),
        sa.Column("relationship_version", sa.Text(), nullable=False),
        sa.Column("levels", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["session_id"], ["ingestion_sessions.session_id"], ondelete="CASCADE"
        ),
    )
    op.create_table(
        "orderbook_updates",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("market_id", sa.Text(), nullable=True),
        sa.Column("connection_id", sa.Text(), nullable=True),
        sa.Column("subscription_id", sa.Text(), nullable=True),
        sa.Column("source_sequence", sa.BigInteger(), nullable=True),
        sa.Column("exchange_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_ts_ms", sa.BigInteger(), nullable=False),
        sa.Column("price", sa.Numeric(), nullable=True),
        sa.Column("quantity_delta", sa.Numeric(), nullable=True),
        sa.Column("market_rules_hash", sa.Text(), nullable=True),
        sa.Column("fee_schedule_version", sa.Text(), nullable=False),
        sa.Column("relationship_version", sa.Text(), nullable=False),
        sa.Column("entry_json", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["session_id"], ["ingestion_sessions.session_id"], ondelete="CASCADE"
        ),
    )
    op.create_table(
        "replay_sessions",
        sa.Column("replay_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("source_session_id", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("speed", sa.Numeric(), nullable=False),
        sa.Column("position_ordinal", sa.Integer(), nullable=False),
        sa.Column("position_ms", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["source_session_id"], ["ingestion_sessions.session_id"]),
    )
    op.create_table(
        "session_checkpoints",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("session_id", sa.Text(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("now_ms", sa.BigInteger(), nullable=False),
        sa.Column("source_position", sa.Integer(), nullable=True),
        sa.Column("state_digest", sa.Text(), nullable=False),
        sa.Column("checkpoint_json", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("session_id", "ordinal", name="uq_session_checkpoint_ordinal"),
        sa.ForeignKeyConstraint(
            ["session_id"], ["ingestion_sessions.session_id"], ondelete="CASCADE"
        ),
    )
    op.create_table(
        "detection_legs",
        sa.Column("detection_id", sa.Text(), nullable=False),
        sa.Column("leg_index", sa.Integer(), nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("ratio", sa.Numeric(), nullable=False),
        sa.Column("quantity", sa.Numeric(), nullable=True),
        sa.Column("premium", sa.Numeric(), nullable=True),
        sa.Column("leg_cost", sa.Numeric(), nullable=True),
        sa.PrimaryKeyConstraint("detection_id", "leg_index"),
        sa.ForeignKeyConstraint(["detection_id"], ["detections.detection_id"], ondelete="CASCADE"),
    )
    op.create_table(
        "detection_scenarios",
        sa.Column("detection_id", sa.Text(), nullable=False),
        sa.Column("scenario_index", sa.Integer(), nullable=False),
        sa.Column("state_json", sa.Text(), nullable=False),
        sa.Column("payoff_per_unit", sa.Numeric(), nullable=False),
        sa.Column("is_worst", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("detection_id", "scenario_index"),
        sa.ForeignKeyConstraint(["detection_id"], ["detections.detection_id"], ondelete="CASCADE"),
    )
    op.create_table(
        "markets",
        sa.Column("market_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("ticker", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("series_id", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("open_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("close_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expiration_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("quantity_increment", sa.Numeric(), nullable=False),
        sa.Column("rules_version", sa.Text(), nullable=False),
        sa.Column("rules_hash", sa.Text(), nullable=False),
        sa.Column("provenance", sa.Text(), nullable=False),
        sa.Column("document", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["events.event_id"]),
        sa.ForeignKeyConstraint(["series_id"], ["series.series_id"]),
    )
    op.create_table(
        "market_price_ranges",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("range_index", sa.Integer(), nullable=False),
        sa.Column("start_price", sa.Numeric(), nullable=False),
        sa.Column("end_price", sa.Numeric(), nullable=False),
        sa.Column("step", sa.Numeric(), nullable=False),
        sa.ForeignKeyConstraint(["market_id"], ["markets.market_id"]),
        sa.UniqueConstraint("market_id", "range_index", name="uq_market_price_range"),
    )
    op.create_table(
        "market_rules",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("rules_version", sa.Text(), nullable=False),
        sa.Column("rules_hash", sa.Text(), nullable=False),
        sa.Column("rules_text", sa.Text(), nullable=False),
        sa.Column("settlement", postgresql.JSONB(), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["market_id"], ["markets.market_id"]),
        sa.UniqueConstraint("market_id", "rules_hash", name="uq_market_rules_hash"),
    )
    op.create_index("ix_system_health_checked", "system_health", ["checked_at"])
    op.create_index(
        "ix_ingestion_sessions_fingerprint", "ingestion_sessions", ["fingerprint", "status"]
    )
    op.create_index("ix_relationship_members_market", "relationship_members", ["market_id"])
    op.create_index("ix_detections_classification", "detections", ["classification"])
    op.create_index("ix_detections_first_observed", "detections", ["first_observed_at"])
    op.create_index("ix_detections_last_observed", "detections", ["last_observed_at"])
    op.create_index("ix_detections_session", "detections", ["session_id"])
    op.create_index("ix_events_ticker", "events", ["ticker"])
    op.create_index(
        "ix_orderbook_snapshots_lookup",
        "orderbook_snapshots",
        ["session_id", "market_id", "received_at"],
    )
    op.create_index(
        "ix_orderbook_snapshots_order",
        "orderbook_snapshots",
        ["session_id", "ordinal"],
        unique=True,
    )
    op.create_index(
        "ix_orderbook_updates_replay_order",
        "orderbook_updates",
        ["session_id", "ordinal"],
        unique=True,
    )
    op.create_index(
        "ix_orderbook_updates_session_sequence",
        "orderbook_updates",
        ["session_id", "subscription_id", "source_sequence"],
    )
    op.create_index("ix_markets_ticker", "markets", ["ticker"])
    present = set(sa_tables(op.get_bind()))
    missing = [name for name in _TABLES if name not in present]
    if missing:
        raise RuntimeError(f"migration did not create {missing}")


def downgrade() -> None:
    op.drop_table("market_rules")
    op.drop_table("market_price_ranges")
    op.drop_table("markets")
    op.drop_table("detection_scenarios")
    op.drop_table("detection_legs")
    op.drop_table("session_checkpoints")
    op.drop_table("replay_sessions")
    op.drop_table("orderbook_updates")
    op.drop_table("orderbook_snapshots")
    op.drop_table("events")
    op.drop_table("detections")
    op.drop_table("series")
    op.drop_table("relationship_reviews")
    op.drop_table("relationship_members")
    op.drop_table("ingestion_sessions")
    op.drop_table("system_health")
    op.drop_table("relationships")
    op.drop_table("fee_schedules")
    op.drop_table("data_sources")
    op.drop_table("configuration_versions")


def sa_tables(bind: object) -> list[str]:
    from sqlalchemy import inspect

    return list(inspect(bind).get_table_names())

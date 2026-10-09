"""Migrations and the spec section 11 schema against real PostgreSQL."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import text

from consistency_persistence.db import make_engine
from consistency_persistence.schema import CHECKPOINT_TABLE, SPEC_TABLES

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.integration

MONEY_COLUMNS = {
    "markets": ("quantity_increment",),
    "market_price_ranges": ("start_price", "end_price", "step"),
    "fee_schedules": ("taker_coefficient", "maker_coefficient"),
    "orderbook_updates": ("price", "quantity_delta"),
    "detections": ("max_deviation", "max_capacity", "max_net_edge"),
    "detection_legs": ("ratio", "quantity", "premium", "leg_cost"),
    "detection_scenarios": ("payoff_per_unit",),
    "replay_sessions": ("speed",),
}

INDEXES = (
    "ix_markets_ticker",
    "ix_events_ticker",
    "ix_relationship_members_market",
    "ix_detections_classification",
    "ix_detections_first_observed",
    "ix_orderbook_snapshots_lookup",
    "ix_orderbook_updates_replay_order",
    "ix_orderbook_updates_session_sequence",
)


def _alembic(url: str, *args: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = url
    subprocess.run([sys.executable, "-m", "alembic", *args], cwd=ROOT, env=env, check=True)


def test_upgrade_downgrade_upgrade(database_url: str) -> None:
    _alembic(database_url, "downgrade", "base")
    _alembic(database_url, "upgrade", "head")
    _alembic(database_url, "downgrade", "base")
    _alembic(database_url, "upgrade", "head")


async def test_tables_indexes_numeric_and_timestamptz(db: str) -> None:
    engine = make_engine(db)
    async with engine.connect() as conn:
        tables = set(
            (
                await conn.execute(
                    text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
                )
            ).scalars()
        )
        indexes = set(
            (
                await conn.execute(
                    text("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
                )
            ).scalars()
        )
        floats = (
            await conn.execute(
                text(
                    "SELECT table_name, column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND data_type IN ('double precision', 'real')"
                )
            )
        ).all()
        money = (
            await conn.execute(
                text(
                    "SELECT table_name, column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND column_name IN "
                    "('quantity_increment','start_price','end_price','step','taker_coefficient',"
                    "'maker_coefficient','price','quantity_delta','max_deviation','max_capacity',"
                    "'max_net_edge','ratio','quantity','premium','leg_cost','payoff_per_unit','speed')"
                )
            )
        ).all()
        timestamps = (
            await conn.execute(
                text(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND column_name = 'first_observed_at'"
                )
            )
        ).scalars()
    await engine.dispose()
    assert set(SPEC_TABLES) <= tables
    assert CHECKPOINT_TABLE in tables
    for name in INDEXES:
        assert name in indexes
    assert floats == []
    assert {row[2] for row in money} == {"numeric"}
    assert set(timestamps) == {"timestamp with time zone"}
    assert MONEY_COLUMNS["detections"] == ("max_deviation", "max_capacity", "max_net_edge")

"""Integration tests require a real PostgreSQL. They skip with a reason when it is absent."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import text

from consistency_persistence.db import make_engine, ping
from consistency_persistence.schema import CHECKPOINT_TABLE, SPEC_TABLES

ROOT = Path(__file__).resolve().parents[2]
_SKIP_UNSET = (
    "DATABASE_URL is not set; integration tests require PostgreSQL (never SQLite). "
    "Example: postgresql+asyncpg://consistency:consistency@127.0.0.1:5432/consistency"
)
_SKIP_DOWN = (
    "PostgreSQL is unreachable; integration tests require a running Postgres and were not "
    "executed. Start docker compose (postgres:16) or a local server and set DATABASE_URL."
)


def _url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        pytest.skip(_SKIP_UNSET)
    if url.startswith("sqlite"):
        pytest.skip("SQLite is not supported; integration tests require PostgreSQL")
    return url


@pytest.fixture(scope="session")
def database_url() -> str:
    url = _url()
    import asyncio

    if not asyncio.run(ping(url)):
        pytest.skip(_SKIP_DOWN)
    env = os.environ.copy()
    env["DATABASE_URL"] = url
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env=env,
        check=True,
    )
    return url


@pytest.fixture
async def db(database_url: str) -> str:
    engine = make_engine(database_url)
    tables = ", ".join((*SPEC_TABLES, CHECKPOINT_TABLE))
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
    await engine.dispose()
    return database_url

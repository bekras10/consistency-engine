"""Async engine helpers. Runtime driver is asyncpg; there is no SQLite fallback."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def make_engine(url: str) -> AsyncEngine:
    if url.startswith("sqlite"):
        raise ValueError("SQLite is not a supported database")
    if not url.startswith("postgresql+asyncpg://"):
        raise ValueError("DATABASE_URL must use the postgresql+asyncpg driver")
    return create_async_engine(url, pool_pre_ping=True)


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def ping(url: str) -> bool:
    """Return whether ``url`` accepts a ``SELECT 1``. Never raises; never logs the URL."""
    try:
        engine = make_engine(url)
    except ValueError:
        return False
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
    finally:
        await engine.dispose()

"""Internal process the Next.js dashboard calls.

Routes live under ``/internal``. They are not the Phase 11 ``/api/v1`` catalog.
The browser never needs this port: the Next server proxies ``/app-data``.
"""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from consistency_persistence.dashboard import (
    DetectionQuery,
    book_view,
    get_detection,
    get_market_row,
    get_relationship,
    list_detections,
    list_markets,
    list_relationships,
    read_overview,
)
from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.replay_host import BookTail, ReplayHost, snapshot

Json = dict[str, Any]
Handler = Callable[[async_sessionmaker[AsyncSession]], Awaitable[Json]]


def _optional_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    if value.startswith("-"):
        number = value[1:]
        sign = -1
    else:
        number = value
        sign = 1
    if not number.isdigit():
        raise ValueError("expected an integer")
    return sign * int(number)


def _optional_decimal(value: str | None) -> Decimal | None:
    if value is None or value == "":
        return None
    if any(mark in value for mark in "eE"):
        raise ValueError("exponent notation is not a money string")
    return Decimal(value)


def _text(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    return value


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    url = os.environ.get("DATABASE_URL")
    app.state.database_url = url
    app.state.sessions = None
    app.state.engine = None
    app.state.books = BookTail()
    app.state.replay = ReplayHost()
    if url:
        app.state.engine = make_engine(url)
        app.state.sessions = make_sessionmaker(app.state.engine)
    yield
    engine = app.state.engine
    if engine is not None:
        await engine.dispose()


app = FastAPI(
    title="Consistency dashboard gateway",
    docs_url=None,
    redoc_url=None,
    lifespan=_lifespan,
)


def _sessions(request: Request) -> async_sessionmaker[AsyncSession] | None:
    sessions = request.app.state.sessions
    if isinstance(sessions, async_sessionmaker):
        return sessions
    return None


async def _database_up(sessions: async_sessionmaker[AsyncSession]) -> bool:
    try:
        async with sessions() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def _run(request: Request, handler: Handler) -> JSONResponse:
    sessions = _sessions(request)
    if sessions is None:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "error": "database_unavailable",
                "detail": "DATABASE_URL is not set",
            },
        )
    try:
        body = await handler(sessions)
    except KeyError:
        return JSONResponse(status_code=404, content={"ok": False, "error": "not_found"})
    except ValueError as exc:
        return JSONResponse(
            status_code=400, content={"ok": False, "error": "invalid_input", "detail": str(exc)}
        )
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={"ok": False, "error": "database_unavailable", "detail": type(exc).__name__},
        )
    return JSONResponse(content={"ok": True, "data": body})


@app.get("/internal/health")
async def health(request: Request) -> JSONResponse:
    sessions = _sessions(request)
    if sessions is None or not await _database_up(sessions):
        return JSONResponse(
            status_code=503,
            content={"ok": False, "error": "database_unavailable"},
        )
    return JSONResponse(content={"ok": True, "database": "ok"})


@app.get("/internal/overview")
async def overview(request: Request) -> JSONResponse:
    async def handler(sessions: async_sessionmaker[AsyncSession]) -> Json:
        try:
            await request.app.state.books.refresh(sessions)
            sync = request.app.state.books.summary()
        except Exception as exc:
            sync = {"available": False, "error": type(exc).__name__}
        async with sessions() as session:
            return await read_overview(session, sync)

    return await _run(request, handler)


@app.get("/internal/relationships")
async def relationships(request: Request) -> JSONResponse:
    params = request.query_params

    async def handler(sessions: async_sessionmaker[AsyncSession]) -> Json:
        async with sessions() as session:
            return await list_relationships(
                session,
                relationship_type=_text(params.get("type")),
                status=_text(params.get("status")),
                category=_text(params.get("category")),
                market_id=_text(params.get("market")),
                min_members=_optional_int(params.get("min_members")),
                max_members=_optional_int(params.get("max_members")),
            )

    return await _run(request, handler)


@app.get("/internal/relationships/{relationship_id}")
async def relationship(request: Request, relationship_id: str) -> JSONResponse:
    async def handler(sessions: async_sessionmaker[AsyncSession]) -> Json:
        async with sessions() as session:
            row = await get_relationship(session, relationship_id)
        if row is None:
            raise KeyError(relationship_id)
        return row

    return await _run(request, handler)


@app.get("/internal/detections")
async def detections(request: Request) -> JSONResponse:
    params = request.query_params

    async def handler(sessions: async_sessionmaker[AsyncSession]) -> Json:
        query = DetectionQuery(
            classification=_text(params.get("classification")),
            relationship_type=_text(params.get("relationship_type")),
            since_ms=_optional_int(params.get("since_ms")),
            until_ms=_optional_int(params.get("until_ms")),
            min_net_edge=_optional_decimal(params.get("min_net_edge")),
            min_quantity=_optional_decimal(params.get("min_quantity")),
            market_id=_text(params.get("market")),
            sort=_text(params.get("sort")) or "time",
            direction=_text(params.get("direction")) or "desc",
        )
        async with sessions() as session:
            return await list_detections(session, query)

    return await _run(request, handler)


@app.get("/internal/detections/{detection_id}")
async def detection(request: Request, detection_id: str) -> JSONResponse:
    async def handler(sessions: async_sessionmaker[AsyncSession]) -> Json:
        async with sessions() as session:
            row = await get_detection(session, detection_id)
        if row is None:
            raise KeyError(detection_id)
        try:
            manager = await request.app.state.books.refresh(sessions)
        except Exception:
            manager = None
        books = []
        if manager is not None:
            markets = row.get("markets")
            if isinstance(markets, list):
                for market in markets:
                    if isinstance(market, dict):
                        market_id = market.get("market_id")
                        if isinstance(market_id, str) and manager.has_market(market_id):
                            books.append(book_view(manager, market_id))
        row["books"] = books
        return row

    return await _run(request, handler)


@app.get("/internal/markets")
async def markets(request: Request) -> JSONResponse:
    async def handler(sessions: async_sessionmaker[AsyncSession]) -> Json:
        try:
            manager = await request.app.state.books.refresh(sessions)
        except Exception:
            manager = None
        async with sessions() as session:
            payload = await list_markets(session)
        listed = payload.get("markets")
        if manager is not None and isinstance(listed, list):
            for item in listed:
                if not isinstance(item, dict):
                    continue
                market_id = item.get("market_id")
                if isinstance(market_id, str) and manager.has_market(market_id):
                    item["book"] = book_view(manager, market_id)
                else:
                    item["book"] = None
        return payload

    return await _run(request, handler)


@app.get("/internal/markets/{market_id}")
async def market(request: Request, market_id: str) -> JSONResponse:
    async def handler(sessions: async_sessionmaker[AsyncSession]) -> Json:
        async with sessions() as session:
            row = await get_market_row(session, market_id)
        if row is None:
            raise KeyError(market_id)
        try:
            manager = await request.app.state.books.refresh(sessions)
        except Exception:
            manager = None
        if manager is not None and manager.has_market(market_id):
            row["book"] = book_view(manager, market_id)
        else:
            row["book"] = None
        return row

    return await _run(request, handler)


@app.get("/internal/replay/{replay_id}")
async def replay_state(request: Request, replay_id: str) -> JSONResponse:
    async def handler(sessions: async_sessionmaker[AsyncSession]) -> Json:
        playback = await request.app.state.replay.ensure(sessions, replay_id)
        return snapshot(playback)

    return await _run(request, handler)


@app.post("/internal/replay/{replay_id}/{action}")
async def replay_command(request: Request, replay_id: str, action: str) -> JSONResponse:
    payload = await request.body()
    parsed = json.loads(payload) if payload else {}
    raw = parsed if isinstance(parsed, dict) else {}

    async def handler(sessions: async_sessionmaker[AsyncSession]) -> Json:
        body = await request.app.state.replay.command(sessions, replay_id, action, raw)
        if not isinstance(body, dict):
            raise TypeError("replay snapshot was not an object")
        return body

    return await _run(request, handler)


def main() -> None:
    port = int(os.environ.get("DASHBOARD_GATEWAY_PORT", "8765"))
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")


if __name__ == "__main__":
    main()

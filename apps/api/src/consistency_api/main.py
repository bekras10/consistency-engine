"""FastAPI application. Public routes live under ``/api/v1``."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from consistency_api.http import RateLimit, register_errors
from consistency_api.routes import router
from consistency_connectors.settings import ConfigurationError, Settings
from consistency_persistence.db import make_engine, make_sessionmaker, ping
from consistency_persistence.replay_host import BookTail, ReplayHost


def _origins() -> list[str]:
    raw = os.environ.get("CORS_ORIGINS", "http://127.0.0.1:3000,http://localhost:3000")
    return [item.strip() for item in raw.split(",") if item.strip()]


def create_app(
    settings: Settings | None = None,
    *,
    sessions: async_sessionmaker[AsyncSession] | None = None,
    replay: ReplayHost | None = None,
    books: BookTail | None = None,
) -> FastAPI:
    config_error: str | None = None
    resolved: Settings | None = settings
    if resolved is None:
        try:
            resolved = Settings()
        except (ConfigurationError, ValueError) as exc:
            config_error = type(exc).__name__

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine: AsyncEngine | None = None
        if app.state.sessions is None and resolved is not None and resolved.database_url:
            engine = make_engine(resolved.database_url)
            app.state.sessions = make_sessionmaker(engine)
        yield
        if engine is not None:
            await engine.dispose()

    app = FastAPI(title="Consistency Engine API", version="0.11.0", lifespan=lifespan)
    app.state.sessions = sessions
    app.state.replay = replay if replay is not None else ReplayHost()
    app.state.books = books if books is not None else BookTail()
    app.add_middleware(RateLimit)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_origins(),
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Content-Type", "X-Replay-Token", "Last-Event-ID"],
        allow_credentials=False,
    )
    register_errors(app)
    app.include_router(router)

    @app.get("/api/v1/health/live")
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/v1/health/ready")
    async def ready(response: Response) -> dict[str, Any]:
        if resolved is None:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {
                "status": "not_ready",
                "reason": "invalid_configuration",
                "error": config_error,
            }
        body: dict[str, Any] = {"status": "ready", "configuration": resolved.redacted()}
        if resolved.database_url:
            database_ok = await ping(resolved.database_url)
            body["database"] = "ok" if database_ok else "unavailable"
            if not database_ok:
                response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
                body["status"] = "not_ready"
                body["reason"] = "database_unavailable"
        return body

    return app


app = create_app()

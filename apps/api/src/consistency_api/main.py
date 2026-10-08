"""FastAPI application. Milestone 1 exposes only health endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Response, status

from consistency_connectors.settings import ConfigurationError, Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    app = FastAPI(title="Consistency Engine API", version="0.1.0")
    config_error: str | None = None
    resolved: Settings | None = settings
    if resolved is None:
        try:
            resolved = Settings()
        except (ConfigurationError, ValueError) as exc:
            config_error = type(exc).__name__

    @app.get("/api/v1/health/live")
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/v1/health/ready")
    def ready(response: Response) -> dict[str, Any]:
        if resolved is None:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"status": "not_ready", "reason": "invalid_configuration", "error": config_error}
        return {"status": "ready", "configuration": resolved.redacted()}

    return app


app = create_app()

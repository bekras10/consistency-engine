"""Shared HTTP policy for the public API: errors, auth, and a small rate limit."""

from __future__ import annotations

import os
import secrets
import time
from collections import defaultdict, deque

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

_WINDOW_S = 60.0
_MAX_REQUESTS = 240
_HITS: dict[str, deque[float]] = defaultdict(deque)


def error_body(code: str) -> dict[str, str]:
    return {"error": code}


def replay_mutations_public() -> bool:
    flag = os.environ.get("REPLAY_MUTATIONS_PUBLIC", "false").strip().lower()
    return flag in {"1", "true", "yes"}


def replay_token_ok(presented: str) -> bool:
    """True when replay mutations are public or ``presented`` matches ``REPLAY_API_TOKEN``."""
    if replay_mutations_public():
        return True
    expected = os.environ.get("REPLAY_API_TOKEN", "").strip()
    if not expected or not presented:
        return False
    return secrets.compare_digest(presented, expected)


def authorize_replay(request: Request) -> None:
    """POST replay controls require ``X-Replay-Token``.

    The expected value is ``REPLAY_API_TOKEN``. Mutations stay protected unless
    ``REPLAY_MUTATIONS_PUBLIC`` is explicitly ``true``. GET reads are not checked
    here; they are open on a local deployment. See ``docs/api-reference.md``.
    """
    if not replay_token_ok(request.headers.get("x-replay-token", "")):
        raise HTTPException(status_code=401, detail="unauthorized")


def _allow(host: str) -> bool:
    now = time.monotonic()
    hits = _HITS[host]
    while hits and now - hits[0] > _WINDOW_S:
        hits.popleft()
    if len(hits) >= _MAX_REQUESTS:
        return False
    hits.append(now)
    return True


class RateLimit:
    """Count ``/api/v1`` requests per client. Health probes are exempt.

    This is an ASGI wrapper so it does not buffer an SSE body.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            path = str(scope.get("path", ""))
            if path.startswith("/api/v1/") and not path.startswith("/api/v1/health"):
                client = scope.get("client")
                host = str(client[0]) if isinstance(client, tuple) and client else "unknown"
                if not _allow(host):
                    body = b'{"error":"rate_limited"}'
                    await send(
                        {
                            "type": "http.response.start",
                            "status": 429,
                            "headers": [
                                (b"content-type", b"application/json"),
                                (b"content-length", str(len(body)).encode()),
                            ],
                        }
                    )
                    await send({"type": "http.response.body", "body": body})
                    return
        await self.app(scope, receive, send)


def register_errors(app: FastAPI) -> None:
    @app.exception_handler(HTTPException)
    async def http_error(_request: Request, exc: HTTPException) -> JSONResponse:
        code = exc.detail if isinstance(exc.detail, str) else "error"
        return JSONResponse(error_body(code), status_code=exc.status_code)

    @app.exception_handler(RequestValidationError)
    async def invalid(_request: Request, _exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(error_body("invalid_request"), status_code=422)

    @app.exception_handler(Exception)
    async def unhandled(_request: Request, _exc: Exception) -> JSONResponse:
        return JSONResponse(error_body("internal_error"), status_code=500)

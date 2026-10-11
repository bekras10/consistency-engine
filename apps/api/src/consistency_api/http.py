"""Shared HTTP policy for the public API: errors, auth, timeouts, and a small rate limit."""

from __future__ import annotations

import asyncio
import os
import secrets
import time
from collections import defaultdict, deque

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


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


def _limited(scope: Scope) -> bool:
    if scope["type"] != "http":
        return False
    path = str(scope.get("path", ""))
    return path.startswith("/api/v1/") and not path.startswith("/api/v1/health")


def _client_host(scope: Scope) -> str:
    client = scope.get("client")
    if isinstance(client, tuple) and client:
        return str(client[0])
    return "unknown"


async def _json_error(send: Send, status: int, code: str) -> None:
    body = f'{{"error":"{code}"}}'.encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


class RateLimit:
    """Count ``/api/v1`` requests per client. Health probes are exempt.

    This is an ASGI wrapper so it does not buffer an SSE body. The counter lives
    on the app instance. ``/api/v1/stream`` counts, but a long-lived stream is
    one request.
    """

    def __init__(self, app: ASGIApp, *, max_requests: int = 240, window_s: float = 60.0) -> None:
        self.app = app
        self.max_requests = max_requests
        self.window_s = window_s
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def _allow(self, host: str) -> bool:
        now = time.monotonic()
        hits = self._hits[host]
        while hits and now - hits[0] > self.window_s:
            hits.popleft()
        if len(hits) >= self.max_requests:
            return False
        hits.append(now)
        return True

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if _limited(scope) and not self._allow(_client_host(scope)):
            await _json_error(send, 429, "rate_limited")
            return
        await self.app(scope, receive, send)


class RequestTimeout:
    """Bound ordinary ``/api/v1`` requests. Health and the SSE tail are exempt."""

    def __init__(self, app: ASGIApp, *, timeout_s: float = 30.0) -> None:
        self.app = app
        self.timeout_s = timeout_s

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = str(scope.get("path", ""))
        exempt = scope["type"] != "http" or path.startswith(("/api/v1/health", "/api/v1/stream"))
        if exempt:
            await self.app(scope, receive, send)
            return
        try:
            await asyncio.wait_for(self.app(scope, receive, send), self.timeout_s)
        except TimeoutError:
            await _json_error(send, 504, "timeout")


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

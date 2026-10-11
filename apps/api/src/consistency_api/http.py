"""Shared HTTP policy for the public API: errors, auth, timeouts, and a small rate limit."""

from __future__ import annotations

import asyncio
import os
import secrets
import time
from collections import deque
from collections.abc import Callable
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send


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


def origin_allowed(request: Request) -> bool:
    """Same-origin and missing ``Origin`` are allowed. A foreign ``Origin`` is not.

    Browsers send ``Origin`` on cross-site POST. Non-browser clients (curl, the
    API test client, and other server-side tools) omit it. A missing ``Origin``
    is therefore allowed so those clients can still call replay mutations with
    ``X-Replay-Token``. An unparsable or ``null`` origin is rejected.
    """
    origin = request.headers.get("origin")
    if origin is None or origin.strip() == "":
        return True
    if origin.strip() == "null":
        return False
    presented = urlsplit(origin.strip())
    if presented.scheme not in {"http", "https"} or not presented.netloc:
        return False
    expected = urlsplit(str(request.url))
    return (presented.scheme, presented.netloc) == (expected.scheme, expected.netloc)


def authorize_replay(request: Request) -> None:
    """POST replay controls require ``X-Replay-Token`` and a same-site ``Origin``.

    The expected value is ``REPLAY_API_TOKEN``. Mutations stay protected unless
    ``REPLAY_MUTATIONS_PUBLIC`` is explicitly ``true``. GET reads are not checked
    here; they are open on a local deployment. See ``docs/api-reference.md``.
    A cross-site ``Origin`` is ``403`` ``cross_origin`` before the token is checked.
    """
    if not origin_allowed(request):
        raise HTTPException(status_code=403, detail="cross_origin")
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


def _header(scope: Scope, name: bytes) -> str:
    raw_headers = scope.get("headers")
    if not isinstance(raw_headers, list):
        return ""
    for item in raw_headers:
        if not isinstance(item, tuple) or len(item) != 2:
            continue
        key, value = item
        if isinstance(key, bytes) and key.lower() == name and isinstance(value, bytes):
            return value.decode("latin-1")
    return ""


def trusted_proxy_set(raw: str) -> frozenset[str]:
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def client_identity(
    scope: Scope,
    *,
    trusted_proxy_count: int,
    trusted_proxies: frozenset[str],
) -> str:
    """Identity used by the rate limit.

    ``X-Forwarded-For`` is ignored unless ``trusted_proxy_count`` is at least 1
    and the immediate TCP peer is in ``trusted_proxies``. The client is then the
    address ``trusted_proxy_count`` hops from the right (the hop a trusted proxy
    appended). Entries further left are client-controlled and do not create a
    new bucket. A short header, or a peer that is not listed, stays the TCP peer.
    """
    peer = _client_host(scope)
    if trusted_proxy_count < 1 or peer not in trusted_proxies:
        return peer
    parts = [part.strip() for part in _header(scope, b"x-forwarded-for").split(",") if part.strip()]
    if len(parts) < trusted_proxy_count:
        return peer
    return parts[-trusted_proxy_count]


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
    one request. Entries whose newest hit is outside the window are dropped.
    At most ``max_clients`` identities are stored; a new identity past that cap
    is refused and is not inserted. ``X-Forwarded-For`` is not a client id
    unless a trusted proxy is configured. See ``client_identity``.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        max_requests: int = 240,
        window_s: float = 60.0,
        max_clients: int = 4096,
        trusted_proxy_count: int = 0,
        trusted_proxies: frozenset[str] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.app = app
        self.max_requests = max_requests
        self.window_s = window_s
        self.max_clients = max_clients
        self.trusted_proxy_count = trusted_proxy_count
        self.trusted_proxies = frozenset() if trusted_proxies is None else trusted_proxies
        self._clock = time.monotonic if clock is None else clock
        self._hits: dict[str, deque[float]] = {}

    def _purge(self, now: float) -> None:
        stale = [
            key for key, hits in self._hits.items() if not hits or now - hits[-1] > self.window_s
        ]
        for key in stale:
            del self._hits[key]

    def _allow(self, host: str) -> bool:
        now = self._clock()
        self._purge(now)
        hits = self._hits.get(host)
        if hits is None:
            if len(self._hits) >= self.max_clients:
                return False
            hits = deque()
            self._hits[host] = hits
        while hits and now - hits[0] > self.window_s:
            hits.popleft()
        if len(hits) >= self.max_requests:
            return False
        hits.append(now)
        return True

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if _limited(scope) and not self._allow(
            client_identity(
                scope,
                trusted_proxy_count=self.trusted_proxy_count,
                trusted_proxies=self.trusted_proxies,
            )
        ):
            await _json_error(send, 429, "rate_limited")
            return
        await self.app(scope, receive, send)


class RequestTimeout:
    """Bound ordinary ``/api/v1`` requests. Health and the SSE tail are exempt.

    If the handler has already sent ``http.response.start``, a timeout does not
    write a second status line. The task is cancelled and the connection drops.
    A 504 is sent only when no response bytes have been started.
    """

    def __init__(self, app: ASGIApp, *, timeout_s: float = 30.0) -> None:
        self.app = app
        self.timeout_s = timeout_s

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = str(scope.get("path", ""))
        exempt = scope["type"] != "http" or path.startswith(("/api/v1/health", "/api/v1/stream"))
        if exempt:
            await self.app(scope, receive, send)
            return
        started = False

        async def tracking_send(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await asyncio.wait_for(self.app(scope, receive, tracking_send), self.timeout_s)
        except TimeoutError:
            if started:
                return
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

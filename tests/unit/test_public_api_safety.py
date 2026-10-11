"""Public /api/v1 responses stay small: no stacks, no fetched URLs, a modest rate limit."""

from __future__ import annotations

import asyncio
import inspect
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import consistency_api.routes as routes
from consistency_api.main import create_app
from consistency_connectors.settings import Settings
from consistency_persistence.db import make_engine, make_sessionmaker

ROOT = Path(__file__).resolve().parents[2]
_SECRET = "s3cret-db-password"


def test_ready_fails_closed_when_the_database_is_down(monkeypatch: pytest.MonkeyPatch) -> None:
    url = f"postgresql+asyncpg://consistency:{_SECRET}@127.0.0.1:1/consistency"

    async def down(_url: str) -> bool:
        raise RuntimeError(f"could not connect to {_url}")

    monkeypatch.setattr("consistency_api.main.ping", down)
    engine = make_engine(url)
    app = create_app(Settings(DATABASE_URL=url), sessions=make_sessionmaker(engine))
    try:
        client = TestClient(app)
        response = client.get("/api/v1/health/ready")
    finally:
        asyncio.run(engine.dispose())
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["reason"] == "database_unavailable"
    assert _SECRET not in response.text
    assert "Traceback" not in response.text
    assert "could not connect" not in response.text


def test_unhandled_error_has_no_stack_trace() -> None:
    app = create_app(Settings())

    @app.get("/api/v1/boom")
    def boom() -> dict[str, str]:
        raise RuntimeError(f"leak {_SECRET} postgresql+asyncpg://u:{_SECRET}@db/x")

    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/api/v1/boom")
    assert response.status_code == 500
    assert response.json() == {"error": "internal_error"}
    assert _SECRET not in response.text
    assert "Traceback" not in response.text


def test_invalid_input_is_422_without_a_stack() -> None:
    app = create_app(Settings())
    client = TestClient(app)
    response = client.get("/api/v1/markets", params={"limit": 0, "url": "http://127.0.0.1:9/x"})
    assert response.status_code == 422
    assert response.json() == {"error": "invalid_request"}
    assert "Traceback" not in response.text


def test_rate_limit_rejects_the_next_request() -> None:
    app = create_app(Settings(API_RATE_LIMIT=2, API_RATE_WINDOW_S=60))
    client = TestClient(app)
    assert client.get("/api/v1/markets", params={"limit": 1}).status_code == 503
    assert client.get("/api/v1/markets", params={"limit": 1}).status_code == 503
    limited = client.get("/api/v1/markets", params={"limit": 1})
    assert limited.status_code == 429
    assert limited.json() == {"error": "rate_limited"}
    assert client.get("/api/v1/health/live").status_code == 200


def test_request_timeout_returns_504() -> None:
    app = create_app(Settings(API_REQUEST_TIMEOUT_S=0.05, API_RATE_LIMIT=100))

    @app.get("/api/v1/sleep")
    async def sleeper() -> dict[str, bool]:
        await asyncio.sleep(1)
        return {"ok": True}

    client = TestClient(app)
    response = client.get("/api/v1/sleep")
    assert response.status_code == 504
    assert response.json() == {"error": "timeout"}
    assert "Traceback" not in response.text


def test_timeout_after_headers_does_not_write_a_second_response() -> None:
    """A stall during the body must not emit a second status line.

    The client may see a dropped connection. It must not see a 504 written
    onto a response whose headers were already sent.
    """
    from consistency_api.http import RequestTimeout

    messages: list[dict[str, object]] = []

    async def stalled(scope: dict[str, object], receive: object, send: object) -> None:
        del scope, receive
        assert callable(send)
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await asyncio.sleep(5)
        await send({"type": "http.response.body", "body": b"late"})

    middleware = RequestTimeout(stalled, timeout_s=0.05)

    async def send(message: dict[str, object]) -> None:
        if message.get("type") == "http.response.start" and messages:
            raise RuntimeError("wrote a 504 on an already-started response")
        messages.append(message)

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": b"", "more_body": False}

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.5"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/api/v1/stall",
        "raw_path": b"/api/v1/stall",
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 9),
        "server": ("test", 80),
    }
    asyncio.run(asyncio.wait_for(middleware(scope, receive, send), 2))
    starts = [item for item in messages if item.get("type") == "http.response.start"]
    assert len(starts) == 1
    assert starts[0]["status"] == 200
    assert all(item.get("status") != 504 for item in messages)
    body = b"".join(
        item["body"]
        for item in messages
        if item.get("type") == "http.response.body" and isinstance(item.get("body"), bytes)
    )
    assert b"timeout" not in body


def _http_scope(host: str, xff: str | None = None) -> dict[str, object]:
    headers: list[tuple[bytes, bytes]] = []
    if xff is not None:
        headers.append((b"x-forwarded-for", xff.encode()))
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.5"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/api/v1/markets",
        "raw_path": b"/api/v1/markets",
        "query_string": b"limit=1",
        "headers": headers,
        "client": (host, 1234),
        "server": ("test", 80),
    }


async def _status(app: object, scope: dict[str, object]) -> int:
    status = 0

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, object]) -> None:
        nonlocal status
        if message.get("type") == "http.response.start":
            status = int(message["status"])

    assert callable(app)
    await app(scope, receive, send)
    return status


async def test_rate_limit_forgets_expired_clients() -> None:
    from consistency_api.http import RateLimit

    async def ok(scope: dict[str, object], receive: object, send: object) -> None:
        del scope, receive
        assert callable(send)
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    limiter = RateLimit(ok, max_requests=5, window_s=0.05)
    await limiter(_http_scope("10.1.0.1"), _receive_unused, _send_unused)
    assert "10.1.0.1" in limiter._hits
    await asyncio.sleep(0.12)
    await limiter(_http_scope("10.1.0.2"), _receive_unused, _send_unused)
    assert "10.1.0.1" not in limiter._hits


async def _receive_unused() -> dict[str, object]:
    return {"type": "http.request", "body": b"", "more_body": False}


async def _send_unused(message: dict[str, object]) -> None:
    del message


async def test_rate_limit_caps_distinct_clients() -> None:
    from consistency_api.http import RateLimit

    async def ok(scope: dict[str, object], receive: object, send: object) -> None:
        del scope, receive
        assert callable(send)
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    limiter = RateLimit(ok, max_requests=10, window_s=60, max_clients=2)
    assert await _status(limiter, _http_scope("10.2.0.1")) == 204
    assert await _status(limiter, _http_scope("10.2.0.2")) == 204
    assert await _status(limiter, _http_scope("10.2.0.3")) == 429
    assert len(limiter._hits) <= 2
    assert await _status(limiter, _http_scope("10.2.0.1")) == 204


async def test_spoofed_forwarded_for_does_not_get_a_fresh_bucket() -> None:
    app = create_app(Settings(API_RATE_LIMIT=1, API_RATE_WINDOW_S=60))
    assert await _status(app, _http_scope("10.9.9.9", "1.1.1.1")) == 503
    limited = await _status(app, _http_scope("10.9.9.9", "2.2.2.2"))
    assert limited == 429


async def test_trusted_proxy_limits_the_appended_client_not_a_spoofed_prefix() -> None:
    app = create_app(
        Settings(
            API_RATE_LIMIT=1,
            API_RATE_WINDOW_S=60,
            TRUSTED_PROXY_COUNT=1,
            TRUSTED_PROXIES="10.0.0.8",
        )
    )
    peer = "10.0.0.8"
    assert await _status(app, _http_scope(peer, "1.1.1.1, 203.0.113.5")) == 503
    assert await _status(app, _http_scope(peer, "9.9.9.9, 203.0.113.5")) == 429
    assert await _status(app, _http_scope(peer, "9.9.9.9, 203.0.113.6")) == 503
    # A peer that is not a configured proxy cannot pick an identity with the header.
    assert await _status(app, _http_scope("198.51.100.4", "203.0.113.8")) == 503
    assert await _status(app, _http_scope("198.51.100.4", "203.0.113.9")) == 429


def test_replay_mutation_rejects_a_foreign_origin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPLAY_API_TOKEN", "tok")
    monkeypatch.setenv("REPLAY_MUTATIONS_PUBLIC", "false")
    app = create_app(Settings())
    client = TestClient(app)
    foreign = client.post(
        "/api/v1/replay/sessions/missing/pause",
        headers={"x-replay-token": "tok", "origin": "https://evil.example"},
    )
    assert foreign.status_code == 403
    assert foreign.json() == {"error": "cross_origin"}
    same = client.post(
        "/api/v1/replay/sessions/missing/pause",
        headers={"x-replay-token": "tok", "origin": "http://testserver"},
    )
    assert same.status_code == 404
    missing = client.post(
        "/api/v1/replay/sessions/missing/pause",
        headers={"x-replay-token": "tok"},
    )
    assert missing.status_code == 404


def test_routes_do_not_fetch_urls_or_run_shell() -> None:
    source = inspect.getsource(routes)
    for banned in ("subprocess", "os.system", "urlopen", "httpx", "aiohttp"):
        assert banned not in source
    app = create_app(Settings())
    client = TestClient(app)
    response = client.get(
        "/api/v1/markets",
        params={"limit": 1, "url": "http://127.0.0.1:9/secret", "command": "rm -rf /"},
    )
    assert response.status_code == 503
    assert response.json()["error"] == "database_unavailable"
    assert "127.0.0.1:9" not in response.text


def test_worker_command_line_omits_secrets() -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = f"postgresql+asyncpg://consistency:{_SECRET}@127.0.0.1:1/consistency"
    env["REPLAY_API_TOKEN"] = _SECRET
    env["PYTHONPATH"] = os.pathsep.join(
        [
            str(ROOT / "packages/core/src"),
            str(ROOT / "packages/simulation/src"),
            str(ROOT / "packages/connectors/src"),
            str(ROOT / "packages/pipeline/src"),
            str(ROOT / "packages/persistence/src"),
            str(ROOT / "apps/api/src"),
            str(ROOT / "apps/worker/src"),
        ]
    )
    argv = [sys.executable, "-m", "consistency_worker"]
    assert _SECRET not in " ".join(argv)
    proc = subprocess.Popen(
        argv,
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    args = ""
    try:
        for _ in range(20):
            if proc.poll() is not None:
                break
            args = subprocess.check_output(["ps", "-o", "args=", "-p", str(proc.pid)], text=True)
            if args.strip():
                break
            time.sleep(0.05)
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=5)
    assert _SECRET not in args

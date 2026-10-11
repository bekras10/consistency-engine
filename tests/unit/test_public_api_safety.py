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

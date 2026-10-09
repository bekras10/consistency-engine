"""Contract tests for /api/v1. Values come from PostgreSQL and PlaybackService."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
import pytest
from sqlalchemy import text

from consistency_api.main import create_app
from consistency_connectors.settings import Settings
from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.recording import BatchJournal, VersionStamp
from consistency_persistence.replay_host import BookTail, ReplayHost
from consistency_persistence.store import PersistenceStore
from consistency_pipeline.journal import JournalEntry
from consistency_pipeline.lifecycle import EventKind
from tests.golden.support import fee_calculator
from tests.integration.test_phase10_correctness import _one_market, _open
from tests.integration.test_postgres_pipeline import _messages
from tests.unit.test_detection_pipeline import Harness
from tests.unit.test_ingestion import Feed, snap

pytestmark = pytest.mark.integration

_PATHS = {
    "/api/v1/health/live",
    "/api/v1/health/ready",
    "/api/v1/system/status",
    "/api/v1/system/metrics",
    "/api/v1/markets",
    "/api/v1/markets/{market_id}",
    "/api/v1/markets/{market_id}/orderbook",
    "/api/v1/relationships",
    "/api/v1/relationships/{relationship_id}",
    "/api/v1/detections",
    "/api/v1/detections/{detection_id}",
    "/api/v1/detections/{detection_id}/proof",
    "/api/v1/replay/sessions",
    "/api/v1/replay/sessions/{session_id}",
    "/api/v1/replay/sessions/{session_id}/start",
    "/api/v1/replay/sessions/{session_id}/pause",
    "/api/v1/replay/sessions/{session_id}/seek",
    "/api/v1/replay/sessions/{session_id}/resume",
    "/api/v1/replay/sessions/{session_id}/restart",
    "/api/v1/replay/sessions/{session_id}/step",
    "/api/v1/replay/sessions/{session_id}/speed",
    "/api/v1/stream",
}

_TOKEN = "phase11-token"


def _walk(value: object) -> None:
    if isinstance(value, float):
        raise AssertionError("JSON contained a binary float")
    if isinstance(value, dict):
        for item in value.values():
            _walk(item)
    elif isinstance(value, list):
        for item in value:
            _walk(item)


def _frames(text: str) -> list[dict[str, object]]:
    frames: list[dict[str, object]] = []
    for part in text.split("\n\n"):
        if not part.strip():
            continue
        frame: dict[str, object] = {}
        data: list[str] = []
        for line in part.split("\n"):
            if line.startswith("id:"):
                frame["id"] = line[3:].strip()
            elif line.startswith("event:"):
                frame["event"] = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].strip())
        if data:
            parsed = json.loads("\n".join(data))
            if isinstance(parsed, dict):
                frame["data"] = parsed
        frames.append(frame)
    return frames


async def _client(db: str, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[httpx.AsyncClient]:
    monkeypatch.setenv("DATABASE_URL", db)
    monkeypatch.setenv("REPLAY_API_TOKEN", _TOKEN)
    monkeypatch.setenv("REPLAY_MUTATIONS_PUBLIC", "false")
    engine = make_engine(db)
    app = create_app(
        Settings(),
        sessions=make_sessionmaker(engine),
        replay=ReplayHost(),
        books=BookTail(),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    await engine.dispose()


@pytest.fixture
async def api(db: str, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[httpx.AsyncClient]:
    async for client in _client(db, monkeypatch):
        yield client


async def test_documented_routes_and_empty_reads(api: httpx.AsyncClient) -> None:
    schema = (await api.get("/openapi.json")).json()
    assert set(schema["paths"]) >= _PATHS
    live = await api.get("/api/v1/health/live")
    ready = await api.get("/api/v1/health/ready")
    assert live.status_code == 200 and live.json()["status"] == "ok"
    assert ready.status_code == 200 and ready.json()["database"] == "ok"
    for path in ("/api/v1/system/status", "/api/v1/system/metrics", "/api/v1/markets"):
        response = await api.get(path)
        assert response.status_code == 200
        _walk(response.json())
    missing = await api.get("/api/v1/markets/does-not-exist")
    assert missing.status_code == 404
    assert missing.json() == {"error": "not_found"}
    invalid = await api.get("/api/v1/markets", params={"limit": 0})
    assert invalid.status_code == 422
    assert invalid.json() == {"error": "invalid_request"}
    anonymous = await api.post("/api/v1/replay/sessions/missing/start")
    assert anonymous.status_code == 401
    assert anonymous.json() == {"error": "unauthorized"}


async def test_market_orderbook_and_replay_viewers_use_the_database(
    db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = _one_market()
    fees = fee_calculator()
    store = await _open(db, catalog, [], fees, "ses-api")
    try:
        feed = Feed()
        entries = [
            JournalEntry(ordinal=0, message=feed.msg(snap("A", 1, yes=[("0.40", "10")]))),
            JournalEntry(ordinal=1, message=feed.msg(snap("A", 2, yes=[("0.55", "4")]))),
        ]
        batch = BatchJournal(
            store.sessions,
            "ses-api",
            VersionStamp(fee_schedule_version="fee", relationship_version="rel", rules_hashes={}),
            batch_size=20,
            enabled=True,
        )
        for entry in entries:
            batch.append(entry)
        await batch.flush()
        async for client in _client(db, monkeypatch):
            listed = await client.get("/api/v1/markets")
            assert listed.status_code == 200
            body = listed.json()
            assert body["total"] == 1
            assert body["markets"][0]["market_id"] == "A"
            _walk(body)
            detail = await client.get("/api/v1/markets/A")
            book = await client.get("/api/v1/markets/A/orderbook")
            assert detail.status_code == 200
            assert book.status_code == 200
            orderbook = book.json()["orderbook"]
            assert orderbook["yes_bids"][0]["price"] == "0.55"
            assert orderbook["yes_bids"][0]["quantity"] == "4"
            assert isinstance(orderbook["yes_bids"][0]["price"], str)
            _walk(orderbook)
            sessions = await client.get("/api/v1/replay/sessions")
            assert sessions.json()["sessions"][0]["session_id"] == "ses-api"
            headers = {"X-Replay-Token": _TOKEN}
            first = await client.post("/api/v1/replay/sessions/ses-api/start", headers=headers)
            second = await client.post("/api/v1/replay/sessions/ses-api/start", headers=headers)
            assert first.status_code == 200 and second.status_code == 200
            first_id = first.json()["replay_id"]
            second_id = second.json()["replay_id"]
            assert first_id != second_id
            assert first_id != "ses-api"
            early = await client.post(
                f"/api/v1/replay/sessions/{first_id}/seek",
                headers=headers,
                json={"timestamp_ms": 0},
            )
            late = await client.post(
                f"/api/v1/replay/sessions/{second_id}/seek",
                headers=headers,
                json={"timestamp_ms": 9_000_000_000_000},
            )
            assert early.status_code == 200 and late.status_code == 200
            assert early.json()["cursor"] == 0
            assert late.json()["cursor"] == 2
            assert early.json()["cursor"] != late.json()["cursor"]
            stepped = await client.post(
                f"/api/v1/replay/sessions/{first_id}/step",
                headers=headers,
            )
            assert stepped.status_code == 200
            assert stepped.json()["cursor"] == 1
            again = await client.get(f"/api/v1/replay/sessions/{second_id}")
            assert again.json()["cursor"] == 2
            paused = await client.post(f"/api/v1/replay/sessions/{first_id}/pause", headers=headers)
            assert paused.status_code == 200
            _walk(early.json())
    finally:
        await store.aclose()


async def test_detection_proof_matches_the_stored_certificate(
    db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog, relationships, _unused = _messages()
    fees = fee_calculator()
    store = PersistenceStore(db)
    opened = await store.open(
        preferred_session_id="ses-proof",
        fingerprint="sha256:proof",
        deterministic=True,
        source_label="unit",
        source_kind="synthetic",
        catalog=catalog,
        relationships=relationships,
        fees=fees,
        persist_raw=True,
        pinned=False,
        batch_size=10,
        config_document={"test": "proof"},
    )
    try:
        harness = Harness()
        harness.open_books()
        harness.heartbeats(20, dt=100)
        event = next(ev for ev in harness.events if ev.kind is EventKind.OPENED)
        event = event.model_copy(
            update={"record": event.record.model_copy(update={"session_id": opened.session_id})}
        )
        await opened.sink.write([event])
        async for client in _client(db, monkeypatch):
            listed = await client.get("/api/v1/detections")
            assert listed.status_code == 200
            detections = listed.json()["detections"]
            assert detections
            detection_id = detections[0]["detection_id"]
            detail = await client.get(f"/api/v1/detections/{detection_id}")
            proof = await client.get(f"/api/v1/detections/{detection_id}/proof")
            assert detail.status_code == 200 and proof.status_code == 200
            assert proof.json()["certificate_hash"] == detail.json()["certificate_hash"]
            assert proof.json()["certificate"]["certificate_version"] == "proof-certificate/2"
            _walk(proof.json())
            rels = await client.get("/api/v1/relationships")
            assert rels.status_code == 200
            relationship_id = rels.json()["relationships"][0]["relationship_id"]
            one = await client.get(f"/api/v1/relationships/{relationship_id}")
            assert one.status_code == 200
            assert one.json()["relationship_id"] == relationship_id
            metrics = await client.get("/api/v1/system/metrics")
            assert metrics.json()["detections_total"] >= 1
            assert metrics.json()["markets_monitored"] >= 1
    finally:
        await store.aclose()


async def _pull_sse(
    app: Any,
    headers: list[tuple[bytes, bytes]],
    on_chunk: Callable[[str], Any],
) -> tuple[int, list[tuple[bytes, bytes]], str]:
    """Read SSE until ``on_chunk`` raises ``CancelledError``.

    httpx's ASGI transport buffers until the body ends, which an open stream never does.
    """
    chunks: list[bytes] = []
    status = 0
    response_headers: list[tuple[bytes, bytes]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        nonlocal status
        if message["type"] == "http.response.start":
            status = int(message["status"])
            raw_headers = message.get("headers", [])
            if isinstance(raw_headers, list):
                response_headers.extend(raw_headers)
            return
        if message["type"] != "http.response.body":
            return
        body = message.get("body", b"")
        if isinstance(body, bytes) and body:
            chunks.append(body)
        await on_chunk(b"".join(chunks).decode())

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.5"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/api/v1/stream",
        "raw_path": b"/api/v1/stream",
        "query_string": b"",
        "headers": headers,
        "client": ("127.0.0.1", 50000),
        "server": ("test", 80),
    }
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(app(scope, receive, send), timeout=5)
    return status, response_headers, b"".join(chunks).decode()


async def test_stream_replays_the_boundary_event_on_reconnect(
    db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    monkeypatch.setenv("DATABASE_URL", db)
    monkeypatch.setenv("REPLAY_API_TOKEN", _TOKEN)
    monkeypatch.setenv("REPLAY_MUTATIONS_PUBLIC", "false")
    app = create_app(Settings(), sessions=maker, replay=ReplayHost(), books=BookTail())
    insert = text(
        "INSERT INTO notification_outbox (topic, session_id, payload, created_at) "
        "VALUES ('detection', NULL, CAST(:payload AS jsonb), CURRENT_TIMESTAMP)"
    )

    async def publish(payload: str) -> None:
        async with maker() as session, session.begin():
            await session.execute(insert, {"payload": payload})

    published = False

    async def after_resync(text: str) -> None:
        nonlocal published
        if not published and any(frame.get("event") == "resync" for frame in _frames(text)):
            published = True
            await publish('{"seq":1}')
        if any(frame.get("event") == "detection" for frame in _frames(text)):
            raise asyncio.CancelledError

    status, headers, first_text = await _pull_sse(app, [], after_resync)
    assert status == 200
    content_type = dict(headers).get(b"content-type", b"")
    assert content_type.startswith(b"text/event-stream")
    seen = [frame for frame in _frames(first_text) if frame.get("event") == "detection"]
    assert seen and seen[0]["id"]
    boundary = str(seen[-1]["id"])
    published_next = False

    async def after_boundary(text: str) -> None:
        nonlocal published_next
        frames = [frame for frame in _frames(text) if frame.get("event") == "detection"]
        if frames and frames[0].get("id") == boundary and not published_next:
            published_next = True
            await publish('{"seq":2}')
        if len(frames) >= 2:
            raise asyncio.CancelledError

    status, _headers, second_text = await _pull_sse(
        app,
        [(b"last-event-id", boundary.encode())],
        after_boundary,
    )
    assert status == 200
    replayed = [frame for frame in _frames(second_text) if frame.get("event") == "detection"]
    assert replayed[0]["id"] == boundary
    applied: dict[str, object] = {}
    for frame in _frames(first_text) + replayed:
        if frame.get("event") == "detection" and isinstance(frame.get("id"), str):
            applied[str(frame["id"])] = frame.get("data")
    assert boundary in applied
    assert len(applied) == 2
    sequences = []
    for data in applied.values():
        if isinstance(data, dict) and isinstance(data.get("payload"), dict):
            sequences.append(data["payload"]["seq"])
    assert sorted(sequences) == [1, 2]
    await engine.dispose()

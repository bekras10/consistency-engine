"""An uncertified detection is not served as a verified proof."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from consistency_api import routes
from consistency_api.main import create_app
from consistency_connectors.settings import Settings
from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.replay_host import BookTail, ReplayHost
from tests.integration.test_phase11_api import _client, _frames, _pull_sse

pytestmark = pytest.mark.integration

_DETECTION = "det-uncertified-proof"
_SESSION = "ses-uncertified-proof"
_STORED_HASH = "sha256:" + "ab" * 32
_STORED_JSON = '{"classification":"FEE_ADJUSTED_CANDIDATE","note":"not the hashed proof"}'


async def _seed(db: str) -> None:
    now = datetime.now(UTC)
    engine = make_engine(db)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO data_sources (data_source_id, name, kind, created_at)
                VALUES ('src-uncertified', 'uncertified', 'synthetic', :now)
                """
            ),
            {"now": now},
        )
        await conn.execute(
            text(
                """
                INSERT INTO ingestion_sessions (
                    session_id, data_source_id, status, fingerprint, source_label,
                    deterministic, pinned, raw_persisted, started_at
                ) VALUES (
                    :sid, 'src-uncertified', 'completed', 'sha256:uncertified',
                    'synthetic:uncertified', true, false, false, :now
                )
                """
            ),
            {"sid": _SESSION, "now": now},
        )
        await conn.execute(
            text(
                """
                INSERT INTO detections (
                    detection_id, session_id, relationship_id, strategy_id, template,
                    ordinal, status, classification, peak_classification, reason_codes,
                    first_observed_at, last_observed_at, first_observed_ms, last_observed_ms,
                    first_position, last_position, event_count, certificate_hash,
                    certificate_json, proof_status, record_json
                ) VALUES (
                    :detection_id, :sid, 'rel-uncertified', 'strat-uncertified', 'implication',
                    1, 'EXPIRED', 'FEE_ADJUSTED_CANDIDATE', 'FEE_ADJUSTED_CANDIDATE',
                    CAST('[]' AS jsonb),
                    :now, :now, 1, 2, 1, 2, 1, :cert_hash, :cert_json, 'uncertified',
                    CAST('{"detection_id":"det-uncertified-proof"}' AS jsonb)
                )
                """
            ),
            {
                "detection_id": _DETECTION,
                "sid": _SESSION,
                "now": now,
                "cert_hash": _STORED_HASH,
                "cert_json": _STORED_JSON,
            },
        )
    await engine.dispose()


async def test_uncertified_proof_is_refused_on_the_proof_list_and_resync(
    db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _seed(db)
    async for client in _client(db, monkeypatch):
        listed = await client.get("/api/v1/detections")
        assert listed.status_code == 200
        rows = listed.json()["detections"]
        assert rows
        row = next(item for item in rows if item["detection_id"] == _DETECTION)
        assert row["proof_status"] == "uncertified"
        assert "certificate" not in row

        proof = await client.get(f"/api/v1/detections/{_DETECTION}/proof")
        assert proof.status_code == 403
        body = proof.json()
        assert body == {"error": "uncertified"}
        assert "certificate" not in body
        assert "legs" not in body
        assert "scenarios" not in body
        assert _STORED_JSON not in proof.text

        detail = await client.get(f"/api/v1/detections/{_DETECTION}")
        assert detail.status_code == 200
        assert detail.json()["proof_status"] == "uncertified"
        assert detail.json()["certificate_hash"] == _STORED_HASH

    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    try:
        async with maker() as session:
            _watermark, payload = await routes._resync(session)
        detections = payload["detections"]
        assert isinstance(detections, list)
        snap = next(item for item in detections if item["detection_id"] == _DETECTION)
        assert isinstance(snap, dict)
        assert snap["proof_status"] == "uncertified"

        monkeypatch.setenv("DATABASE_URL", db)
        monkeypatch.setenv("SSE_HEARTBEAT_SECONDS", "30")
        app = create_app(Settings(), sessions=maker, replay=ReplayHost(), books=BookTail())

        async def stop_on_resync(body: str) -> None:
            if any(frame.get("event") == "resync" for frame in _frames(body)):
                raise asyncio.CancelledError

        _status, _headers, first = await _pull_sse(app, [], stop_on_resync)
        opened = next(frame for frame in _frames(first) if frame.get("event") == "resync")
        opened_data = opened["data"]
        assert isinstance(opened_data, dict)
        opened_rows = opened_data["detections"]
        assert isinstance(opened_rows, list)
        streamed = next(item for item in opened_rows if item["detection_id"] == _DETECTION)
        assert isinstance(streamed, dict)
        assert streamed["proof_status"] == "uncertified"
        assert "certificate" not in streamed
    finally:
        await engine.dispose()

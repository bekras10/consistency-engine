"""Stored proofs omit ask ladders when the Kalshi raw-retention gate is closed."""

from __future__ import annotations

import hashlib
import json

import pytest
from sqlalchemy import func, select

from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.recording import DetectionWriter
from consistency_persistence.replay_host import _load_playback
from consistency_persistence.schema import (
    DataSourceRow,
    DetectionRow,
    IngestionSessionRow,
    NotificationOutboxRow,
    OrderbookUpdateRow,
)
from consistency_persistence.timeutil import now_utc
from tests.unit.test_detection_pipeline import Harness

pytestmark = pytest.mark.integration


async def test_disabled_raw_retention_stores_no_ask_ladder_and_blocks_replay(db: str) -> None:
    harness = Harness()
    harness.open_books()
    events = harness.heartbeats(30)
    assert events
    original = events[0].record.certificate_json
    assert original is not None
    assert "asks" in original
    engine = make_engine(db)
    sessions = make_sessionmaker(engine)
    async with sessions() as session, session.begin():
        session.add(
            DataSourceRow(
                data_source_id="src-kalshi",
                name="kalshi",
                kind="kalshi_authorized",
                created_at=now_utc(),
            )
        )
        session.add(
            IngestionSessionRow(
                session_id="unit",
                data_source_id="src-kalshi",
                status="running",
                fingerprint="fp",
                source_label="kalshi:production",
                deterministic=False,
                pinned=False,
                raw_persisted=False,
                started_at=now_utc(),
            )
        )
    writer = DetectionWriter(sessions, retain_quote_levels=False)
    await writer.write(events)
    async with sessions() as session:
        row = await session.get(DetectionRow, events[0].detection_id)
        assert row is not None
        assert row.certificate_json is not None
        body = json.loads(row.certificate_json)
        books = body["books"]
        assert books
        assert books[0]["asks"] == []
        assert body["top_of_book"]["best_asks"]
        assert "Derived evidence profile" in " ".join(body["notes"])
        assert row.certificate_hash == (
            "sha256:" + hashlib.sha256(row.certificate_json.encode()).hexdigest()
        )
        updates = await session.scalar(select(func.count()).select_from(OrderbookUpdateRow))
        assert updates == 0
        outbox = (await session.execute(select(NotificationOutboxRow).limit(1))).scalar_one()
        payload = outbox.payload
        detection = payload["detection"]
        assert isinstance(detection, dict)
        stored = json.loads(str(detection["certificate_json"]))
        assert stored["books"][0]["asks"] == []
        with pytest.raises(ValueError, match="no stored journal"):
            await _load_playback(session, "unit")
    await engine.dispose()

"""SSE resync uses one snapshot boundary, including a backlog above 10_000."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from consistency_api import routes
from consistency_api.main import create_app
from consistency_connectors.settings import Settings
from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.replay_host import BookTail, ReplayHost
from consistency_persistence.store import PersistenceStore
from tests.golden.support import fee_calculator
from tests.integration.test_phase11_api import _frames, _pull_sse
from tests.integration.test_postgres_pipeline import _messages

pytestmark = pytest.mark.integration


async def _seed_detection(db: str, session_id: str) -> None:
    catalog, relationships, _unused = _messages()
    store = PersistenceStore(db)
    await store.open(
        preferred_session_id=session_id,
        fingerprint=f"sha256:{session_id}",
        deterministic=True,
        source_label="unit",
        source_kind="synthetic",
        catalog=catalog,
        relationships=relationships,
        fees=fee_calculator(),
        persist_raw=True,
        pinned=False,
        batch_size=10,
        config_document={"test": session_id},
    )
    async with store.sessions() as session, session.begin():
        await session.execute(
            text(
                """
                INSERT INTO detections (
                    detection_id, session_id, relationship_id, strategy_id, template,
                    ordinal, status, classification, peak_classification, reason_codes,
                    first_observed_at, last_observed_at, first_observed_ms, last_observed_ms,
                    first_position, last_position, event_count, certificate_hash, record_json
                ) VALUES (
                    :detection_id, :session_id, 'rel', 'strategy', 'implication',
                    0, '1', '1', '1', '[]'::jsonb,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0, 0,
                    0, 0, 1, 'sha256:seed', '{}'::jsonb
                )
                """
            ),
            {"detection_id": f"det-{session_id}", "session_id": session_id},
        )
    await store.aclose()


async def _insert_versions(db: str, count: int) -> list[int]:
    engine = make_engine(db)
    try:
        async with engine.begin() as conn:
            rows = (
                await conn.execute(
                    text(
                        """
                        INSERT INTO notification_outbox (topic, session_id, payload, created_at)
                        SELECT 'detection', NULL,
                               jsonb_build_object('version', g),
                               CURRENT_TIMESTAMP
                        FROM generate_series(1, :count) AS g
                        RETURNING id
                        """
                    ),
                    {"count": count},
                )
            ).scalars()
        return [int(item) for item in rows]
    finally:
        await engine.dispose()


def _status(payload: dict[str, object]) -> str:
    detections = payload["detections"]
    assert isinstance(detections, list) and detections
    first = detections[0]
    assert isinstance(first, dict)
    return str(first["status"])


async def test_resync_backlog_above_10000_matches_detection_snapshot(
    db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _seed_detection(db, "ses-resync")
    ids = await _insert_versions(db, 10_001)
    assert len(ids) == 10_001
    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    try:
        async with maker() as session, session.begin():
            await session.execute(
                text("UPDATE detections SET status = '10001', classification = '10001'")
            )
        async with maker() as session:
            watermark, payload = await routes._resync(session)
        assert watermark == ids[-1]
        assert payload["outbox_id"] == ids[-1]
        assert _status(payload) == "10001"
        async with maker() as session:
            version = (
                await session.execute(
                    text("SELECT payload->>'version' FROM notification_outbox WHERE id = :id"),
                    {"id": watermark},
                )
            ).scalar_one()
        assert version == "10001"
        assert version == _status(payload)

        monkeypatch.setenv("DATABASE_URL", db)
        monkeypatch.setenv("SSE_HEARTBEAT_SECONDS", "30")
        app = create_app(Settings(), sessions=maker, replay=ReplayHost(), books=BookTail())

        async def stop_on_resync(body: str) -> None:
            if any(frame.get("event") == "resync" for frame in _frames(body)):
                raise asyncio.CancelledError

        _status_code, _headers, first = await _pull_sse(app, [], stop_on_resync)
        opened = next(frame for frame in _frames(first) if frame.get("event") == "resync")
        opened_data = opened["data"]
        assert isinstance(opened_data, dict)
        assert opened_data["outbox_id"] == ids[-1]
        opened_rows = opened_data["detections"]
        assert isinstance(opened_rows, list) and isinstance(opened_rows[0], dict)
        assert opened_rows[0]["status"] == "10001"
        boundary = str(ids[-1])
        published = False

        async def stop_on_boundary(body: str) -> None:
            nonlocal published
            frames = [frame for frame in _frames(body) if frame.get("event") == "detection"]
            if frames and frames[0].get("id") == boundary and not published:
                published = True
                async with maker() as session, session.begin():
                    await session.execute(
                        text(
                            """
                            INSERT INTO notification_outbox
                                (topic, session_id, payload, created_at)
                            VALUES (
                                'detection', NULL, CAST(:payload AS jsonb), CURRENT_TIMESTAMP
                            )
                            """
                        ),
                        {"payload": '{"version": 10002}'},
                    )
            versions = [
                str(frame["data"]["payload"]["version"])
                for frame in frames
                if isinstance(frame.get("data"), dict)
                and isinstance(frame["data"].get("payload"), dict)
            ]
            if "10002" in versions:
                raise asyncio.CancelledError

        _status_code, _headers, second = await _pull_sse(
            app, [(b"last-event-id", boundary.encode())], stop_on_boundary
        )
        replayed = [frame for frame in _frames(second) if frame.get("event") == "detection"]
        assert replayed[0]["id"] == boundary
        versions = [
            int(str(frame["data"]["payload"]["version"]))
            for frame in replayed
            if isinstance(frame.get("data"), dict)
            and isinstance(frame["data"].get("payload"), dict)
        ]
        assert versions[0] == 10001
        assert 10002 in versions
        assert versions == sorted(versions)
    finally:
        await engine.dispose()


async def test_commit_during_resync_cannot_pair_a_new_snapshot_with_an_old_watermark(
    db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _seed_detection(db, "ses-race")
    ids = await _insert_versions(db, 3)
    engine = make_engine(db)
    maker = make_sessionmaker(engine)
    started = asyncio.Event()
    release = asyncio.Event()
    original = routes._detection_snapshot

    async def delayed(session: AsyncSession) -> list[dict[str, object]]:
        started.set()
        await release.wait()
        return await original(session)

    monkeypatch.setattr(routes, "_detection_snapshot", delayed)
    try:
        async with maker() as session, session.begin():
            await session.execute(text("UPDATE detections SET status = '3', classification = '3'"))

        async def run() -> tuple[int, dict[str, object]]:
            async with maker() as session:
                return await routes._resync(session)

        task = asyncio.create_task(run())
        await asyncio.wait_for(started.wait(), 5)
        async with maker() as session, session.begin():
            await session.execute(
                text(
                    """
                    INSERT INTO notification_outbox (topic, session_id, payload, created_at)
                    VALUES (
                        'detection', NULL, CAST(:payload AS jsonb), CURRENT_TIMESTAMP
                    )
                    """
                ),
                {"payload": '{"version": 4}'},
            )
            await session.execute(text("UPDATE detections SET status = '4', classification = '4'"))
        release.set()
        watermark, payload = await task
        async with maker() as session:
            version = (
                await session.execute(
                    text("SELECT payload->>'version' FROM notification_outbox WHERE id = :id"),
                    {"id": watermark},
                )
            ).scalar_one()
        assert _status(payload) == version
        assert watermark in {ids[-1], ids[-1] + 1}
        # A later read sees the commit. Replaying it cannot walk the status backwards.
        async with maker() as session:
            later, later_payload = await routes._resync(session)
        assert later >= watermark
        assert int(_status(later_payload)) >= int(_status(payload))
    finally:
        await engine.dispose()

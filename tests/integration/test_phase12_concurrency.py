"""Failed detection commits roll back; a retry and overlapping writes leave one row each."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import func, select, text

from consistency_persistence.schema import DetectionLegRow, DetectionRow, NotificationOutboxRow
from consistency_persistence.store import PersistenceStore
from consistency_pipeline.lifecycle import EventKind
from tests.golden.support import fee_calculator
from tests.integration.test_postgres_pipeline import _messages
from tests.unit.test_detection_pipeline import Harness

pytestmark = pytest.mark.integration


async def _opened(db: str, session_id: str) -> tuple[PersistenceStore, object]:
    catalog, relationships, _unused = _messages()
    store = PersistenceStore(db)
    opened = await store.open(
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
    return store, opened


async def test_failed_commit_retries_once_and_concurrent_writes_do_not_orphan(
    db: str,
) -> None:
    store, opened = await _opened(db, "ses-conc")
    try:
        harness = Harness()
        harness.open_books()
        harness.heartbeats(20, dt=100)
        event = next(ev for ev in harness.events if ev.kind is EventKind.OPENED)
        event = event.model_copy(
            update={"record": event.record.model_copy(update={"session_id": opened.session_id})}
        )
        opened.sink.fault = RuntimeError("commit failed")
        with pytest.raises(RuntimeError, match="commit failed"):
            await opened.sink.write([event])
        async with store.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(DetectionRow)) == 0
            assert (
                await session.scalar(select(func.count()).select_from(NotificationOutboxRow)) == 0
            )

        opened.sink.fault = None
        await opened.sink.write([event])
        await opened.sink.write([event])
        async with store.sessions() as session:
            rows = (await session.execute(select(DetectionRow))).scalars().all()
        assert len(rows) == 1
        assert rows[0].detection_id == event.detection_id

        other = event.model_copy(
            update={
                "detection_id": event.detection_id + "-b",
                "record": event.record.model_copy(
                    update={
                        "detection_id": event.detection_id + "-b",
                        "session_id": opened.session_id,
                    }
                ),
            }
        )
        await asyncio.gather(opened.sink.write([event]), opened.sink.write([other]))
        async with store.sessions() as session:
            ids = set((await session.execute(select(DetectionRow.detection_id))).scalars().all())
            legs = (await session.execute(select(DetectionLegRow.detection_id))).scalars().all()
            orphans = await session.scalar(
                text(
                    """
                    SELECT count(*)
                    FROM detection_legs AS leg
                    LEFT JOIN detections AS det ON det.detection_id = leg.detection_id
                    WHERE det.detection_id IS NULL
                    """
                )
            )
        assert ids == {event.detection_id, other.detection_id}
        assert set(legs) <= ids
        assert int(orphans or 0) == 0
    finally:
        await store.aclose()

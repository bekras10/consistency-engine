"""A qualifying detection is not stored unless its certificate bytes match the hash.

Missing JSON and a hash that does not match the stored bytes both abort the
write. The transaction rolls back, so a later failure does not leave the earlier
detection row from the same batch, and a bad update does not replace a matching pair.
"""

from __future__ import annotations

import pytest
from sqlalchemy import func, select

from consistency_core.models.detection import Classification
from consistency_persistence.dashboard import DetectionQuery, list_detections
from consistency_persistence.recording import DetectionWriter
from consistency_persistence.schema import DetectionRow, NotificationOutboxRow
from consistency_persistence.store import PersistenceStore
from consistency_pipeline.lifecycle import DetectionEvent, EventKind
from consistency_worker.bootstrap import fee_calculator
from tests.conftest import FIXTURES
from tests.integration.test_certificate_close_integrity import _digest, _retarget
from tests.integration.test_postgres_pipeline import _with_reference
from tests.unit.test_detection_pipeline import Harness, _candidate

pytestmark = pytest.mark.integration


async def _session(db: str, name: str) -> tuple[PersistenceStore, str, DetectionWriter]:
    harness = Harness()
    store = PersistenceStore(db)
    opened = await store.open(
        preferred_session_id=f"ses-proof-{name}",
        fingerprint=f"sha256:proof-{name}",
        deterministic=True,
        source_label="unit",
        source_kind="synthetic",
        catalog=_with_reference(harness.catalog),
        relationships=harness.rels,
        fees=fee_calculator(FIXTURES / "fees"),
        persist_raw=False,
        pinned=False,
        batch_size=10,
        config_document={"test": name},
    )
    assert isinstance(opened.sink, DetectionWriter)
    return store, opened.session_id, opened.sink


def _events() -> tuple[DetectionEvent, DetectionEvent]:
    harness = Harness()
    _candidate(harness)
    opened = next(event for event in harness.events if event.kind is EventKind.OPENED)
    updated = next(event for event in harness.events if event.kind is EventKind.UPDATED)
    assert opened.record.certificate_json is not None
    assert updated.record.certificate_json is not None
    assert opened.record.detection_id == updated.record.detection_id
    return opened, updated


async def _counts(store: PersistenceStore, session_id: str) -> tuple[int, int]:
    async with store.sessions() as session:
        detections = await session.scalar(
            select(func.count())
            .select_from(DetectionRow)
            .where(DetectionRow.session_id == session_id)
        )
        outbox = await session.scalar(
            select(func.count())
            .select_from(NotificationOutboxRow)
            .where(NotificationOutboxRow.session_id == session_id)
        )
    return int(detections or 0), int(outbox or 0)


async def test_missing_certificate_json_is_not_persisted(db: str) -> None:
    opened, _updated = _events()
    broken = opened.model_copy(
        update={"record": opened.record.model_copy(update={"certificate_json": None})}
    )
    store, session_id, sink = await _session(db, "missing")
    try:
        with pytest.raises(ValueError, match="certificate JSON"):
            await sink.write([_retarget(broken, session_id)])
        detections, outbox = await _counts(store, session_id)
        assert detections == 0
        assert outbox == 0
    finally:
        await store.aclose()


async def test_mismatched_certificate_json_is_not_persisted(db: str) -> None:
    opened, _updated = _events()
    assert opened.record.certificate_json is not None
    corrupted = opened.record.certificate_json.replace("FEE_ADJUSTED_CANDIDATE", "NOT_A_CLASS", 1)
    if corrupted == opened.record.certificate_json:
        corrupted = opened.record.certificate_json + " "
    assert _digest(corrupted) != opened.record.certificate_hash
    broken = opened.model_copy(
        update={"record": opened.record.model_copy(update={"certificate_json": corrupted})}
    )
    store, session_id, sink = await _session(db, "corrupt")
    try:
        with pytest.raises(ValueError, match="certificate hash"):
            await sink.write([_retarget(broken, session_id)])
        detections, outbox = await _counts(store, session_id)
        assert detections == 0
        assert outbox == 0
    finally:
        await store.aclose()


async def test_corrupt_event_rolls_back_the_detection_transaction(db: str) -> None:
    opened, updated = _events()
    assert updated.record.certificate_json is not None
    corrupted = updated.record.certificate_json + " "
    bad_update = updated.model_copy(
        update={"record": updated.record.model_copy(update={"certificate_json": corrupted})}
    )
    store, session_id, sink = await _session(db, "rollback")
    try:
        with pytest.raises(ValueError, match="certificate hash"):
            await sink.write([_retarget(opened, session_id), _retarget(bad_update, session_id)])
        detections, outbox = await _counts(store, session_id)
        assert detections == 0
        assert outbox == 0
    finally:
        await store.aclose()


async def test_update_without_json_does_not_leave_a_mismatched_hash(db: str) -> None:
    opened, updated = _events()
    assert opened.record.certificate_json is not None
    assert updated.record.certificate_hash != opened.record.certificate_hash
    missing = updated.model_copy(
        update={"record": updated.record.model_copy(update={"certificate_json": None})}
    )
    store, session_id, sink = await _session(db, "update-missing")
    try:
        await sink.write([_retarget(opened, session_id)])
        with pytest.raises(ValueError, match="certificate JSON"):
            await sink.write([_retarget(missing, session_id)])
        rows = await store.list_detection_rows(session_id)
        assert len(rows) == 1
        assert rows[0].certificate_json == opened.record.certificate_json
        assert rows[0].certificate_hash == opened.record.certificate_hash
        assert _digest(rows[0].certificate_json or "") == rows[0].certificate_hash
    finally:
        await store.aclose()


async def test_corrupt_update_does_not_replace_a_matching_certificate(db: str) -> None:
    opened, updated = _events()
    assert opened.record.certificate_json is not None
    assert updated.record.certificate_json is not None
    store, session_id, sink = await _session(db, "update")
    try:
        await sink.write([_retarget(opened, session_id)])
        corrupted = updated.record.certificate_json + " "
        bad_update = updated.model_copy(
            update={"record": updated.record.model_copy(update={"certificate_json": corrupted})}
        )
        with pytest.raises(ValueError, match="certificate hash"):
            await sink.write([_retarget(bad_update, session_id)])
        rows = await store.list_detection_rows(session_id)
        assert len(rows) == 1
        assert rows[0].certificate_json == opened.record.certificate_json
        assert rows[0].certificate_hash == opened.record.certificate_hash
        assert _digest(rows[0].certificate_json or "") == rows[0].certificate_hash
    finally:
        await store.aclose()


async def test_classification_facet_lists_classes_outside_the_filter(db: str) -> None:
    opened, _updated = _events()
    other = opened.model_copy(
        update={
            "record": opened.record.model_copy(
                update={
                    "detection_id": f"{opened.record.detection_id}-other",
                    "classification": Classification.THEORETICAL_ONLY,
                }
            )
        }
    )
    store, session_id, sink = await _session(db, "facet")
    try:
        await sink.write([_retarget(opened, session_id), _retarget(other, session_id)])
        async with store.sessions() as session:
            listed = await list_detections(
                session,
                DetectionQuery(classification=opened.record.classification.value),
            )
        shown = {item["detection_id"] for item in listed["detections"]}
        assert shown == {opened.record.detection_id}
        assert set(listed["classifications"]) == {
            opened.record.classification.value,
            Classification.THEORETICAL_ONLY.value,
        }
    finally:
        await store.aclose()

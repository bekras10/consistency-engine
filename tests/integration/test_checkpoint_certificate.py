"""Restoring a checkpoint must put back that detection's own proof certificate."""

from __future__ import annotations

import hashlib

import pytest

from consistency_persistence.schema import DetectionRow
from consistency_persistence.store import PersistenceStore, _reconcile
from consistency_pipeline import checkpoint as cp
from consistency_pipeline.lifecycle import DetectionEvent, EventKind
from consistency_worker.bootstrap import fee_calculator
from tests.conftest import FIXTURES
from tests.integration.test_postgres_pipeline import _with_reference
from tests.unit.test_detection_pipeline import GE2, GE3, Harness, _candidate
from tests.unit.test_ingestion import snap

pytestmark = pytest.mark.integration


def _retarget(event: DetectionEvent, session_id: str) -> DetectionEvent:
    record = event.record.model_copy(update={"session_id": session_id})
    return event.model_copy(update={"record": record})


async def test_reconcile_restores_checkpoint_certificate_not_a_later_one(db: str) -> None:
    harness = Harness()
    _candidate(harness)
    checkpoint_event = harness.events[-1]
    assert checkpoint_event.kind is EventKind.UPDATED
    assert checkpoint_event.evaluation is not None
    original_json = checkpoint_event.evaluation.certificate_json()
    original_hash = checkpoint_event.record.certificate_hash
    assert original_hash == "sha256:" + hashlib.sha256(original_json.encode()).hexdigest()
    checkpoint = cp.take(
        "pending",
        harness.journal[-1].ordinal,
        harness.now,
        None,
        harness.mgr,
        harness.engine,
    )

    assert harness.send(snap(GE3, harness.next_seq(), yes=[("0.70", "300")])) == []
    later = harness.send(snap(GE2, harness.next_seq(), no=[("0.65", "300")]))
    assert later and later[0].evaluation is not None
    later_json = later[0].evaluation.certificate_json()
    assert later_json != original_json
    assert later[0].record.certificate_hash != original_hash

    fees = fee_calculator(FIXTURES / "fees")
    store = PersistenceStore(db)
    opened = await store.open(
        preferred_session_id="ses-cert-restore",
        fingerprint="sha256:cert-restore",
        deterministic=True,
        source_label="unit",
        source_kind="synthetic",
        catalog=_with_reference(harness.catalog),
        relationships=harness.rels,
        fees=fees,
        persist_raw=True,
        pinned=False,
        batch_size=10,
        config_document={"test": "cert-restore"},
    )
    await opened.sink.write(
        [
            _retarget(checkpoint_event, opened.session_id),
            _retarget(later[0], opened.session_id),
        ]
    )
    stale = await store.list_detection_rows(opened.session_id)
    assert len(stale) == 1
    assert stale[0].certificate_json == later_json

    async with store.sessions() as session, session.begin():
        await _reconcile(session, opened.session_id, checkpoint)

    rows = await store.list_detection_rows(opened.session_id)
    assert len(rows) == 1
    row: DetectionRow = rows[0]
    assert row.certificate_json is not None
    assert row.certificate_json == original_json
    assert row.certificate_hash == original_hash
    digest = "sha256:" + hashlib.sha256(row.certificate_json.encode()).hexdigest()
    assert digest == row.certificate_hash
    assert row.certificate_json != later_json
    await store.aclose()

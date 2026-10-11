"""A terminal close must not replace the qualifying certificate with the closing evaluation.

The stored certificate JSON, its hash, walked legs, and scenarios are one proof.
SHA-256 of that JSON is the stored hash. The close is a separate snapshot.
"""

from __future__ import annotations

import hashlib
import json

import pytest
from sqlalchemy import select

from consistency_connectors.ingestion import BookManager
from consistency_core.events import MarketStatusEvent
from consistency_core.models import MarketStatus, Side
from consistency_core.models.detection import Classification
from consistency_persistence.schema import DetectionLegRow, DetectionRow, DetectionScenarioRow
from consistency_persistence.store import PersistenceStore, _reconcile
from consistency_pipeline import checkpoint as cp
from consistency_pipeline.driver import JournalApplier
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.lifecycle import CloseReason, DetectionEvent, EventKind
from consistency_worker.bootstrap import fee_calculator
from tests.conftest import FIXTURES
from tests.integration.test_postgres_pipeline import _with_reference
from tests.unit.test_detection_pipeline import GE2, GE3, Harness, _candidate
from tests.unit.test_ingestion import delta

pytestmark = pytest.mark.integration


def _digest(certificate_json: str) -> str:
    return "sha256:" + hashlib.sha256(certificate_json.encode()).hexdigest()


def _retarget(event: DetectionEvent, session_id: str) -> DetectionEvent:
    record = event.record.model_copy(update={"session_id": session_id})
    return event.model_copy(update={"record": record})


def _close_case(kind: str) -> tuple[Harness, DetectionEvent]:
    harness = Harness()
    _candidate(harness)
    if kind == "EXPIRED":
        closed = harness.send(MarketStatusEvent(market_id=GE2, status=MarketStatus.CLOSED))
        assert closed[0].close_reason is CloseReason.MARKET_NOT_OPEN
    elif kind == "RESOLVED":
        assert harness.send(delta(GE3, harness.next_seq(), Side.YES, "0.30", "100")) == []
        closed = harness.send(delta(GE3, harness.next_seq(), Side.YES, "0.70", "-100"))
        assert closed[0].close_reason is CloseReason.NO_PRE_FEE_EDGE
    elif kind == "INVALIDATED":
        harness.seq["1"] += 1
        closed = harness.send(delta(GE3, harness.next_seq(), Side.YES, "0.71", "1"))
        assert closed[0].close_reason is CloseReason.BOOK_UNSYNCHRONIZED
    else:
        raise AssertionError(kind)
    assert closed[0].kind.value == kind
    assert closed[0].evaluation is not None
    assert closed[0].record.classification is Classification.FEE_ADJUSTED_CANDIDATE
    assert closed[0].evaluation.classification is not Classification.FEE_ADJUSTED_CANDIDATE
    assert closed[0].record.certificate_json is not None
    assert _digest(closed[0].record.certificate_json) == closed[0].record.certificate_hash
    assert closed[0].evaluation.certificate_json() != closed[0].record.certificate_json
    return harness, closed[0]


async def _persist(db: str, name: str, events: list[DetectionEvent]) -> PersistenceStore:
    harness = Harness()
    fees = fee_calculator(FIXTURES / "fees")
    store = PersistenceStore(db)
    opened = await store.open(
        preferred_session_id=f"ses-cert-{name}",
        fingerprint=f"sha256:cert-{name}",
        deterministic=True,
        source_label="unit",
        source_kind="synthetic",
        catalog=_with_reference(harness.catalog),
        relationships=harness.rels,
        fees=fees,
        persist_raw=True,
        pinned=False,
        batch_size=10,
        config_document={"test": name},
    )
    await opened.sink.write([_retarget(event, opened.session_id) for event in events])
    return store


def _proof(row: DetectionRow, qualifying_json: str, qualifying_hash: str) -> None:
    assert row.certificate_json == qualifying_json
    assert row.certificate_hash == qualifying_hash
    assert row.certificate_json is not None
    assert _digest(row.certificate_json) == row.certificate_hash
    body = json.loads(row.certificate_json)
    assert body["classification"] == "FEE_ADJUSTED_CANDIDATE"


async def test_opened_and_updated_certificates_match_their_hashes(db: str) -> None:
    harness = Harness()
    _candidate(harness)
    opened = next(event for event in harness.events if event.kind is EventKind.OPENED)
    updated = next(event for event in harness.events if event.kind is EventKind.UPDATED)
    assert opened.classification is Classification.DEPTH_SUPPORTED
    assert updated.classification is Classification.FEE_ADJUSTED_CANDIDATE
    store = await _persist(db, "open-update", [opened, updated])
    try:
        rows = await store.list_detection_rows("ses-cert-open-update")
        assert len(rows) == 1
        row = rows[0]
        assert row.status == "UPDATED"
        assert updated.record.certificate_json is not None
        _proof(row, updated.record.certificate_json, updated.record.certificate_hash)
        assert json.loads(row.certificate_json or "")["classification"] == "FEE_ADJUSTED_CANDIDATE"
        assert row.closing_classification is None
        assert row.closing_certificate_json is None
    finally:
        await store.aclose()


@pytest.mark.parametrize("kind", ["EXPIRED", "RESOLVED", "INVALIDATED"])
async def test_close_keeps_qualifying_certificate_and_stores_the_close(db: str, kind: str) -> None:
    harness, closed = _close_case(kind)
    assert closed.evaluation is not None
    store = await _persist(db, kind.lower(), harness.events)
    try:
        rows = await store.list_detection_rows(f"ses-cert-{kind.lower()}")
        assert len(rows) == 1
        row = rows[0]
        assert row.status == kind
        assert row.classification == "FEE_ADJUSTED_CANDIDATE"
        assert row.close_reason == closed.close_reason.value
        assert closed.record.certificate_json is not None
        _proof(row, closed.record.certificate_json, closed.record.certificate_hash)
        assert row.certificate_json != closed.evaluation.certificate_json()
        assert row.closing_classification == closed.evaluation.classification.value
        assert row.closing_certificate_hash == closed.evaluation.certificate_hash
        assert row.closing_certificate_json == closed.evaluation.certificate_json()
        assert row.closing_certificate_json is not None
        assert _digest(row.closing_certificate_json) == row.closing_certificate_hash
        assert (
            json.loads(row.closing_certificate_json)["classification"] != "FEE_ADJUSTED_CANDIDATE"
        )
        async with store.sessions() as session:
            legs = (
                (
                    await session.execute(
                        select(DetectionLegRow)
                        .where(DetectionLegRow.detection_id == row.detection_id)
                        .order_by(DetectionLegRow.leg_index)
                    )
                )
                .scalars()
                .all()
            )
            scenarios = (
                (
                    await session.execute(
                        select(DetectionScenarioRow).where(
                            DetectionScenarioRow.detection_id == row.detection_id
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert legs
        assert all(leg.quantity is not None and leg.premium is not None for leg in legs)
        assert scenarios
        assert any(scenario.is_worst for scenario in scenarios)
    finally:
        await store.aclose()


async def test_checkpoint_restore_keeps_qualifying_proof_and_close_reason(db: str) -> None:
    harness, closed = _close_case("EXPIRED")
    checkpoint = cp.take(
        "pending",
        harness.journal[-1].ordinal,
        harness.now,
        None,
        harness.mgr,
        harness.engine,
    )
    fees = fee_calculator(FIXTURES / "fees")
    manager = BookManager(harness.catalog.markets, source="unit")
    engine = DetectionEngine(manager, harness.rels, fees, session_id="unit")
    replayed = JournalApplier(manager, engine).apply_all(harness.journal)
    assert replayed[-1].record.certificate_json == closed.record.certificate_json
    assert replayed[-1].record.close_reason is CloseReason.MARKET_NOT_OPEN
    store = await _persist(db, "restore", replayed)
    try:
        session_id = "ses-cert-restore"
        rows = await store.list_detection_rows(session_id)
        assert len(rows) == 1
        assert closed.record.certificate_json is not None
        _proof(rows[0], closed.record.certificate_json, closed.record.certificate_hash)
        assert rows[0].close_reason == "MARKET_NOT_OPEN"
        checkpoint = checkpoint.model_copy(update={"session_id": session_id})
        async with store.sessions() as session, session.begin():
            await _reconcile(session, session_id, checkpoint)
        restored = await store.list_detection_rows(session_id)
        assert len(restored) == 1
        _proof(restored[0], closed.record.certificate_json, closed.record.certificate_hash)
        assert restored[0].close_reason == "MARKET_NOT_OPEN"
        assert restored[0].status == "EXPIRED"
        assert restored[0].closing_classification == "INSUFFICIENT_LIQUIDITY"
        assert restored[0].certificate_json != restored[0].closing_certificate_json
    finally:
        await store.aclose()

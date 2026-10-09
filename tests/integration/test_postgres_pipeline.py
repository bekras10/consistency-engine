"""Real pipeline against PostgreSQL: seed, atomic writes, restart, S1–S8."""

from __future__ import annotations

import asyncio
import hashlib
from collections import Counter

import pytest
from sqlalchemy import select, update

from consistency_connectors.base import ConnectionState
from consistency_connectors.ingestion import BookManager
from consistency_connectors.settings import Settings
from consistency_connectors.sources.recorded import RecordedStreamSource
from consistency_core.events import StreamMessage
from consistency_core.models.common import DataSourceKind
from consistency_core.models.market import Catalog, Event, Series
from consistency_core.models.relationship import Relationship
from consistency_core.pricing.portfolio import parse_strategy_id
from consistency_persistence.recording import load_journal
from consistency_persistence.schema import (
    DetectionLegRow,
    DetectionScenarioRow,
    IngestionSessionRow,
    OrderbookUpdateRow,
)
from consistency_persistence.seed import load_reference
from consistency_persistence.store import PersistenceStore
from consistency_pipeline.driver import JournalApplier
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.lifecycle import EventKind
from consistency_simulation.datasets import load
from consistency_simulation.families import SIM_EPOCH
from consistency_worker.bootstrap import (
    SessionInputs,
    build_inputs,
    fee_calculator,
    relationships_for,
)
from consistency_worker.service import WorkerService
from consistency_worker.store import DatabaseSessionStore
from tests.conftest import FIXTURES
from tests.replay.test_pipeline_ground_truth import EXPECTED_DURATIONS
from tests.unit.test_detection_pipeline import Harness

pytestmark = pytest.mark.integration


def _with_reference(catalog: Catalog) -> Catalog:
    series = {s.series_id: s for s in catalog.series}
    events = {e.event_id: e for e in catalog.events}
    for market in catalog.markets:
        series.setdefault(
            market.series_id,
            Series(
                series_id=market.series_id,
                title=market.series_id,
                category="test",
                provenance=market.provenance,
            ),
        )
        events.setdefault(
            market.event_id,
            Event(
                event_id=market.event_id,
                series_id=market.series_id,
                title=market.event_id,
                provenance=market.provenance,
            ),
        )
    return Catalog(
        series=tuple(series[k] for k in sorted(series)),
        events=tuple(events[k] for k in sorted(events)),
        markets=catalog.markets,
    )


def _messages() -> tuple[Catalog, list[Relationship], list[StreamMessage]]:
    harness = Harness()
    harness.open_books()
    harness.heartbeats(40, dt=100)
    messages = [entry.message for entry in harness.journal if entry.message is not None]
    return _with_reference(harness.catalog), harness.rels, messages


async def test_seed_loads_markets_relationships_reviews_and_fees(db: str) -> None:
    dataset = load(FIXTURES / "datasets/inconsistent")
    relationships = relationships_for(
        dataset.catalog, FIXTURES / "relationships/manual-reviews.yaml", as_of=SIM_EPOCH
    )
    fees = fee_calculator(FIXTURES / "fees")
    counts = await load_reference(
        db,
        dataset.catalog,
        relationships,
        list(fees.registry.schedules),
        source_id="synthetic",
        source_name="synthetic:inconsistent",
        source_kind="synthetic",
    )
    assert counts["markets"] > 0
    assert counts["relationships"] > 0
    assert counts["reviews"] > 0
    assert counts["fee_schedules"] >= 2


async def test_detection_batch_is_atomic(db: str) -> None:
    catalog, relationships, _unused_messages = _messages()
    fees = fee_calculator(FIXTURES / "fees")
    store = PersistenceStore(db)
    opened = await store.open(
        preferred_session_id="ses-atomic",
        fingerprint="sha256:atomic",
        deterministic=True,
        source_label="unit",
        source_kind="synthetic",
        catalog=catalog,
        relationships=relationships,
        fees=fees,
        persist_raw=True,
        pinned=False,
        batch_size=10,
        config_document={"test": "atomic"},
    )
    harness = Harness()
    harness.open_books()
    harness.heartbeats(20, dt=100)
    event = next(ev for ev in harness.events if ev.kind is EventKind.OPENED)
    event = event.model_copy(
        update={"record": event.record.model_copy(update={"session_id": opened.session_id})}
    )
    opened.sink.fault = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        await opened.sink.write([event])
    assert await store.list_detection_rows(opened.session_id) == []
    opened.sink.fault = None
    await opened.sink.write([event])
    rows = await store.list_detection_rows(opened.session_id)
    assert len(rows) == 1
    row = rows[0]
    assert row.certificate_json is not None
    digest = "sha256:" + hashlib.sha256(row.certificate_json.encode()).hexdigest()
    assert digest == row.certificate_hash
    assert row.certificate_version == "proof-certificate/2"
    async with store.sessions() as session:
        legs = (
            await session.execute(
                select(DetectionLegRow).where(DetectionLegRow.detection_id == row.detection_id)
            )
        ).scalars()
        scenarios = (
            await session.execute(
                select(DetectionScenarioRow).where(
                    DetectionScenarioRow.detection_id == row.detection_id
                )
            )
        ).scalars()
    assert list(legs)
    assert list(scenarios)
    await store.aclose()


async def test_restart_continues_lifecycle_without_duplicates(db: str) -> None:
    catalog, relationships, messages = _messages()
    fees = fee_calculator(FIXTURES / "fees")
    settings = Settings(
        DATABASE_URL=db,
        checkpoint_every=15,
        journal_batch_size=8,
        retention_interval_s=3600,
    )
    fingerprint = "sha256:restart-lifecycle"
    gate = asyncio.Event()

    def inputs(source: RecordedStreamSource) -> SessionInputs:
        return SessionInputs(
            source=source,
            catalog=catalog,
            relationships=relationships,
            fees=fees,
            source_label="replay:unit",
            deterministic=True,
            fingerprint=fingerprint,
        )

    class _Gated(RecordedStreamSource):
        def __init__(self) -> None:
            super().__init__(DataSourceKind.REPLAY, catalog, messages, speed=None)

        async def subscribe_orderbooks(self, market_ids: object = None) -> object:
            del market_ids
            self._state = ConnectionState.CONNECTED
            for index, msg in enumerate(self.messages):
                if index == 20:
                    await gate.wait()
                self.position = msg.position
                yield msg
            self._state = ConnectionState.DISCONNECTED

    store = DatabaseSessionStore(settings)
    first = WorkerService(settings, inputs=inputs(_Gated()), store=store)
    stop = asyncio.Event()
    task = asyncio.create_task(first.run(stop))
    await asyncio.wait_for(first.started.wait(), 5)
    while first.listener is None or first.listener.stats.checkpoints < 1:
        if task.done():
            break
        await asyncio.sleep(0.01)
    stop.set()
    gate.set()
    interrupted = await asyncio.wait_for(task, 30)
    assert interrupted.status == "interrupted"

    second_service = WorkerService(
        settings,
        inputs=inputs(RecordedStreamSource(DataSourceKind.REPLAY, catalog, messages, speed=None)),
        store=store,
    )
    sub = second_service.broker.subscribe({"detection"}, maxsize=5_000)
    finished = await asyncio.wait_for(second_service.run(), 30)
    assert finished.status == "completed"
    assert finished.resumed_from is not None
    payloads = []
    while (message := sub.get_nowait()) is not None:
        payloads.append(message.payload)
    assert sub.dropped == 0
    opened = [p for p in payloads if p["event"] == "OPENED"]
    assert len(opened) == len({p["detection"]["strategy_id"] for p in opened})
    rows = await store._inner.list_detection_rows(finished.session_id)
    assert rows
    assert not [row for row in rows if row.status in ("OPEN", "UPDATED")]
    ids = [row.detection_id for row in rows]
    assert len(ids) == len(set(ids))
    async with store._inner.sessions() as session:
        ordinals = (
            await session.execute(
                select(OrderbookUpdateRow.ordinal).where(
                    OrderbookUpdateRow.session_id == finished.session_id
                )
            )
        ).scalars()
    ordinal_list = list(ordinals)
    assert len(ordinal_list) == len(set(ordinal_list))
    assert finished.session_id == interrupted.session_id
    await store.aclose()


async def test_retention_keeps_pinned_demos(db: str) -> None:
    catalog, relationships, _messages_ignored = _messages()
    fees = fee_calculator(FIXTURES / "fees")
    store = PersistenceStore(db)
    for session_id, pinned in (("ses-old", False), ("ses-demo", True)):
        await store.open(
            preferred_session_id=session_id,
            fingerprint=f"sha256:{session_id}",
            deterministic=True,
            source_label="synthetic:inconsistent" if pinned else "synthetic:other",
            source_kind="synthetic",
            catalog=catalog,
            relationships=relationships,
            fees=fees,
            persist_raw=True,
            pinned=pinned,
            batch_size=10,
            config_document={"id": session_id},
        )
    from datetime import UTC, datetime, timedelta

    from consistency_persistence.retention import RetentionPolicy

    async with store.sessions() as session, session.begin():
        await session.execute(
            update(IngestionSessionRow)
            .where(IngestionSessionRow.session_id == "ses-old")
            .values(started_at=datetime.now(UTC) - timedelta(days=30))
        )
    dropped = await store.run_retention(
        RetentionPolicy(max_age_hours=168, max_sessions=20, third_party_hours=0)
    )
    assert dropped == ["ses-old"]
    async with store.sessions() as session:
        flags = dict(
            (
                await session.execute(
                    select(IngestionSessionRow.session_id, IngestionSessionRow.raw_persisted)
                )
            ).all()
        )
    assert flags["ses-demo"] is True
    assert flags["ses-old"] is False
    await store.aclose()


async def test_inconsistent_dataset_classifications_and_lifecycle(db: str) -> None:
    settings = Settings(
        DATABASE_URL=db,
        DATA_SOURCE="replay",
        REPLAY_DATASET_PATH=str(FIXTURES / "datasets/inconsistent"),
        checkpoint_every=5_000,
        journal_batch_size=200,
        retention_interval_s=3600,
    )
    inputs = await build_inputs(settings, fast=True)
    store = DatabaseSessionStore(settings)
    service = WorkerService(settings, inputs=inputs, store=store)
    subscription = service.broker.subscribe({"detection"}, maxsize=20_000)
    result = await asyncio.wait_for(service.run(), 180)
    assert result.status == "completed"
    assert subscription.dropped == 0
    async with store._inner.sessions() as session:
        entries = await load_journal(session, result.session_id)
    assert entries[-1].control is not None and entries[-1].control.action == "session_end"
    fees = inputs.fees
    engine = DetectionEngine(
        BookManager(inputs.catalog.markets, source=inputs.source_label),
        inputs.relationships,
        fees,
        session_id=result.session_id,
    )
    applier = JournalApplier(engine.manager, engine)
    findings = []
    for finding in load(FIXTURES / "datasets/inconsistent").metadata["expected_findings"]:
        members = set(finding["members"])
        hits = [
            rel
            for rel in inputs.relationships
            if rel.relationship_type.value == finding["relationship_type"]
            and set(rel.members) == members
        ]
        findings.append(
            (
                int(finding["evaluate_at_ms"]),
                str(finding["scenario_id"]),
                str(finding["expected_classification"]),
                list(finding["expected_reason_codes"]),
                engine.strategy_key_for(
                    hits[0].relationship_id, parse_strategy_id(str(finding["strategy_id"]))
                ),
            )
        )
    findings.sort()
    events = []
    seen: dict[str, object] = {}

    def read(item: tuple[int, str, str, list[str], str]) -> None:
        at, scenario_id, expected, reasons, key = item
        evaluation = engine.peek(key, at)
        assert evaluation.classification.value == expected, scenario_id
        assert list(evaluation.reason_codes) == reasons, scenario_id
        assert evaluation.certificate.timing.observed_duration_ms == EXPECTED_DURATIONS.get(
            scenario_id
        ), scenario_id
        seen[scenario_id] = evaluation.certificate_hash

    for entry in entries[:-1]:
        while findings and entry.now_ms > findings[0][0]:
            read(findings.pop(0))
        events += applier.apply(entry)
    for item in findings:
        read(item)
    assert len(seen) == 8
    assert dict(Counter(ev.kind.value for ev in events)) == {
        "OPENED": 12,
        "UPDATED": 120,
        "EXPIRED": 7,
        "RESOLVED": 5,
    }
    active: dict[str, str] = {}
    for ev in events:
        key = f"{ev.record.relationship_id}::{ev.record.strategy_id}"
        if ev.kind is EventKind.OPENED:
            assert key not in active
            active[key] = ev.detection_id
        else:
            assert active.get(key) == ev.detection_id
            if not ev.record.status.active:
                del active[key]
    assert active == {}
    closing = applier.apply(entries[-1])
    assert not [ev for ev in closing if ev.kind is EventKind.OPENED]
    events += closing
    rows = {
        row.detection_id: row for row in await store._inner.list_detection_rows(result.session_id)
    }
    by_id: dict[str, object] = {}
    for ev in events:
        by_id[ev.detection_id] = ev
    assert set(rows) == set(by_id)
    for detection_id, ev in by_id.items():
        row = rows[detection_id]
        assert row.status == ev.record.status.value
        assert row.classification == ev.record.classification.value
        assert row.certificate_hash == ev.record.certificate_hash
        assert row.event_count == ev.record.event_count
        assert row.max_deviation == ev.record.max_deviation
        if ev.evaluation is not None:
            assert row.certificate_json == ev.evaluation.certificate_json()
    await store.aclose()

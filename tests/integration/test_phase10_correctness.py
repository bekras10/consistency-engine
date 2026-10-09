"""Phase 10 regressions: journal rollback, historical replay versions, market status."""

from __future__ import annotations

import pytest
from sqlalchemy import update

from consistency_connectors.ingestion import BookManager
from consistency_core.events import MarketStatusEvent
from consistency_core.fees import FeeCalculator, FeeScheduleRegistry
from consistency_core.models import MarketStatus, Side
from consistency_core.models.market import Catalog, Event, Market, Series
from consistency_core.models.relationship import Relationship
from consistency_core.money import dec, dec_str
from consistency_persistence.dashboard import (
    apply_book,
    apply_journal_market,
    get_market_row,
    list_markets,
)
from consistency_persistence.recording import BatchJournal, CheckpointWriter, VersionStamp
from consistency_persistence.reference import (
    upsert_catalog,
    upsert_fee_schedules,
    upsert_relationships,
)
from consistency_persistence.replay_host import BookTail, ReplayHost
from consistency_persistence.schema import MarketRow, OrderbookUpdateRow
from consistency_persistence.store import PersistenceStore, _truncate_after, _versions
from consistency_pipeline.checkpoint import take
from consistency_pipeline.driver import JournalApplier
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.journal import JournalEntry
from consistency_pipeline.playback import PlaybackSession
from tests.factories import market
from tests.golden.support import fee_calculator
from tests.unit.test_detection_pipeline import Harness
from tests.unit.test_ingestion import Feed, delta, snap

pytestmark = pytest.mark.integration


def _referenced(catalog: Catalog) -> Catalog:
    series = {item.series_id: item for item in catalog.series}
    events = {item.event_id: item for item in catalog.events}
    for listed in catalog.markets:
        series.setdefault(
            listed.series_id,
            Series(
                series_id=listed.series_id,
                title=listed.series_id,
                category="test",
                provenance=listed.provenance,
            ),
        )
        events.setdefault(
            listed.event_id,
            Event(
                event_id=listed.event_id,
                series_id=listed.series_id,
                title=listed.event_id,
                provenance=listed.provenance,
            ),
        )
    return Catalog(
        series=tuple(series[key] for key in sorted(series)),
        events=tuple(events[key] for key in sorted(events)),
        markets=catalog.markets,
    )


def _one_market() -> Catalog:
    listed = market("A")
    return _referenced(Catalog(markets=(listed,)))


async def _open(
    db: str,
    catalog: Catalog,
    relationships: list[Relationship],
    fees: FeeCalculator,
    session_id: str,
) -> PersistenceStore:
    store = PersistenceStore(db)
    await store.open(
        preferred_session_id=session_id,
        fingerprint=f"sha256:{session_id}",
        deterministic=True,
        source_label="unit",
        source_kind="synthetic",
        catalog=catalog,
        relationships=relationships,
        fees=fees,
        persist_raw=True,
        pinned=False,
        batch_size=200,
        config_document={"test": session_id},
    )
    return store


def _yes(manager: BookManager, market_id: str = "A") -> list[tuple[str, str]]:
    book = manager.book(market_id)
    return [(dec_str(level.price), dec_str(level.quantity)) for level in book.yes_bids]


def _fingerprint(session: PlaybackSession) -> tuple[str, tuple[tuple[str, str, str], ...]]:
    events = tuple(
        (event.kind.value, event.record.classification.value, event.record.certificate_hash)
        for event in session.outcome.events
    )
    return session.outcome.manager.state_digest(), events


def _memory_fingerprint(
    session_id: str,
    markets: tuple[Market, ...],
    relationships: list[Relationship],
    fees: FeeCalculator,
    entries: list[JournalEntry],
) -> tuple[str, tuple[tuple[str, str, str], ...]]:
    manager = BookManager(markets, source="replay")
    engine = DetectionEngine(manager, relationships, fees, session_id=session_id)
    applier = JournalApplier(manager, engine)
    events = []
    for entry in entries:
        events.extend(applier.apply(entry))
    recorded = tuple(
        (event.kind.value, event.record.classification.value, event.record.certificate_hash)
        for event in events
    )
    return manager.state_digest(), recorded


async def test_book_tail_rebuilds_after_same_session_truncate_and_replay(db: str) -> None:
    catalog = _one_market()
    fees = fee_calculator()
    store = await _open(db, catalog, [], fees, "ses-tail")
    try:
        feed = Feed()
        entries = [
            JournalEntry(
                ordinal=0, message=feed.msg(snap("A", 1, yes=[("0.40", "10"), ("0.30", "4")]))
            ),
            JournalEntry(ordinal=1, message=feed.msg(delta("A", 2, Side.YES, "0.40", "15"))),
            JournalEntry(ordinal=2, message=feed.msg(delta("A", 3, Side.YES, "0.40", "15"))),
        ]
        manager = BookManager(catalog.markets, source="unit")
        engine = DetectionEngine(manager, [], fees, session_id="ses-tail")
        apply_book(manager, entries[0])
        writer = CheckpointWriter(store.sessions)
        await writer.save(
            take(
                "ses-tail",
                entries[0].ordinal,
                entries[0].now_ms,
                None if entries[0].message is None else entries[0].message.position,
                manager,
                engine,
            )
        )
        apply_book(manager, entries[1])
        apply_book(manager, entries[2])
        await writer.save(
            take(
                "ses-tail",
                entries[2].ordinal,
                entries[2].now_ms,
                None if entries[2].message is None else entries[2].message.position,
                manager,
                engine,
            )
        )
        batch = BatchJournal(
            store.sessions,
            "ses-tail",
            VersionStamp(fee_schedule_version="fee", relationship_version="rel", rules_hashes={}),
            batch_size=20,
            enabled=True,
        )
        for entry in entries:
            batch.append(entry)
        await batch.flush()
        tail = BookTail()
        cached = await tail.refresh(store.sessions)
        assert cached is not None
        before = _yes(cached)
        assert before == [("0.40", "40"), ("0.30", "4")]
        async with store.sessions() as session, session.begin():
            await _truncate_after(session, "ses-tail", entries[0].ordinal)
        rewritten = JournalEntry(ordinal=1, message=feed.msg(delta("A", 2, Side.YES, "0.40", "-7")))
        batch.append(rewritten)
        await batch.flush()
        await tail.refresh(store.sessions)
        fresh_manager = await BookTail().refresh(store.sessions)
        assert fresh_manager is not None and tail.manager is not None
        assert _yes(tail.manager) == _yes(fresh_manager)
        assert _yes(tail.manager) != before
        assert _yes(tail.manager) == [("0.40", "3"), ("0.30", "4")]
    finally:
        await store.aclose()


async def test_book_tail_rebuilds_when_cached_ordinal_entry_changes(db: str) -> None:
    catalog = _one_market()
    fees = fee_calculator()
    store = await _open(db, catalog, [], fees, "ses-tail-edit")
    try:
        feed = Feed()
        entries = [
            JournalEntry(ordinal=0, message=feed.msg(snap("A", 1, yes=[("0.40", "10")]))),
            JournalEntry(ordinal=1, message=feed.msg(delta("A", 2, Side.YES, "0.40", "5"))),
            JournalEntry(ordinal=2, message=feed.msg(delta("A", 3, Side.YES, "0.40", "5"))),
        ]
        manager = BookManager(catalog.markets, source="unit")
        engine = DetectionEngine(manager, [], fees, session_id="ses-tail-edit")
        apply_book(manager, entries[0])
        await CheckpointWriter(store.sessions).save(
            take(
                "ses-tail-edit",
                0,
                entries[0].now_ms,
                None if entries[0].message is None else entries[0].message.position,
                manager,
                engine,
            )
        )
        batch = BatchJournal(
            store.sessions,
            "ses-tail-edit",
            VersionStamp(fee_schedule_version="fee", relationship_version="rel", rules_hashes={}),
            batch_size=20,
            enabled=True,
        )
        for entry in entries:
            batch.append(entry)
        await batch.flush()
        tail = BookTail()
        cached = await tail.refresh(store.sessions)
        assert cached is not None
        before = _yes(cached)
        replacement = JournalEntry(
            ordinal=2, message=feed.msg(delta("A", 3, Side.YES, "0.40", "-4"))
        )
        async with store.sessions() as session, session.begin():
            await session.execute(
                update(OrderbookUpdateRow)
                .where(
                    OrderbookUpdateRow.session_id == "ses-tail-edit",
                    OrderbookUpdateRow.ordinal == 2,
                )
                .values(entry_json=replacement.model_dump(mode="json"))
            )
        await tail.refresh(store.sessions)
        fresh = await BookTail().refresh(store.sessions)
        assert fresh is not None and tail.manager is not None
        assert _yes(tail.manager) == _yes(fresh)
        assert _yes(tail.manager) != before
    finally:
        await store.aclose()


async def test_market_status_follows_journal_after_catalog_row(db: str) -> None:
    catalog = _one_market()
    fees = fee_calculator()
    store = await _open(db, catalog, [], fees, "ses-status")
    try:
        feed = Feed()
        entries = [
            JournalEntry(ordinal=0, message=feed.msg(snap("A", 1, yes=[("0.40", "10")]))),
            JournalEntry(
                ordinal=1,
                message=feed.msg(MarketStatusEvent(market_id="A", status=MarketStatus.PAUSED)),
            ),
        ]
        batch = BatchJournal(
            store.sessions,
            "ses-status",
            VersionStamp(fee_schedule_version="fee", relationship_version="rel", rules_hashes={}),
            batch_size=20,
            enabled=True,
        )
        for entry in entries:
            batch.append(entry)
        await batch.flush()
        async with store.sessions() as session, session.begin():
            await session.execute(update(MarketRow).values(status="closed"))
        tail = BookTail()
        manager = await tail.refresh(store.sessions)
        assert manager is not None
        assert manager.market_status("A") is MarketStatus.PAUSED
        async with store.sessions() as session:
            listed = await list_markets(session)
            detail = await get_market_row(session, "A")
        assert detail is not None
        assert detail["status"] == "closed"
        markets = listed["markets"]
        assert isinstance(markets, list)
        for item in markets:
            assert isinstance(item, dict)
            apply_journal_market(item, manager)
        apply_journal_market(detail, manager)
        assert detail["status"] == "paused"
        assert markets[0]["status"] == "paused"
    finally:
        await store.aclose()


async def test_replay_pins_recorded_reference_versions(db: str) -> None:
    harness = Harness()
    harness.open_books()
    harness.heartbeats(40, dt=100)
    catalog = _referenced(harness.catalog)
    fees = fee_calculator()
    store = await _open(db, catalog, list(harness.rels), fees, "ses-provenance")
    try:
        versions = _versions(catalog, list(harness.rels), fees)
        batch = BatchJournal(
            store.sessions,
            "ses-provenance",
            versions,
            batch_size=500,
            enabled=True,
        )
        for entry in harness.journal:
            batch.append(entry)
        await batch.flush()
        host = ReplayHost()
        playback = await host.ensure(store.sessions, "ses-provenance")
        while playback.step():
            pass
        original = _fingerprint(playback)
        assert original[1]
        assert any(item[2].startswith("sha256:") for item in original[1])
        memory = _memory_fingerprint(
            "ses-provenance", catalog.markets, list(harness.rels), fees, list(playback.entries)
        )
        assert memory == original
        mutated_markets = tuple(
            listed.model_copy(
                update={
                    "status": MarketStatus.PAUSED,
                    "settlement_rules": listed.settlement_rules + " revised after the session",
                }
            )
            for listed in catalog.markets
        )
        rejected = [
            rel.invalidate(
                "reference data changed after the session", rel.updated_at, needs_review=False
            )
            for rel in harness.rels
        ]
        changed_fees = [
            schedule.model_copy(update={"taker_coefficient": dec("0.25")})
            for schedule in fees.registry.schedules
        ]
        poisoned = _memory_fingerprint(
            "ses-provenance",
            mutated_markets,
            rejected,
            FeeCalculator(FeeScheduleRegistry(changed_fees)),
            list(playback.entries),
        )
        assert poisoned != original
        async with store.sessions() as session, session.begin():
            await upsert_catalog(
                session,
                Catalog(series=catalog.series, events=catalog.events, markets=mutated_markets),
                "synthetic",
            )
            await upsert_relationships(session, rejected)
            await upsert_fee_schedules(session, changed_fees)
        replayed = await ReplayHost().ensure(store.sessions, "ses-provenance")
        while replayed.step():
            pass
        assert _fingerprint(replayed) == original
    finally:
        await store.aclose()

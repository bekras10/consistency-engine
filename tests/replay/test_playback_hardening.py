"""Playback starts at a true zero-entry state and applies each forward entry once."""

from __future__ import annotations

import pytest

from consistency_connectors.ingestion import BookManager
from consistency_pipeline import checkpoint as cp
from consistency_pipeline.driver import JournalApplier
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.journal import JournalEntry
from consistency_pipeline.playback import PlaybackSession
from consistency_pipeline.replay import replay_entries
from tests.golden.support import fee_calculator
from tests.unit.test_detection_pipeline import Harness


def _recording() -> tuple[Harness, list[JournalEntry]]:
    harness = Harness()
    harness.open_books()
    harness.heartbeats(12, dt=100)
    return harness, list(harness.journal)


def _session(
    harness: Harness,
    entries: list[JournalEntry],
    *,
    replay_id: str = "play",
    checkpoints: list[cp.Checkpoint] | None = None,
    source_label: str | None = None,
) -> PlaybackSession:
    async def sleep(seconds: float) -> None:
        del seconds

    return PlaybackSession(
        replay_id,
        entries,
        harness.catalog.markets,
        harness.rels,
        fee_calculator(),
        session_id="play",
        checkpoints=checkpoints,
        sleep=sleep,
        source_label=source_label,
    )


def _checkpoints(harness: Harness, entries: list[JournalEntry]) -> list[cp.Checkpoint]:
    fees = fee_calculator()
    manager = BookManager(harness.catalog.markets, source="replay")
    engine = DetectionEngine(manager, harness.rels, fees, session_id="play")
    applier = JournalApplier(manager, engine)
    taken: list[cp.Checkpoint] = []
    for entry in entries:
        applier.apply(entry)
        if entry.ordinal % 4 == 3:
            position = None if entry.message is None else entry.message.position
            taken.append(cp.take("play", entry.ordinal, entry.now_ms, position, manager, engine))
    return taken


def test_fresh_and_restarted_playback_have_applied_zero_entries() -> None:
    harness, entries = _recording()
    assert entries
    fees = fee_calculator()
    blank = replay_entries(
        [],
        harness.catalog.markets,
        harness.rels,
        fees,
        session_id="play",
    )
    session = _session(harness, entries)
    assert session.cursor == 0
    assert session.outcome.events == []
    assert session.outcome.book_digest == blank.book_digest
    assert session.outcome.state_digest == blank.state_digest
    assert session.outcome.engine.active_detections() == []

    assert session.step() is True
    assert session.cursor == 1
    assert session.outcome.book_digest != blank.book_digest

    session.restart()
    assert session.cursor == 0
    assert session.outcome.events == []
    assert session.outcome.book_digest == blank.book_digest
    assert session.outcome.state_digest == blank.state_digest
    assert session.outcome.engine.active_detections() == []


def test_forward_playback_applies_each_entry_once_and_seek_backward_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness, entries = _recording()
    checkpoints = _checkpoints(harness, entries)
    fees = fee_calculator()
    target = entries[6].now_ms
    later = entries[10].now_ms
    direct = replay_entries(
        entries,
        harness.catalog.markets,
        harness.rels,
        fees,
        session_id="play",
        through_ms=target,
    )
    forward = replay_entries(
        entries,
        harness.catalog.markets,
        harness.rels,
        fees,
        session_id="play",
        through_ms=later,
    )
    applied: list[int] = []
    real_apply = JournalApplier.apply

    def spy(self: JournalApplier, entry: JournalEntry) -> list[object]:
        applied.append(entry.ordinal)
        return real_apply(self, entry)

    monkeypatch.setattr(JournalApplier, "apply", spy)

    session = _session(harness, entries, checkpoints=checkpoints)
    applied.clear()
    for _ in entries:
        assert session.step() is True
    assert applied == [entry.ordinal for entry in entries]
    assert session.cursor == len(entries)

    applied.clear()
    session.seek(target)
    assert session.cursor == sum(1 for entry in entries if entry.now_ms <= target)
    assert session.outcome.book_digest == direct.book_digest
    assert session.outcome.state_digest == direct.state_digest
    assert session.outcome.classifications() == direct.classifications()
    # Latest checkpoint at or before entries[6] is ordinal 3; only the suffix is applied.
    assert applied == [
        entry.ordinal for entry in entries if entry.ordinal > 3 and entry.now_ms <= target
    ]

    before = session.cursor
    applied.clear()
    session.seek(later)
    assert applied == [entry.ordinal for entry in entries[before:] if entry.now_ms <= later]
    assert session.outcome.book_digest == forward.book_digest
    assert session.outcome.state_digest == forward.state_digest
    assert session.outcome.classifications() == forward.classifications()


async def test_start_and_resume_apply_each_entry_once(monkeypatch: pytest.MonkeyPatch) -> None:
    harness, entries = _recording()
    applied: list[int] = []
    real_apply = JournalApplier.apply

    def spy(self: JournalApplier, entry: JournalEntry) -> list[object]:
        applied.append(entry.ordinal)
        return real_apply(self, entry)

    monkeypatch.setattr(JournalApplier, "apply", spy)
    session = _session(harness, entries)
    applied.clear()
    await session.play()
    assert session.status == "finished"
    assert session.cursor == len(entries)
    assert applied == [entry.ordinal for entry in entries]

    session.restart()
    session.seek(entries[4].now_ms)
    applied.clear()
    await session.resume()
    assert session.status == "finished"
    assert applied == [entry.ordinal for entry in entries if entry.now_ms > entries[4].now_ms]


def test_seek_cursor_stops_before_a_later_entry_at_the_same_timestamp() -> None:
    harness, entries = _recording()
    shared = entries[0].now_ms
    second = entries[1]
    assert second.message is not None
    tied = second.model_copy(
        update={"message": second.message.model_copy(update={"received_ts_ms": shared})}
    )
    prefix = [entries[0], tied, *entries[2:6]]
    fees = fee_calculator()
    manager = BookManager(harness.catalog.markets, source="replay")
    engine = DetectionEngine(manager, harness.rels, fees, session_id="play")
    applier = JournalApplier(manager, engine)
    applier.apply(prefix[0])
    applier.apply(prefix[1])
    position = None if prefix[1].message is None else prefix[1].message.position
    late = cp.take("play", prefix[1].ordinal, shared, position, manager, engine)
    direct = replay_entries(
        prefix[:1],
        harness.catalog.markets,
        harness.rels,
        fees,
        session_id="play",
    )
    session = _session(harness, prefix, checkpoints=[late])
    session.seek_cursor(1)
    assert session.cursor == 1
    assert session.position_ms == shared
    assert session.outcome.book_digest == direct.book_digest
    session.seek(shared)
    assert session.cursor == 2
    session.seek_cursor(1)
    assert session.cursor == 1
    assert session.outcome.book_digest == direct.book_digest
    assert session.outcome.state_digest == direct.state_digest


def test_playback_books_keep_the_recording_source_label() -> None:
    harness, entries = _recording()
    session = _session(harness, entries, source_label="synthetic:inconsistent")
    assert session.step() is True
    market_id = harness.catalog.markets[0].market_id
    assert session.outcome.manager.book(market_id).source == "synthetic:inconsistent"

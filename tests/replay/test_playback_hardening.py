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

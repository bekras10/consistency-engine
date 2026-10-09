"""Same recording twice, and seek-via-checkpoint, excluding wall-clock telemetry."""

from __future__ import annotations

from decimal import Decimal

import pytest

from consistency_pipeline import checkpoint as cp
from consistency_pipeline.driver import JournalApplier
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.playback import PlaybackService, PlaybackSession, parse_speed
from consistency_pipeline.replay import (
    TELEMETRY_EXCLUDED,
    compare_outcomes,
    latest_checkpoint_through,
    replay_entries,
)
from tests.golden.support import fee_calculator
from tests.unit.test_detection_pipeline import Harness


def _recording() -> tuple[Harness, list]:
    harness = Harness()
    harness.open_books()
    harness.heartbeats(12, dt=100)
    return harness, list(harness.journal)


def test_same_recording_twice_matches_except_telemetry() -> None:
    harness, entries = _recording()
    fees = fee_calculator()
    kwargs = {
        "markets": harness.catalog.markets,
        "relationships": harness.rels,
        "fees": fees,
        "session_id": "det",
    }
    first = replay_entries(entries, **kwargs)
    second = replay_entries(entries, **kwargs)
    compared = compare_outcomes(first, second)
    assert compared.identical
    assert compared.book_match
    assert first.comparable_events() == second.comparable_events()
    assert first.classifications() == second.classifications()
    differed: set[str] = set()
    for left, right in zip(first.events, second.events, strict=True):
        left_timing = left.timing.model_dump()
        right_timing = right.timing.model_dump()
        for key in left_timing:
            if left_timing[key] != right_timing[key]:
                differed.add(key)
    assert differed <= set(TELEMETRY_EXCLUDED)
    assert "processing_started_ns" in TELEMETRY_EXCLUDED
    assert "detection_completed_ns" in TELEMETRY_EXCLUDED


def test_seek_via_checkpoint_matches_replay_from_start() -> None:
    harness, entries = _recording()
    fees = fee_calculator()
    from consistency_connectors.ingestion import BookManager

    manager = BookManager(harness.catalog.markets, source="replay")
    engine = DetectionEngine(manager, harness.rels, fees, session_id="seek")
    applier = JournalApplier(manager, engine)
    checkpoints = []
    for entry in entries:
        applier.apply(entry)
        if entry.ordinal % 4 == 3:
            position = None if entry.message is None else entry.message.position
            checkpoints.append(
                cp.take("seek", entry.ordinal, entry.now_ms, position, manager, engine)
            )
    target = entries[7].now_ms
    chosen = latest_checkpoint_through(checkpoints, target)
    assert chosen is not None
    via = replay_entries(
        entries,
        harness.catalog.markets,
        harness.rels,
        fees,
        session_id="seek",
        checkpoint=chosen,
        through_ms=target,
    )
    direct = replay_entries(
        entries,
        harness.catalog.markets,
        harness.rels,
        fees,
        session_id="seek",
        through_ms=target,
    )
    assert via.state_digest == direct.state_digest
    assert via.book_digest == direct.book_digest
    assert via.classifications() == direct.classifications()


def test_playback_sessions_are_isolated_and_honour_speed() -> None:
    harness, entries = _recording()
    fees = fee_calculator()

    async def sleep(seconds: float) -> None:
        del seconds

    def session(replay_id: str) -> PlaybackSession:
        return PlaybackSession(
            replay_id,
            entries,
            harness.catalog.markets,
            harness.rels,
            fees,
            session_id="play",
            sleep=sleep,
        )

    service = PlaybackService()
    first = service.open(session("a"))
    second = service.open(session("b"))
    assert service.step("a") is True
    assert first.cursor == 1
    assert second.cursor == 0
    assert second.outcome.book_digest != first.outcome.book_digest
    service.restart("a")
    assert first.cursor == 0
    assert service.set_speed("b", Decimal("2")) == Decimal("2")
    with pytest.raises(ValueError, match="playback speed"):
        parse_speed("3")
    assert parse_speed("0.5") == Decimal("0.5")
    assert parse_speed(1) == Decimal("1")


async def test_playback_pause_step_and_speed_delay() -> None:
    harness, entries = _recording()
    fees = fee_calculator()
    delays: list[float] = []

    async def sleep(seconds: float) -> None:
        delays.append(seconds)
        session.pause()

    session = PlaybackSession(
        "paced",
        entries,
        harness.catalog.markets,
        harness.rels,
        fees,
        session_id="play",
        sleep=sleep,
    )
    session.set_speed(Decimal("2"))
    await session.play()
    assert session.status == "paused"
    assert session.cursor == 1
    assert delays
    gap_ms = entries[1].now_ms - entries[0].now_ms
    assert delays[0] == pytest.approx(float(Decimal(gap_ms) / Decimal("2") / Decimal(1000)))
    assert session.step() is True
    assert session.cursor == 2

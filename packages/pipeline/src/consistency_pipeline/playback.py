"""Playback controls over an isolated replay session (spec 12.2).

Phase 11 will call this service. Each ``replay_id`` owns its own book manager and detection
engine; stepping one session cannot change another. Speeds are exactly 0.5, 1, 2, 5, and 10.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Literal

from consistency_connectors.ingestion import BookManager
from consistency_core.fees import FeeCalculator
from consistency_core.models.market import Market
from consistency_core.models.relationship import Relationship
from consistency_pipeline.checkpoint import Checkpoint, restore
from consistency_pipeline.driver import JournalApplier
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.journal import JournalEntry
from consistency_pipeline.lifecycle import DetectionEvent
from consistency_pipeline.replay import ReplayOutcome, latest_checkpoint_through

PlaybackStatus = Literal["ready", "playing", "paused", "finished"]
ALLOWED_SPEEDS: frozenset[Decimal] = frozenset(
    {Decimal("0.5"), Decimal("1"), Decimal("2"), Decimal("5"), Decimal("10")}
)


def parse_speed(value: Decimal | str | int) -> Decimal:
    speed = Decimal(str(value))
    if speed not in ALLOWED_SPEEDS:
        raise ValueError("playback speed must be one of 0.5, 1, 2, 5, 10")
    return speed


class PlaybackSession:
    def __init__(
        self,
        replay_id: str,
        entries: list[JournalEntry],
        markets: list[Market] | tuple[Market, ...],
        relationships: list[Relationship],
        fees: FeeCalculator,
        *,
        session_id: str,
        checkpoints: list[Checkpoint] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.replay_id = replay_id
        self.entries = sorted(entries, key=lambda e: e.ordinal)
        self._markets = markets
        self._relationships = relationships
        self._fees = fees
        self._session_id = session_id
        self._checkpoints = list(checkpoints or [])
        self._sleep = sleep
        self.status: PlaybackStatus = "ready"
        self.speed: Decimal = Decimal("1")
        self._cursor = 0
        self._paused = False
        self._task: asyncio.Task[None] | None = None
        self._applier, self._outcome = self._blank()

    @property
    def cursor(self) -> int:
        """Number of journal entries applied."""
        return self._cursor

    @property
    def position_ms(self) -> int | None:
        if self._cursor == 0:
            return None
        return self.entries[self._cursor - 1].now_ms

    @property
    def outcome(self) -> ReplayOutcome:
        return self._outcome

    @property
    def recording_id(self) -> str:
        return self._session_id

    def isolated_copy(self, replay_id: str) -> PlaybackSession:
        """Another viewer of this recording, starting at the beginning.

        The copy does not share the book manager, detection engine, or cursor.
        """
        return PlaybackSession(
            replay_id,
            list(self.entries),
            self._markets,
            list(self._relationships),
            self._fees,
            session_id=self._session_id,
            checkpoints=list(self._checkpoints),
            sleep=self._sleep,
        )

    def set_speed(self, speed: Decimal | str | int) -> Decimal:
        self.speed = parse_speed(speed)
        return self.speed

    def pause(self) -> None:
        self._paused = True
        if self.status == "playing":
            self.status = "paused"

    def restart(self) -> None:
        self._stop_task()
        self._paused = False
        self._cursor = 0
        self._applier, self._outcome = self._blank()
        self.status = "ready"

    def step(self) -> bool:
        """Apply one entry and leave the session paused. Returns whether an entry was applied."""
        self.pause()
        self._stop_task()
        if self._cursor >= len(self.entries):
            self.status = "finished"
            return False
        self._apply_forward(self._cursor + 1)
        self.status = "finished" if self._cursor >= len(self.entries) else "paused"
        return True

    def seek(self, timestamp_ms: int) -> None:
        """Jump to the state after every entry with ``now_ms <= timestamp_ms``.

        Seeking forward continues the live applier and does not replay entries already
        applied. Seeking backward rebuilds from the latest checkpoint at or before the
        target, then applies the following entries once.
        """
        self.pause()
        self._stop_task()
        target = self._index_at(timestamp_ms)
        if target >= self._cursor:
            self._apply_forward(target)
        else:
            self._rebuild(timestamp_ms, target)
        self.status = "paused"

    def start(self) -> None:
        if self.status == "playing":
            return
        self._paused = False
        self.status = "playing"
        self._task = asyncio.get_running_loop().create_task(self._run())

    async def resume(self) -> None:
        self.start()
        task = self._task
        if task is not None:
            await task

    async def play(self) -> None:
        """Run until paused or the recording ends."""
        self._paused = False
        self.status = "playing"
        await self._run()

    def _blank(self) -> tuple[JournalApplier, ReplayOutcome]:
        """Books and detections with no journal entry applied."""
        manager = BookManager(self._markets, source="replay")
        engine = DetectionEngine(
            manager, self._relationships, self._fees, session_id=self._session_id
        )
        applier = JournalApplier(manager, engine)
        return applier, ReplayOutcome(events=[], manager=manager, engine=engine)

    def _index_at(self, timestamp_ms: int) -> int:
        """Prefix length of entries whose pipeline clock is ``<= timestamp_ms``.

        Journal ordinals are processing order, so receipt time does not go backwards and
        this count is the cursor after those entries.
        """
        count = 0
        for entry in self.entries:
            if entry.now_ms > timestamp_ms:
                break
            count += 1
        return count

    def _apply_forward(self, count: int) -> None:
        """Apply ``entries[cursor:count]`` once on the persistent applier."""
        if count < self._cursor:
            raise ValueError("forward playback cannot rewind; seek backward instead")
        for entry in self.entries[self._cursor : count]:
            self._outcome.events.extend(self._applier.apply(entry))
        self._cursor = count

    def _rebuild(self, timestamp_ms: int, target: int) -> None:
        """Restore the latest checkpoint at or before ``timestamp_ms``, then apply once."""
        checkpoint = latest_checkpoint_through(self._checkpoints, timestamp_ms)
        if checkpoint is None:
            applier, outcome = self._blank()
            selected = [entry for entry in self.entries if entry.now_ms <= timestamp_ms]
        else:
            manager, engine = restore(checkpoint, self._fees)
            applier = JournalApplier(manager, engine)
            applier.last_ordinal = checkpoint.ordinal
            outcome = ReplayOutcome(events=[], manager=manager, engine=engine)
            selected = [
                entry
                for entry in self.entries
                if entry.ordinal > checkpoint.ordinal and entry.now_ms <= timestamp_ms
            ]
        events: list[DetectionEvent] = []
        for entry in selected:
            events.extend(applier.apply(entry))
        outcome.events.extend(events)
        self._applier = applier
        self._outcome = outcome
        self._cursor = target

    async def _run(self) -> None:
        self.status = "playing"
        while self._cursor < len(self.entries):
            if self._paused:
                self.status = "paused"
                return
            if self._cursor > 0:
                delta_ms = self.entries[self._cursor].now_ms - self.entries[self._cursor - 1].now_ms
                delay_ms = Decimal(max(delta_ms, 0)) / self.speed
                if await self._wait(float(delay_ms) / 1000):
                    self.status = "paused"
                    return
            self._apply_forward(self._cursor + 1)
        self.status = "finished"

    async def _wait(self, seconds: float) -> bool:
        """Sleep, then report whether ``pause`` ran during the wait."""
        await self._sleep(seconds)
        return self._paused

    def close(self) -> None:
        """Stop playback and cancel an in-flight task. The recording is unchanged."""
        self._paused = True
        self._stop_task()

    def _stop_task(self) -> None:
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()


class PlaybackService:
    """Registry of isolated playback sessions."""

    def __init__(self) -> None:
        self._sessions: dict[str, PlaybackSession] = {}

    def open(self, session: PlaybackSession) -> PlaybackSession:
        if session.replay_id in self._sessions:
            raise ValueError(f"replay session {session.replay_id} already exists")
        self._sessions[session.replay_id] = session
        return session

    def discard(self, replay_id: str) -> None:
        self._sessions.pop(replay_id, None)

    def __len__(self) -> int:
        return len(self._sessions)

    def get(self, replay_id: str) -> PlaybackSession:
        try:
            return self._sessions[replay_id]
        except KeyError:
            raise KeyError(replay_id) from None

    def start(self, replay_id: str) -> None:
        self.get(replay_id).start()

    def pause(self, replay_id: str) -> None:
        self.get(replay_id).pause()

    async def resume(self, replay_id: str) -> None:
        await self.get(replay_id).resume()

    def restart(self, replay_id: str) -> None:
        self.get(replay_id).restart()

    def step(self, replay_id: str) -> bool:
        return self.get(replay_id).step()

    def seek(self, replay_id: str, timestamp_ms: int) -> None:
        self.get(replay_id).seek(timestamp_ms)

    def set_speed(self, replay_id: str, speed: Decimal | str | int) -> Decimal:
        return self.get(replay_id).set_speed(speed)

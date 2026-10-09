"""Playback controls over an isolated replay session (spec 12.2).

Phase 11 will call this service. Each ``replay_id`` owns its own book manager and detection
engine; stepping one session cannot change another. Speeds are exactly 0.5, 1, 2, 5, and 10.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Literal

from consistency_core.fees import FeeCalculator
from consistency_core.models.market import Market
from consistency_core.models.relationship import Relationship
from consistency_pipeline.checkpoint import Checkpoint
from consistency_pipeline.journal import JournalEntry
from consistency_pipeline.replay import (
    ReplayOutcome,
    latest_checkpoint_through,
    replay_entries,
)

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
        self._outcome = self._fresh(None, None)

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
        self._outcome = self._fresh(None, None)
        self.status = "ready"

    def step(self) -> bool:
        """Apply one entry and leave the session paused. Returns whether an entry was applied."""
        self.pause()
        self._stop_task()
        if self._cursor >= len(self.entries):
            self.status = "finished"
            return False
        self._apply_through(self._cursor + 1)
        self.status = "finished" if self._cursor >= len(self.entries) else "paused"
        return True

    def seek(self, timestamp_ms: int) -> None:
        """Jump to the state after every entry with ``now_ms <= timestamp_ms``."""
        self.pause()
        self._stop_task()
        checkpoint = latest_checkpoint_through(self._checkpoints, timestamp_ms)
        self._outcome = self._fresh(checkpoint, timestamp_ms)
        self._cursor = sum(1 for entry in self.entries if entry.now_ms <= timestamp_ms)
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

    def _fresh(self, checkpoint: Checkpoint | None, through_ms: int | None) -> ReplayOutcome:
        return replay_entries(
            self.entries,
            self._markets,
            self._relationships,
            self._fees,
            session_id=self._session_id,
            checkpoint=checkpoint,
            through_ms=through_ms,
        )

    def _apply_through(self, count: int) -> None:
        """Replay from the start through ``count`` entries (isolated; no shared books)."""
        if count <= 0:
            self._cursor = 0
            self._outcome = self._fresh(None, None)
            return
        prefix = self.entries[:count]
        self._outcome = replay_entries(
            prefix,
            self._markets,
            self._relationships,
            self._fees,
            session_id=self._session_id,
        )
        self._cursor = count

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
            self._apply_through(self._cursor + 1)
        self.status = "finished"

    async def _wait(self, seconds: float) -> bool:
        """Sleep, then report whether ``pause`` ran during the wait."""
        await self._sleep(seconds)
        return self._paused

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

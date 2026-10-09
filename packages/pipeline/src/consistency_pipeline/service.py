"""Live wiring: runner listener -> journal + engine -> outbox -> (persist, then broadcast).

:class:`PipelineListener` is the ``RunnerListener`` the ingestion runner calls inline. For each
message (and each runner-originated loss) it assigns the next journal ordinal, appends the
journal entry, runs the detection engine synchronously and hands the resulting lifecycle events
to the :class:`Outbox`. Clock ticks, relationship changes and session end go through the same
listener so that *every* state change is journaled in processing order: replaying the journal
with :class:`~consistency_pipeline.driver.JournalApplier` reproduces the session exactly.

:class:`Outbox` decouples detection from I/O with a bounded queue: a writer task persists each
batch atomically (``DetectionSink``) and only then publishes it on the broker, so subscribers
never see an event that is not durable. Entries are applied synchronously and enqueued with
``put_nowait``; the listener then awaits ``wait_capacity`` while the queue is at its bound, which
propagates backpressure to the runner's inbound queue (runner losses, which also run during
cancellation, never wait and may exceed the bound).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from consistency_connectors.ingestion import BookUpdate, RunnerLoss
from consistency_core.events import StreamMessage
from consistency_core.models.relationship import Relationship
from consistency_pipeline import checkpoint as cp
from consistency_pipeline.broker import Broker
from consistency_pipeline.checkpoint import Checkpoint
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.journal import ControlEvent, JournalEntry
from consistency_pipeline.lifecycle import CloseReason, DetectionEvent
from consistency_pipeline.serialize import DETECTION_TOPIC, detection_event_payload

log = logging.getLogger("consistency.pipeline")


class JournalSink(Protocol):
    def append(self, entry: JournalEntry) -> None:
        """Buffer one entry (never blocks)."""

    async def wait_capacity(self) -> None:
        """Flush if the buffer reached its batch size (backpressure point)."""

    async def flush(self) -> None: ...


class CheckpointStore(Protocol):
    async def save(self, checkpoint: Checkpoint) -> None: ...


class DetectionSink(Protocol):
    async def write(self, events: Sequence[DetectionEvent]) -> None:
        """Persist one batch atomically (detections + legs + scenarios + certificates)."""


@dataclass
class OutboxStats:
    batches: int = 0
    events: int = 0
    high_water: int = 0
    write_errors: int = 0


class Outbox:
    def __init__(
        self,
        sink: DetectionSink | None = None,
        broker: Broker | None = None,
        *,
        maxsize: int = 1_024,
    ) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self.sink = sink
        self.broker = broker
        self.maxsize = maxsize
        self._queue: deque[list[DetectionEvent]] = deque()
        self._not_empty = asyncio.Event()
        self._not_full = asyncio.Event()
        self._not_full.set()
        self._idle = asyncio.Event()
        self._idle.set()
        self.stats = OutboxStats()

    def __len__(self) -> int:
        return len(self._queue)

    def _push(self, events: list[DetectionEvent]) -> None:
        self._queue.append(events)
        self._idle.clear()
        self._not_empty.set()
        self.stats.high_water = max(self.stats.high_water, len(self._queue))
        if len(self._queue) >= self.maxsize:
            self._not_full.clear()

    def put_nowait(self, events: list[DetectionEvent]) -> None:
        if events:
            self._push(events)

    async def wait_capacity(self) -> None:
        """Backpressure point: wait while the queue is at or above its bound."""
        while len(self._queue) >= self.maxsize:
            self._not_full.clear()
            await self._not_full.wait()

    async def run(self) -> None:
        """Writer loop: persist each batch, then publish it. Runs until cancelled."""
        while True:
            if not self._queue:
                self._idle.set()
                self._not_empty.clear()
                await self._not_empty.wait()
                continue
            batch = self._queue[0]
            if self.sink is not None:
                try:
                    await self.sink.write(batch)
                except Exception:
                    self.stats.write_errors += 1
                    log.exception("detection batch persistence failed; retrying in 1s")
                    await asyncio.sleep(1.0)
                    continue
            self._queue.popleft()
            if len(self._queue) < self.maxsize:
                self._not_full.set()
            self.stats.batches += 1
            self.stats.events += len(batch)
            if self.broker is not None:
                for ev in batch:
                    self.broker.publish(DETECTION_TOPIC, detection_event_payload(ev))

    async def drain(self) -> None:
        """Wait until every queued batch has been written and published (needs ``run``)."""
        while self._queue:
            await self._idle.wait()


@dataclass
class ListenerStats:
    entries: int = 0
    controls: int = 0
    checkpoints: int = 0
    internal_latency_ns: deque[int] = field(default_factory=lambda: deque(maxlen=10_000))


class PipelineListener:
    def __init__(
        self,
        engine: DetectionEngine,
        outbox: Outbox,
        *,
        journal: JournalSink | None = None,
        checkpoints: CheckpointStore | None = None,
        checkpoint_every: int = 2_000,
        start_ordinal: int = 0,
        clock_ms: Callable[[], int] | None = None,
        clock_ns: Callable[[], int] = time.perf_counter_ns,
        on_events: Callable[[list[DetectionEvent]], None] | None = None,
        before_checkpoint: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        if checkpoint_every <= 0:
            raise ValueError("checkpoint_every must be positive")
        self.engine = engine
        self.manager = engine.manager
        self.outbox = outbox
        self.journal = journal
        self.checkpoints = checkpoints
        self.checkpoint_every = checkpoint_every
        self.next_ordinal = start_ordinal
        self.last_message_position: int | None = None
        self._since_checkpoint = 0
        self._clock_ms = clock_ms or self._stream_clock
        self._clock_ns = clock_ns
        self._on_events = on_events
        self._before_checkpoint = before_checkpoint
        self.stats = ListenerStats()

    def _stream_clock(self) -> int:
        return self.manager.last_received_ts_ms or 0

    def now_ms(self) -> int:
        return self._clock_ms()

    def _next(self) -> int:
        ordinal = self.next_ordinal
        self.next_ordinal += 1
        self.stats.entries += 1
        return ordinal

    def _record(self, entry: JournalEntry, events: list[DetectionEvent]) -> None:
        """Synchronous tail of every entry: journal buffer + outbox. Nothing between the ordinal
        assignment and this call awaits, so entries can never interleave or half-apply."""
        if self.journal is not None:
            self.journal.append(entry)
        if events:
            if self._on_events is not None:
                self._on_events(events)
            self.outbox.put_nowait(events)

    async def _backpressure(self) -> None:
        if self.journal is not None:
            await self.journal.wait_capacity()
        await self.outbox.wait_capacity()

    # ------------------------------------------------------------------ RunnerListener
    async def on_message(self, msg: StreamMessage, updates: Sequence[BookUpdate]) -> None:
        started = self._clock_ns()
        entry = JournalEntry(ordinal=self._next(), message=msg)
        events = self.engine.on_message(entry.ordinal, msg.received_ts_ms, updates)
        self.last_message_position = msg.position
        self._record(entry, events)
        self.stats.internal_latency_ns.append(self._clock_ns() - started)
        self._since_checkpoint += 1
        await self._backpressure()
        if self.checkpoints is not None and self._since_checkpoint >= self.checkpoint_every:
            await self.checkpoint()

    def on_loss(self, loss: RunnerLoss, updates: Sequence[BookUpdate]) -> None:
        control = ControlEvent(
            action="recovery_failed" if loss.recovery_request is not None else "connection_lost",
            now_ms=self.now_ms(),
            reason=loss.reason,
            connection_ids=loss.connection_ids,
            recovery_request=loss.recovery_request,
        )
        entry = JournalEntry(ordinal=self._next(), control=control)
        self.stats.controls += 1
        self._record(entry, self.engine.on_message(entry.ordinal, control.now_ms, list(updates)))

    # ------------------------------------------------------------------ other inputs
    async def _control(self, control: ControlEvent) -> list[DetectionEvent]:
        entry = JournalEntry(ordinal=self._next(), control=control)
        self.stats.controls += 1
        match control.action:
            case "tick":
                events = self.engine.tick(entry.ordinal, control.now_ms)
            case "relationships_changed":
                assert control.relationships is not None
                events = self.engine.set_relationships(
                    control.relationships, entry.ordinal, control.now_ms
                )
            case "session_end":
                events = self.engine.close_all(
                    CloseReason(control.reason or CloseReason.SESSION_ENDED.value),
                    entry.ordinal,
                    control.now_ms,
                )
            case _:
                raise ValueError(f"unexpected control action {control.action}")
        self._record(entry, events)
        await self._backpressure()
        return events

    async def tick(self, now_ms: int | None = None) -> list[DetectionEvent]:
        """Clock-only progress for live sources (time-sensitive strategies age out)."""
        return await self._control(
            ControlEvent(action="tick", now_ms=self.now_ms() if now_ms is None else now_ms)
        )

    async def change_relationships(
        self,
        relationships: Sequence[Relationship],
        *,
        version: str | None = None,
        now_ms: int | None = None,
    ) -> list[DetectionEvent]:
        return await self._control(
            ControlEvent(
                action="relationships_changed",
                now_ms=self.now_ms() if now_ms is None else now_ms,
                relationships=tuple(relationships),
                relationships_version=version,
            )
        )

    async def end_session(
        self, reason: CloseReason = CloseReason.SESSION_ENDED, *, now_ms: int | None = None
    ) -> list[DetectionEvent]:
        return await self._control(
            ControlEvent(
                action="session_end",
                now_ms=self.now_ms() if now_ms is None else now_ms,
                reason=reason.value,
            )
        )

    async def checkpoint(self) -> Checkpoint | None:
        """Snapshot book + engine state after the last journaled entry. The snapshot is taken
        synchronously (state and ordinal read together); the journal is flushed before the
        checkpoint is saved, so a checkpoint never references entries that are not durable."""
        if self.checkpoints is None or self.next_ordinal == 0:
            return None
        snap = cp.take(
            self.engine.session_id,
            self.next_ordinal - 1,
            self.now_ms(),
            self.last_message_position,
            self.manager,
            self.engine,
        )
        self._since_checkpoint = 0
        # Persist queued detections before the checkpoint is recorded, so a resume point never
        # references evaluations that were not committed. Callers that have no writer leave this
        # unset (draining without a writer would wait forever).
        if self._before_checkpoint is not None:
            await self._before_checkpoint()
        if self.journal is not None:
            await self.journal.flush()
        await self.checkpoints.save(snap)
        self._since_checkpoint = 0
        self.stats.checkpoints += 1
        return snap


# ---------------------------------------------------------------------- in-memory stores
class MemoryJournal:
    def __init__(self) -> None:
        self.entries: list[JournalEntry] = []

    def append(self, entry: JournalEntry) -> None:
        self.entries.append(entry)

    async def wait_capacity(self) -> None:
        return None

    async def flush(self) -> None:
        return None


class MemoryCheckpoints:
    def __init__(self) -> None:
        self.items: list[Checkpoint] = []

    async def save(self, checkpoint: Checkpoint) -> None:
        self.items.append(checkpoint)

    def latest_at_or_before(self, ordinal: int) -> Checkpoint | None:
        best = None
        for c in self.items:
            if c.ordinal <= ordinal and (best is None or c.ordinal > best.ordinal):
                best = c
        return best


class MemorySink:
    def __init__(self) -> None:
        self.events: list[DetectionEvent] = []
        self.batches = 0

    async def write(self, events: Sequence[DetectionEvent]) -> None:
        self.batches += 1
        self.events.extend(events)

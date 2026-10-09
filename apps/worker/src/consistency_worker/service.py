"""The long-running worker: source -> ingestion -> BookManager -> detection -> outbox.

Session outcomes:

* deterministic source (synthetic / replay) whose stream completed, or any source that failed
  past the restart budget: every open detection is closed (``SESSION_ENDED``), a final
  checkpoint is written and the session is marked ``completed`` / ``failed``;
* deterministic source stopped (signal) or crashed: the session stays ``interrupted``; the next
  start with the same input fingerprint resumes from its latest checkpoint (the store discards
  everything recorded after it), which yields exactly the uninterrupted result;
* live source stopped: open detections are closed and the session is ``stopped``. A live session
  cannot be resumed; a store finding one still ``running`` at startup (crash) invalidates its open
  detections with ``SERVICE_RESTART``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal, Protocol

from consistency_connectors.ingestion import (
    Backoff,
    BookManager,
    BookUpdate,
    CoalescingQueue,
    IngestionRunner,
    merge_book_updates,
)
from consistency_connectors.settings import Settings
from consistency_connectors.sources.recorded import RecordedStreamSource
from consistency_core.serialization import sha256_of
from consistency_pipeline import checkpoint as cp
from consistency_pipeline.broker import Broker
from consistency_pipeline.checkpoint import Checkpoint
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.lifecycle import DetectionEvent
from consistency_pipeline.serialize import MARKET_TOPIC, SYSTEM_TOPIC, market_update_payload
from consistency_pipeline.service import (
    CheckpointStore,
    DetectionSink,
    JournalSink,
    Outbox,
    PipelineListener,
)
from consistency_worker.bootstrap import SessionInputs, build_inputs
from consistency_worker.supervisor import Supervisor

log = logging.getLogger("consistency.worker")

SessionStatus = Literal["running", "completed", "failed", "interrupted", "stopped"]


@dataclass
class OpenedSession:
    session_id: str
    journal: JournalSink | None = None
    checkpoints: CheckpointStore | None = None
    sink: DetectionSink | None = None
    resume_from: Checkpoint | None = None


class SessionStore(Protocol):
    async def open_session(self, inputs: SessionInputs, settings: Settings) -> OpenedSession: ...

    async def close_session(
        self, session_id: str, *, status: SessionStatus, detail: str | None
    ) -> None: ...


def default_session_id(inputs: SessionInputs) -> str:
    return "ses-" + sha256_of({"fingerprint": inputs.fingerprint}).removeprefix("sha256:")[:24]


@dataclass
class WorkerResult:
    session_id: str
    status: SessionStatus
    detail: str | None
    resumed_from: int | None
    restarts: int
    events: dict[str, int] = field(default_factory=dict)


class WorkerService:
    def __init__(
        self,
        settings: Settings,
        *,
        inputs: SessionInputs | None = None,
        store: SessionStore | None = None,
        broker: Broker | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        wall_clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.settings = settings
        self.inputs = inputs
        self.store = store
        self.broker = broker or Broker()
        self._sleep = sleep
        self._wall_ms = wall_clock_ms
        self.listener: PipelineListener | None = None
        self.supervisor: Supervisor | None = None
        self.outbox: Outbox | None = None
        self.started = asyncio.Event()

    def _log_events(self, events: list[DetectionEvent]) -> None:
        for ev in events:
            rec = ev.record
            log.info(
                "detection %s",
                ev.kind.value.lower(),
                extra={
                    "detection_id": ev.detection_id,
                    "relationship_id": rec.relationship_id,
                    "strategy_id": rec.strategy_id,
                    "status": rec.status.value,
                    "classification": rec.classification.value,
                    "reason_codes": list(rec.reason_codes),
                    "close_reason": None if ev.close_reason is None else ev.close_reason.value,
                    "position": ev.position,
                    "source_latency_ms": ev.timing.source_latency_ms,
                    "internal_latency_ns": ev.timing.internal_latency_ns,
                },
            )

    async def _feed(self, updates: CoalescingQueue[BookUpdate], manager: BookManager) -> None:
        while True:
            _, update = await updates.get()
            self.broker.publish(MARKET_TOPIC, market_update_payload(update, manager))

    async def _ticker(self, listener: PipelineListener) -> None:
        interval = self.settings.sweep_interval_ms / 1000
        while True:
            await self._sleep(interval)
            await listener.tick(self._wall_ms())

    async def run(self, stop: asyncio.Event | None = None) -> WorkerResult:
        s = self.settings
        stop = stop or asyncio.Event()
        inputs = self.inputs or await build_inputs(s)
        opened = (
            await self.store.open_session(inputs, s)
            if self.store is not None
            else OpenedSession(session_id=default_session_id(inputs))
        )
        resume = opened.resume_from
        if resume is not None:
            manager, engine = cp.restore(resume, inputs.fees)
            start_ordinal = resume.ordinal + 1
            if (
                isinstance(inputs.source, RecordedStreamSource)
                and resume.source_position is not None
            ):
                inputs.source.position = resume.source_position
        else:
            manager = BookManager(inputs.catalog.markets, source=inputs.source_label)
            engine = DetectionEngine(
                manager,
                inputs.relationships,
                inputs.fees,
                session_id=opened.session_id,
                sweep_interval_ms=s.sweep_interval_ms,
            )
            start_ordinal = 0
        outbox = Outbox(opened.sink, self.broker, maxsize=s.outbox_maxsize)

        async def _drain_outbox() -> None:
            await asyncio.wait_for(outbox.drain(), timeout=30)

        listener = PipelineListener(
            engine,
            outbox,
            journal=opened.journal,
            checkpoints=opened.checkpoints,
            checkpoint_every=s.checkpoint_every,
            start_ordinal=start_ordinal,
            clock_ms=None if inputs.deterministic else self._wall_ms,
            on_events=self._log_events,
            before_checkpoint=_drain_outbox,
        )
        if resume is not None:
            listener.last_message_position = resume.source_position
        self.listener, self.outbox = listener, outbox
        updates: CoalescingQueue[BookUpdate] = CoalescingQueue(
            s.update_queue_maxsize, merge=merge_book_updates
        )

        def make_runner() -> IngestionRunner:
            return IngestionRunner(
                inputs.source,
                manager,
                updates,
                inbound_maxsize=s.inbound_queue_maxsize,
                # A recording's gaps are part of the recording: injecting wall-clock heartbeat
                # losses into a deterministic replay would make it irreproducible.
                heartbeat_timeout_s=None if inputs.deterministic else s.heartbeat_timeout_s,
                listener=listener,
            )

        supervisor = Supervisor(
            make_runner,
            backoff=Backoff(
                base_s=s.supervisor_backoff_base_s,
                max_s=s.supervisor_backoff_max_s,
                max_attempts=s.supervisor_max_restarts,
            ),
            sleep=self._sleep,
        )
        self.supervisor = supervisor
        log.info(
            "session starting",
            extra={
                "session_id": opened.session_id,
                "source": inputs.source_label,
                "deterministic": inputs.deterministic,
                "resumed_from_ordinal": None if resume is None else resume.ordinal,
                "relationships": len(inputs.relationships),
                **s.persistence_summary(),
            },
        )
        self.broker.publish(
            SYSTEM_TOPIC,
            {
                "event": "session_started",
                "session_id": opened.session_id,
                "source": inputs.source_label,
            },
        )
        background = [
            asyncio.create_task(outbox.run(), name="outbox"),
            asyncio.create_task(self._feed(updates, manager), name="market-feed"),
        ]
        retain = getattr(self.store, "retention_loop", None)
        if retain is not None:
            background.append(asyncio.create_task(retain(), name="retention"))
        if not inputs.deterministic:
            background.append(asyncio.create_task(self._ticker(listener), name="ticker"))
        sup_task = asyncio.create_task(supervisor.run(), name="supervisor")
        stop_task = asyncio.create_task(stop.wait(), name="stop")
        self.started.set()
        status: SessionStatus
        detail: str | None = None
        try:
            done, _ = await asyncio.wait({sup_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
            if sup_task in done and stop_task not in done:
                failure = sup_task.result()
                status, detail = ("completed", None) if failure is None else ("failed", failure)
            else:
                sup_task.cancel()
                await asyncio.gather(sup_task, return_exceptions=True)
                status = "interrupted" if inputs.deterministic else "stopped"
            if status != "interrupted":
                await listener.end_session()
                await listener.checkpoint()
            await asyncio.wait_for(outbox.drain(), timeout=30)
            if opened.journal is not None:
                await opened.journal.flush()
        except BaseException:
            status, detail = "interrupted", "worker crashed or was cancelled"
            raise
        finally:
            stop_task.cancel()
            sup_task.cancel()
            for t in background:
                t.cancel()
            await asyncio.gather(stop_task, sup_task, *background, return_exceptions=True)
            if self.store is not None:
                with contextlib.suppress(Exception):
                    await self.store.close_session(opened.session_id, status=status, detail=detail)
            log.info(
                "session finished",
                extra={
                    "session_id": opened.session_id,
                    "status": status,
                    "detail": detail,
                    "engine": engine.stats.as_dict(),
                    "restarts": supervisor.stats.restarts,
                },
            )
        self.broker.publish(
            SYSTEM_TOPIC,
            {"event": "session_finished", "session_id": opened.session_id, "status": status},
        )
        return WorkerResult(
            session_id=opened.session_id,
            status=status,
            detail=detail,
            resumed_from=None if resume is None else resume.ordinal,
            restarts=supervisor.stats.restarts,
            events=dict(engine.stats.events),
        )

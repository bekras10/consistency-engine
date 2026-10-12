"""The long-running worker: source -> ingestion -> BookManager -> detection -> outbox.

Session outcomes:

* deterministic source (synthetic / replay) whose stream completed, or any source that failed
  past the restart budget: every open detection is closed (``SESSION_ENDED``), a final
  checkpoint is written and the session is marked ``completed`` / ``failed``;
* deterministic source stopped (signal) or crashed: the session stays ``interrupted``; the next
  start with the same input fingerprint resumes from its latest checkpoint when that
  fingerprint's latest session is resumable (the store discards everything recorded after
  it), which yields exactly the uninterrupted result. A later ``failed`` or ``interrupted``
  reseed is resumed even if an older session ``completed``, and that resume keeps the reseed
  id. If the latest session ``completed``, it is not ingested again unless ``RESEED_SESSION``
  is set;
* live source stopped: open detections are closed and the session is ``stopped``. A live session
  is not resumed. Startup allocates a new session id.
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
from consistency_connectors.settings import DataSourceMode, Settings
from consistency_connectors.sources.recorded import RecordedStreamSource
from consistency_core.pricing.evaluator import Evaluation
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


def _episodes_from_lifecycle(events: list[dict[str, object]]) -> list[dict[str, object]]:
    """One episode per detection. Lifecycle updates share that detection id."""
    episodes: list[dict[str, object]] = []
    for event in events:
        if event.get("kind") != "lifecycle":
            continue
        payload = event.get("payload")
        if not isinstance(payload, dict):
            continue
        detection_id = str(event.get("subject_id", ""))
        study_id = str(event.get("study_id", ""))
        episodes.append(
            {
                "episode_id": f"{study_id}|{detection_id}",
                "study_id": study_id,
                "session_id": str(event.get("session_id", "")),
                "detection_id": detection_id,
                "relationship_id": str(payload.get("relationship_id", "")),
                "strategy_id": str(payload.get("strategy_id", "")),
                "classification": str(event.get("classification", "")),
                "status": str(payload.get("status", "")),
                "first_ms": payload.get("first_ms"),
                "last_ms": payload.get("last_ms"),
                "close_reason": payload.get("close_reason"),
                "gross_edge": payload.get("gross_edge"),
                "fees": payload.get("fees"),
                "net_edge": payload.get("net_edge"),
                "quantity": payload.get("quantity"),
            }
        )
    return episodes


SessionStatus = Literal["running", "completed", "failed", "interrupted", "stopped"]


@dataclass
class OpenedSession:
    session_id: str
    journal: JournalSink | None = None
    checkpoints: CheckpointStore | None = None
    sink: DetectionSink | None = None
    resume_from: Checkpoint | None = None
    skip_ingest: bool = False


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
        self._study_pending: list[dict[str, object]] = []
        self._study_session_id = ""

    def _study_enabled(self) -> bool:
        return bool(self.settings.study_id) and (
            self.settings.data_source is DataSourceMode.KALSHI_AUTHORIZED
        )

    def _observe_evaluation(
        self, ev: Evaluation, relationship_id: str, strategy_id: str, now_ms: int
    ) -> None:
        if not self._study_enabled() or not self._study_session_id:
            return
        classification = ev.classification.value
        synchronized = classification not in ("UNSYNCHRONIZED_DATA", "STALE_DATA")
        key = (
            f"{self._study_session_id}|evaluation|{relationship_id}|{strategy_id}|"
            f"{now_ms}|{classification}"
        )
        self._study_pending.append(
            {
                "idempotency_key": key,
                "study_id": self.settings.study_id,
                "session_id": self._study_session_id,
                "kind": "evaluation",
                "occurred_ms": now_ms,
                "subject_id": relationship_id,
                "classification": classification,
                "synchronized": synchronized,
                "payload": {"strategy_id": strategy_id},
            }
        )

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
            self._queue_lifecycle(ev)

    def _queue_lifecycle(self, ev: DetectionEvent) -> None:
        if not self._study_enabled() or not self._study_session_id:
            return
        rec = ev.record
        self._study_pending.append(
            {
                "idempotency_key": f"{rec.session_id}|lifecycle|{ev.detection_id}|{ev.seq}",
                "study_id": self.settings.study_id,
                "session_id": rec.session_id,
                "kind": "lifecycle",
                "occurred_ms": ev.at_ms,
                "subject_id": ev.detection_id,
                "classification": rec.classification.value,
                "synchronized": True,
                "payload": {
                    "kind": ev.kind.value,
                    "relationship_id": rec.relationship_id,
                    "strategy_id": rec.strategy_id,
                    "status": rec.status.value,
                    "first_ms": rec.first_observed_ms,
                    "last_ms": rec.last_observed_ms,
                    "close_reason": None if ev.close_reason is None else ev.close_reason.value,
                    "gross_edge": None
                    if rec.metrics.gross_edge is None
                    else format(rec.metrics.gross_edge, "f"),
                    "fees": None
                    if rec.metrics.total_fees is None
                    else format(rec.metrics.total_fees, "f"),
                    "net_edge": None
                    if rec.metrics.net_edge is None
                    else format(rec.metrics.net_edge, "f"),
                    "quantity": None
                    if rec.metrics.depth_supported_quantity is None
                    else format(rec.metrics.depth_supported_quantity, "f"),
                },
            }
        )

    async def _flush_study(self) -> None:
        pending = self._study_pending
        self._study_pending = []
        if not pending or self.store is None:
            return
        record = getattr(self.store, "record_study_events", None)
        episodes = getattr(self.store, "record_study_episodes", None)
        if callable(record):
            await record(pending)
        if callable(episodes):
            await episodes(_episodes_from_lifecycle(pending))

    async def _feed(self, updates: CoalescingQueue[BookUpdate], manager: BookManager) -> None:
        while True:
            _, update = await updates.get()
            self.broker.publish(MARKET_TOPIC, market_update_payload(update, manager))

    async def _ticker(
        self, listener: PipelineListener, inputs: SessionInputs, manager: BookManager
    ) -> None:
        interval = self.settings.sweep_interval_ms / 1000
        last_maintenance = 0
        while True:
            await self._sleep(interval)
            now = self._wall_ms()
            await listener.tick(now)
            await self._flush_study()
            if now - last_maintenance < 5000:
                continue
            last_maintenance = now
            try:
                await self._record_feed(inputs)
                consider = getattr(inputs.source, "consider_maintenance", None)
                if callable(consider):
                    await consider(manager)
                await self._apply_universe(inputs, listener)
                await self._flush_study()
            except Exception:
                log.warning("live feed maintenance failed")

    async def _apply_universe(self, inputs: SessionInputs, listener: PipelineListener) -> None:
        source = inputs.source
        drain = getattr(source, "drain_membership_changes", None)
        if callable(drain):
            for change in drain():
                if not self._study_enabled():
                    continue
                action = str(getattr(change, "action", ""))
                subject_id = str(getattr(change, "subject_id", ""))
                kind = str(getattr(change, "subject_kind", ""))
                at_ms = int(getattr(change, "at_ms", 0))
                self._study_pending.append(
                    {
                        "idempotency_key": (
                            f"{self._study_session_id}|membership|{kind}|{subject_id}|{action}|{at_ms}"
                        ),
                        "study_id": self.settings.study_id,
                        "session_id": self._study_session_id,
                        "kind": "membership",
                        "occurred_ms": at_ms,
                        "subject_id": subject_id,
                        "classification": str(getattr(change, "verification_status", "")),
                        "synchronized": True,
                        "payload": {
                            "action": action,
                            "subject_kind": kind,
                            "fingerprint": str(getattr(change, "fingerprint", "")),
                        },
                    }
                )
        take = getattr(source, "take_relationship_revision", None)
        if callable(take):
            revised = take()
            if revised:
                inputs.relationships = list(revised)
                await listener.change_relationships(revised)

    async def _record_feed(self, inputs: SessionInputs) -> None:
        status_fn = getattr(inputs.source, "feed_status", None)
        record = None
        if self.store is not None:
            record = getattr(self.store, "record_component_health", None)
        if not callable(status_fn) or not callable(record):
            return
        detail = status_fn()
        if isinstance(detail, dict):
            await record("kalshi_feed", str(detail.get("state", "unknown")), detail)

    async def run(self, stop: asyncio.Event | None = None) -> WorkerResult:
        s = self.settings
        stop = stop or asyncio.Event()
        inputs = self.inputs or await build_inputs(s)
        opened = (
            await self.store.open_session(inputs, s)
            if self.store is not None
            else OpenedSession(session_id=default_session_id(inputs))
        )
        if opened.skip_ingest:
            log.info(
                "completed session already stored; not ingesting",
                extra={"session_id": opened.session_id, "source": inputs.source_label},
            )
            return WorkerResult(
                session_id=opened.session_id,
                status="completed",
                detail="already_completed",
                resumed_from=None,
                restarts=0,
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
                config=inputs.evaluation_config,
            )
            start_ordinal = 0
        self._study_session_id = opened.session_id
        if self._study_enabled():
            engine.evaluation_observer = self._observe_evaluation
        note = getattr(inputs.source, "note_relationships", None)
        if callable(note):
            note(inputs.relationships)
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
        await self._apply_universe(inputs, listener)
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
        if not inputs.deterministic:
            background.append(
                asyncio.create_task(self._ticker(listener, inputs, manager), name="ticker")
            )
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
                    await self._flush_study()
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

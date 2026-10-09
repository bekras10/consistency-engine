"""Phase 7: broker, outbox, live listener (journal), checkpoints, supervisor, worker service."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Sequence

import pytest

from consistency_connectors.base import (
    ConnectionHealth,
    ConnectionState,
    MarketDataSource,
    RecoveryRequest,
    SourceAuthenticationError,
)
from consistency_connectors.ingestion import (
    Backoff,
    BookManager,
    BookUpdate,
    CoalescingQueue,
    IngestionRunner,
)
from consistency_connectors.settings import Settings
from consistency_core.events import HeartbeatEvent, OrderBookSnapshotEvent, StreamMessage
from consistency_core.models import DataSourceKind, Market, MarketRules
from consistency_core.models.market import Event, Series
from consistency_core.relationships.discovery import discover
from consistency_pipeline import checkpoint as cp
from consistency_pipeline.broker import Broker
from consistency_pipeline.driver import JournalApplier
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.lifecycle import CloseReason, DetectionEvent, EventKind
from consistency_pipeline.service import (
    MemoryCheckpoints,
    MemoryJournal,
    MemorySink,
    Outbox,
    PipelineListener,
)
from consistency_worker import logs
from consistency_worker.__main__ import main as worker_main
from consistency_worker.bootstrap import SessionInputs, build_inputs
from consistency_worker.service import WorkerService
from consistency_worker.supervisor import Supervisor
from tests.factories import T0
from tests.golden.support import fee_calculator
from tests.unit.test_detection_pipeline import GE2, GE3, _catalog
from tests.unit.test_ingestion import Feed, snap


async def _no_sleep(_: float) -> None:
    await asyncio.sleep(0)


# ---------------------------------------------------------------------- broker
def test_broker_fanout_topics_and_sequence() -> None:
    b = Broker()
    all_ = b.subscribe()
    det = b.subscribe({"detection"})
    b.publish("market", {"m": 1})
    b.publish("detection", {"d": 1})
    a1, a2 = all_.get_nowait(), all_.get_nowait()
    assert a1 is not None and a2 is not None
    assert [a1.seq, a2.seq] == [1, 2]
    d = det.get_nowait()
    assert d is not None and d.topic == "detection" and d.seq == 2
    assert det.get_nowait() is None


def test_slow_subscriber_drops_oldest_and_is_told_it_lagged() -> None:
    b = Broker()
    sub = b.subscribe(maxsize=3)
    for i in range(5):
        b.publish("detection", {"i": i})
    assert sub.dropped == 2
    first = sub.get_nowait()
    assert first is not None and first.lagged and first.payload == {"i": 2}
    rest = [sub.get_nowait(), sub.get_nowait()]
    assert [m.payload["i"] for m in rest if m is not None] == [3, 4]
    assert all(m is not None and not m.lagged for m in rest)


async def test_subscription_async_iteration_and_close() -> None:
    b = Broker()
    sub = b.subscribe()

    async def consume() -> list[int]:
        return [m.payload["i"] async for m in sub]

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    b.publish("x", {"i": 1})
    b.publish("x", {"i": 2})
    await asyncio.sleep(0)
    sub.close()
    assert await asyncio.wait_for(task, 1) == [1, 2]
    assert b.subscriber_count == 0


# ---------------------------------------------------------------------- outbox
class _OrderedSink:
    def __init__(self, broker: Broker, fail_first: int = 0) -> None:
        self.broker = broker
        self.fail_first = fail_first
        self.written: list[int] = []
        self.published_at_write: list[int] = []

    async def write(self, events: Sequence[DetectionEvent]) -> None:
        if self.fail_first:
            self.fail_first -= 1
            raise RuntimeError("db down")
        self.published_at_write.append(self.broker.published)
        self.written += [e.seq for e in events]


def _fake_events(n: int) -> list[DetectionEvent]:
    h = _harness_with_detection()
    ev = h[0]
    return [ev.model_copy(update={"seq": i}) for i in range(n)]


def _harness_with_detection() -> list[DetectionEvent]:
    from tests.unit.test_detection_pipeline import Harness

    h = Harness()
    return h.open_books()


async def test_outbox_persists_before_publishing() -> None:
    broker = Broker()
    sub = broker.subscribe()
    sink = _OrderedSink(broker)
    outbox = Outbox(sink, broker, maxsize=4)
    task = asyncio.create_task(outbox.run())
    events = _fake_events(3)
    for e in events:
        outbox.put_nowait([e])
    await asyncio.wait_for(outbox.drain(), 2)
    task.cancel()
    assert sink.written == [0, 1, 2]
    # each batch was written while nothing of it had been published yet
    assert sink.published_at_write == [0, 1, 2]
    got = []
    while (m := sub.get_nowait()) is not None:
        got.append(m.payload["seq"])
    assert got == [0, 1, 2]


async def test_outbox_retries_failed_writes_without_loss(monkeypatch: pytest.MonkeyPatch) -> None:
    real_sleep = asyncio.sleep
    monkeypatch.setattr(asyncio, "sleep", lambda _s: real_sleep(0))
    broker = Broker()
    sink = _OrderedSink(broker, fail_first=2)
    outbox = Outbox(sink, broker)
    task = asyncio.create_task(outbox.run())
    outbox.put_nowait(_fake_events(1))
    await asyncio.wait_for(outbox.drain(), 2)
    task.cancel()
    assert sink.written == [0]
    assert outbox.stats.write_errors == 2


async def test_outbox_backpressure_blocks_until_drained() -> None:
    outbox = Outbox(MemorySink(), maxsize=2)
    outbox.put_nowait(_fake_events(1))
    outbox.put_nowait(_fake_events(1))
    waiter = asyncio.create_task(outbox.wait_capacity())
    await asyncio.sleep(0.01)
    assert not waiter.done()
    task = asyncio.create_task(outbox.run())
    await asyncio.wait_for(waiter, 1)
    await asyncio.wait_for(outbox.drain(), 1)
    task.cancel()
    assert outbox.stats.high_water == 2


# ---------------------------------------------------------------------- live listener + runner
class _LiveSource(MarketDataSource):
    """A live-like source (``stream_is_finite = False``): each subscription plays the next
    scripted session and then ends, which the runner must treat as END_OF_STREAM."""

    kind = DataSourceKind.SYNTHETIC
    stream_is_finite = False

    def __init__(self, sessions: list[list[object]], *, after: str = "hang") -> None:
        self.sessions = sessions
        self.after = after
        self.subscriptions = 0
        self.feed = Feed()

    async def get_series(self) -> Sequence[Series]:
        return ()

    async def get_events(self) -> Sequence[Event]:
        return ()

    async def get_markets(self) -> Sequence[Market]:
        return _catalog().markets

    async def get_market_rules(self, market_id: str) -> MarketRules:
        raise NotImplementedError

    async def get_market_orderbook(self, market_id: str) -> OrderBookSnapshotEvent:
        raise NotImplementedError

    async def subscribe_orderbooks(
        self, market_ids: Sequence[str] | None = None
    ) -> AsyncIterator[StreamMessage]:
        i = self.subscriptions
        self.subscriptions += 1
        if i >= len(self.sessions):
            if self.after == "auth":
                raise SourceAuthenticationError("token revoked")
            await asyncio.sleep(3600)
            return
        for item in self.sessions[i]:
            if isinstance(item, tuple):
                ev, dt = item
            else:
                ev, dt = item, 10
            yield self.feed.msg(ev, dt=dt)  # type: ignore[arg-type]
            await asyncio.sleep(0)

    async def unsubscribe_orderbooks(self, market_ids: Sequence[str]) -> None:
        return None

    async def request_recovery(self, request: RecoveryRequest) -> None:
        return None

    def get_connection_health(self) -> ConnectionHealth:
        return ConnectionHealth(source_kind=self.kind, state=ConnectionState.CONNECTED)


def _session(sid: str, n_heartbeats: int = 12) -> list[object]:
    out: list[object] = [
        snap(GE3, 1, sid=sid, yes=[("0.70", "100")]),
        snap(GE2, 2, sid=sid, no=[("0.65", "100")]),
    ]
    out += [(HeartbeatEvent(connection_id="c1"), 100) for _ in range(n_heartbeats)]
    return out


def _pipeline(
    *, checkpoint_every: int = 1000
) -> tuple[BookManager, DetectionEngine, PipelineListener, MemoryJournal, MemoryCheckpoints]:
    cat = _catalog()
    mgr = BookManager(cat.markets, source="live-test")
    engine = DetectionEngine(mgr, discover(cat, as_of=T0), fee_calculator(), session_id="live")
    journal, cps = MemoryJournal(), MemoryCheckpoints()
    listener = PipelineListener(
        engine,
        Outbox(MemorySink()),
        journal=journal,
        checkpoints=cps,
        checkpoint_every=checkpoint_every,
    )
    return mgr, engine, listener, journal, cps


def _replay(journal: MemoryJournal) -> tuple[BookManager, list[DetectionEvent]]:
    cat = _catalog()
    mgr = BookManager(cat.markets, source="live-test")
    engine = DetectionEngine(mgr, discover(cat, as_of=T0), fee_calculator(), session_id="live")
    return mgr, JournalApplier(mgr, engine).apply_all(journal.entries)


async def test_supervisor_restarts_after_live_end_of_stream_and_journal_replays() -> None:
    src = _LiveSource([_session("s1"), _session("s2")], after="auth")
    mgr, engine, listener, journal, _ = _pipeline()
    events: list[DetectionEvent] = []
    listener._on_events = events.extend
    q: CoalescingQueue[BookUpdate] = CoalescingQueue(1000)
    sup = Supervisor(
        lambda: IngestionRunner(src, mgr, q, heartbeat_timeout_s=None, listener=listener),
        backoff=Backoff(max_attempts=3),
        sleep=_no_sleep,
    )
    result = await asyncio.wait_for(sup.run(), 5)
    # two END_OF_STREAM restarts, then a non-retryable authentication failure
    assert sup.stats.restarts == 2
    assert result is not None and result.startswith("authentication")
    kinds = [(e.kind.value, e.close_reason) for e in events]
    assert kinds == [
        ("OPENED", None),
        ("UPDATED", None),
        ("INVALIDATED", CloseReason.BOOK_UNSYNCHRONIZED),  # END_OF_STREAM, fail-closed
        ("OPENED", None),  # new subscription: must re-earn the duration
        ("UPDATED", None),
        ("INVALIDATED", CloseReason.BOOK_UNSYNCHRONIZED),
    ]
    assert "END_OF_STREAM" in (events[2].close_detail or "")
    assert events[3].record.ordinal == 2
    controls = [e.control.action for e in journal.entries if e.control is not None]
    assert controls == ["connection_lost", "connection_lost"]
    assert [e.ordinal for e in journal.entries] == list(range(len(journal.entries)))
    # replaying the journal reproduces the live session exactly
    rmgr, replayed = _replay(journal)
    assert [e.comparable() for e in replayed] == [e.comparable() for e in events]
    assert rmgr.state_digest() == mgr.state_digest()


async def test_supervisor_gives_up_after_restart_budget() -> None:
    src = _LiveSource([[] for _ in range(10)])
    mgr, _, listener, _, _ = _pipeline()
    q: CoalescingQueue[BookUpdate] = CoalescingQueue(1000)
    sup = Supervisor(
        lambda: IngestionRunner(src, mgr, q, heartbeat_timeout_s=None, listener=listener),
        backoff=Backoff(max_attempts=2),
        sleep=_no_sleep,
    )
    result = await asyncio.wait_for(sup.run(), 5)
    assert result is not None and result.startswith("restart budget exhausted")
    assert sup.stats.restarts == 2


async def test_checkpoint_restore_continues_like_uninterrupted_run() -> None:
    src = _LiveSource([_session("s1", 30)])
    mgr, engine, listener, journal, cps = _pipeline(checkpoint_every=7)
    events: list[DetectionEvent] = []
    listener._on_events = events.extend
    q: CoalescingQueue[BookUpdate] = CoalescingQueue(1000)
    runner = IngestionRunner(src, mgr, q, heartbeat_timeout_s=None, listener=listener)
    await asyncio.wait_for(runner.run(), 5)
    assert len(cps.items) == len([e for e in journal.entries if e.message is not None]) // 7
    for c in cps.items:
        fees = fee_calculator()
        m2, e2 = cp.restore(c, fees)
        app = JournalApplier(m2, e2)
        app.last_ordinal = c.ordinal
        rest = app.apply_all(journal.entries[c.ordinal + 1 :])
        tail = [e.comparable() for e in events if e.position > c.ordinal]
        assert [e.comparable() for e in rest] == tail
        assert m2.state_digest() == mgr.state_digest()
        assert cp.state_digest(m2.export_state(), e2.export_state()) == cp.state_digest(
            mgr.export_state(), engine.export_state()
        )


async def test_listener_ticks_and_relationship_changes_are_journaled() -> None:
    mgr, engine, listener, journal, _ = _pipeline()
    feed = Feed()
    for ev in (snap(GE3, 1, yes=[("0.70", "100")]), snap(GE2, 2, no=[("0.65", "100")])):
        msg = feed.msg(ev)
        await listener.on_message(msg, mgr.process(msg))
    expired = await listener.tick(feed.t + 5000)
    assert [e.kind for e in expired] == [EventKind.EXPIRED]
    rels = [r.invalidate("review", T0, needs_review=False) for r in engine.relationships]
    await listener.change_relationships(rels, version="v2")
    await listener.end_session()
    actions = [e.control.action for e in journal.entries if e.control is not None]
    assert actions == ["tick", "relationships_changed", "session_end"]
    _, replayed = _replay(journal)
    assert [e.kind for e in replayed] == [EventKind.OPENED, EventKind.EXPIRED]


# ---------------------------------------------------------------------- worker service
async def _inputs(src: MarketDataSource, *, deterministic: bool) -> SessionInputs:
    cat = _catalog()
    return SessionInputs(
        source=src,
        catalog=cat,
        relationships=discover(cat, as_of=T0),
        fees=fee_calculator(),
        source_label="test",
        deterministic=deterministic,
        fingerprint="sha256:test",
    )


async def test_worker_graceful_stop_on_live_source_closes_detections() -> None:
    src = _LiveSource([_session("s1", 12)])
    inputs = await _inputs(src, deterministic=False)
    clock = {"ms": 0}
    svc = WorkerService(
        Settings(), inputs=inputs, sleep=_no_sleep, wall_clock_ms=lambda: clock["ms"]
    )
    sub = svc.broker.subscribe({"detection", "system"})
    stop = asyncio.Event()
    task = asyncio.create_task(svc.run(stop))
    await asyncio.wait_for(svc.started.wait(), 2)
    while src.subscriptions == 0 or (svc.listener and svc.listener.stats.entries < 14):
        await asyncio.sleep(0.01)
    stop.set()
    result = await asyncio.wait_for(task, 5)
    assert result.status == "stopped"
    payloads = []
    while (m := sub.get_nowait()) is not None:
        payloads.append(m.payload)
    detection_events = [p["event"] for p in payloads if "detection" in p]
    assert detection_events[:2] == ["OPENED", "UPDATED"]
    assert detection_events[-1] == "INVALIDATED"  # RUNNER_STOPPED desync on shutdown
    assert payloads[-1] == {
        "event": "session_finished",
        "session_id": result.session_id,
        "status": "stopped",
    }


async def test_worker_completes_deterministic_session() -> None:
    settings = Settings(SYNTHETIC_PRESET="smoke")
    inputs = await build_inputs(settings, fast=True)
    result = await asyncio.wait_for(WorkerService(settings, inputs=inputs).run(), 60)
    assert result.status == "completed"
    assert result.restarts == 0


async def test_worker_stop_on_deterministic_source_leaves_session_resumable() -> None:
    settings = Settings(SYNTHETIC_PRESET="smoke")
    inputs = await build_inputs(settings, fast=True)
    svc = WorkerService(settings, inputs=inputs)
    stop = asyncio.Event()
    stop.set()
    result = await asyncio.wait_for(svc.run(stop), 60)
    assert result.status == "interrupted"


def test_kalshi_source_stays_refused_by_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_SOURCE", "kalshi_authorized")
    monkeypatch.setenv("ENABLE_KALSHI_API", "true")
    monkeypatch.setenv("KALSHI_AUTHORIZATION_CONFIRMED", "true")
    assert worker_main([]) == 2


def test_worker_refuses_live_trading(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_LIVE_TRADING", "true")
    assert worker_main([]) == 2


def test_json_log_lines(capsys: pytest.CaptureFixture[str]) -> None:
    logs.configure("INFO")
    logging.getLogger("consistency.test").info("hello", extra={"detection_id": "det-1"})
    line = capsys.readouterr().err.strip().splitlines()[-1]
    obj = json.loads(line)
    assert obj["msg"] == "hello" and obj["detection_id"] == "det-1" and obj["level"] == "INFO"

"""Phase 7: incremental detection engine and lifecycle (spec 10.1 / 10.2).

Driven through a real ``BookManager`` with golden fixture A's verified implication
(NO(GE3) ask 0.30 + YES(GE2) ask 0.35 -> FEE_ADJUSTED_CANDIDATE once it has persisted for
``minimum_candidate_duration_ms`` = 1000 ms) plus an unrelated copy of it (markets ``GOLD-Z-*``).
"""

from __future__ import annotations

import dataclasses
from datetime import timedelta
from typing import Any

import pytest

from consistency_connectors.ingestion import BookManager, BookUpdate, DesyncReason
from consistency_core.events import HeartbeatEvent, MarketStatusEvent, StreamEvent
from consistency_core.models import MarketStatus, Side
from consistency_core.models.detection import Classification
from consistency_core.models.market import Catalog
from consistency_core.models.relationship import Relationship
from consistency_core.money import dec
from consistency_core.pricing.evaluator import Evaluation
from consistency_core.relationships.discovery import discover
from consistency_core.relationships.review import revalidate
from consistency_pipeline.driver import JournalApplier
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.journal import JournalEntry
from consistency_pipeline.lifecycle import (
    CloseReason,
    DetectionEvent,
    DetectionMetrics,
    DetectionStatus,
    EventKind,
)
from tests.factories import T0
from tests.golden.support import catalog, fee_calculator, load
from tests.unit.test_ingestion import Feed, delta, snap

GE3, GE2 = "GOLD-A-GE3", "GOLD-A-GE2"
Z3, Z2 = "GOLD-Z-GE3", "GOLD-Z-GE2"


def _catalog(*, ge2_rules: str | None = None) -> Catalog:
    a = catalog(load("A"))
    fz = load("A")
    fz["common_settlement"]["underlying_id"] = "golden-z-index"
    for m in fz["markets"]:
        m["id"] = m["id"].replace("-A-", "-Z-")
    z = catalog(fz)
    markets = list(a.markets) + list(z.markets)
    if ge2_rules is not None:
        markets = [
            m.model_copy(update={"settlement_rules": ge2_rules}) if m.market_id == GE2 else m
            for m in markets
        ]
    return Catalog(markets=tuple(markets))


class Harness:
    """Feeds messages through BookManager -> DetectionEngine exactly like the live listener."""

    def __init__(self, **engine_kw: Any) -> None:
        self.catalog = _catalog()
        self.rels = discover(self.catalog, as_of=T0)
        mgr_kw = {"clock_ns": engine_kw["clock_ns"]} if "clock_ns" in engine_kw else {}
        self.mgr = BookManager(self.catalog.markets, source="unit", **mgr_kw)
        self.engine = DetectionEngine(
            self.mgr, self.rels, fee_calculator(), session_id="unit", **engine_kw
        )
        self.feed = Feed()
        self.journal: list[JournalEntry] = []
        self.events: list[DetectionEvent] = []
        self.seq: dict[str, int] = {}

    @property
    def now(self) -> int:
        return self.feed.t + 5

    def send(self, event: StreamEvent, *, dt: int = 10) -> list[DetectionEvent]:
        msg = self.feed.msg(event, dt=dt)
        entry = JournalEntry(ordinal=len(self.journal), message=msg)
        self.journal.append(entry)
        updates = self.mgr.process(msg)
        self.mgr.drain_recovery_requests()
        out = self.engine.on_message(entry.ordinal, msg.received_ts_ms, updates)
        self.events += out
        return out

    def next_seq(self, sid: str = "1") -> int:
        self.seq[sid] = self.seq.get(sid, 0) + 1
        return self.seq[sid]

    def open_books(self, *, sid: str = "1") -> list[DetectionEvent]:
        out = self.send(snap(GE3, self.next_seq(sid), sid=sid, yes=[("0.70", "100")]))
        out += self.send(snap(GE2, self.next_seq(sid), sid=sid, no=[("0.65", "100")]))
        return out

    def heartbeats(self, n: int, *, dt: int = 100) -> list[DetectionEvent]:
        out: list[DetectionEvent] = []
        for _ in range(n):
            out += self.send(HeartbeatEvent(connection_id="c1"), dt=dt)
        return out

    def key(self, rel_member: str = GE3) -> str:
        (key,) = self.engine.strategy_keys_for_market(rel_member)
        return key

    def relationship(self, member: str = GE3) -> Relationship:
        (rid,) = self.engine.relationship_ids_for_market(member)
        return next(r for r in self.engine.relationships if r.relationship_id == rid)


def _kinds(events: list[DetectionEvent]) -> list[str]:
    return [e.kind.value for e in events]


def _candidate(h: Harness) -> None:
    opened = h.open_books()
    assert _kinds(opened) == ["OPENED"]
    assert opened[0].classification is Classification.DEPTH_SUPPORTED
    assert opened[0].reason_codes == ("DURATION_BELOW_MINIMUM",)
    upd = h.heartbeats(11)
    assert _kinds(upd) == ["UPDATED"]
    assert upd[0].classification is Classification.FEE_ADJUSTED_CANDIDATE


def test_index_only_covers_verified_leg_markets() -> None:
    h = Harness()
    assert h.engine.strategy_keys_for_market(GE3) == h.engine.strategy_keys_for_market(GE2)
    assert h.engine.strategy_keys_for_market(Z3) != h.engine.strategy_keys_for_market(GE3)
    assert h.engine.strategy_keys_for_market("UNKNOWN") == []


def test_unrelated_updates_never_rescan_other_relationships() -> None:
    h = Harness()
    h.open_books()
    a_key, z_key = h.key(GE3), h.key(Z3)
    a_state = h.engine.state(a_key)
    assert a_state is not None
    before = h.engine.stats.samples
    h.send(snap(Z3, 1, sid="z", conn="c2", yes=[("0.50", "10")]))
    h.send(delta(Z3, 2, Side.YES, "0.51", "5", sid="z", conn="c2"))
    # exactly one strategy (Z's) was sampled per Z message; A's state was not touched
    assert h.engine.stats.samples - before == 2
    assert h.engine.state(a_key) == a_state
    z_state = h.engine.state(z_key)
    assert z_state is not None and z_state.position == len(h.journal) - 1


def test_lifecycle_open_update_resolve_without_spam() -> None:
    h = Harness()
    _candidate(h)
    det_id = h.events[0].detection_id
    assert h.heartbeats(30) == []  # unchanged re-evaluations: no events
    rec = h.engine.active_detections()[0]
    assert rec.status is DetectionStatus.UPDATED
    assert rec.last_observed_ms == h.now
    assert rec.max_candidate_duration_ms is not None and rec.max_candidate_duration_ms >= 4000
    assert rec.max_deviation == rec.metrics.theoretical_deviation
    # a worse NO ask arrives first (best ask unchanged) -> nothing; then the 0.70 bid leaves
    assert h.send(delta(GE3, h.next_seq(), Side.YES, "0.30", "100")) == []
    resolved = h.send(delta(GE3, h.next_seq(), Side.YES, "0.70", "-100"))
    assert _kinds(resolved) == ["RESOLVED"]
    ev = resolved[0]
    assert ev.detection_id == det_id
    assert ev.close_reason is CloseReason.NO_PRE_FEE_EDGE
    assert ev.record.status is DetectionStatus.RESOLVED
    assert ev.record.peak_classification is Classification.FEE_ADJUSTED_CANDIDATE
    assert [e.seq for e in h.events] == [1, 2, 3]
    assert h.engine.active_detections() == []


def test_growing_maximum_updates_and_shrink_reports_current_capacity() -> None:
    h = Harness()
    _candidate(h)
    before = h.engine.active_detections()[0].max_capacity
    assert h.send(snap(GE3, h.next_seq(), yes=[("0.70", "300")])) == []  # GE2 still limits
    grew = h.send(snap(GE2, h.next_seq(), no=[("0.65", "300")]))
    assert _kinds(grew) == ["UPDATED"]
    cap = grew[0].record.max_capacity
    assert before is not None and cap is not None and cap > before
    assert cap == grew[0].record.metrics.depth_supported_quantity
    shrank = h.send(snap(GE2, h.next_seq(), no=[("0.65", "100")]))
    assert _kinds(shrank) == ["UPDATED"]
    rec = shrank[0].record
    assert rec.max_capacity == cap
    current = rec.metrics.depth_supported_quantity
    assert current is not None and current < cap
    assert h.engine.active_detections()[0].max_capacity == cap
    assert h.engine.active_detections()[0].metrics.depth_supported_quantity == current
    assert h.send(snap(GE2, h.next_seq(), no=[("0.65", "100")])) == []


def _with_net_edge(ev: Evaluation, net: str) -> Evaluation:
    quoted = ev.certificate.evaluation
    assert quoted is not None
    certificate = ev.certificate.model_copy(
        update={"evaluation": quoted.model_copy(update={"net_profit": dec(net)})}
    )
    return ev.model_copy(update={"certificate": certificate})


def test_decreasing_net_edge_reports_current_value_and_keeps_maximum() -> None:
    """A later, smaller net edge is the current value. The historical maximum stays."""
    h = Harness()
    _candidate(h)
    slot = h.engine._slots[h.key()]
    base = slot.evaluation
    assert base is not None and slot.detection is not None
    higher = _with_net_edge(base, "0.10")
    lower = _with_net_edge(base, "0.04")
    higher_metrics = DetectionMetrics.of(higher)
    slot.detection = slot.detection.model_copy(
        update={
            "max_deviation": higher_metrics.theoretical_deviation,
            "max_capacity": higher_metrics.depth_supported_quantity,
            "max_net_edge": dec("0.10"),
            "metrics": higher_metrics,
            "certificate_hash": higher.certificate_hash,
        }
    )
    timing = h.events[-1].timing
    decreased = h.engine._apply(slot, lower, h.now + 1, len(h.journal), timing)
    assert _kinds(decreased) == ["UPDATED"]
    rec = h.engine.active_detections()[0]
    assert rec.metrics.net_edge == dec("0.04")
    assert rec.max_net_edge == dec("0.10")
    assert decreased[0].record.metrics.net_edge == dec("0.04")
    assert decreased[0].record.max_net_edge == dec("0.10")
    again = h.engine._apply(slot, lower, h.now + 2, len(h.journal) + 1, timing)
    assert again == []
    rec = h.engine.active_detections()[0]
    assert rec.metrics.net_edge == dec("0.04")
    assert rec.max_net_edge == dec("0.10")


def test_sequence_gap_invalidates_immediately_and_duration_restarts() -> None:
    h = Harness()
    _candidate(h)
    h.seq["1"] += 1  # lose one message
    inv = h.send(delta(GE3, h.next_seq(), Side.YES, "0.71", "1"))
    assert _kinds(inv) == ["INVALIDATED"]
    assert inv[0].close_reason is CloseReason.BOOK_UNSYNCHRONIZED
    assert DesyncReason.SEQUENCE_GAP in (inv[0].close_detail or "")
    state = h.engine.state(h.key())
    assert state is not None
    assert state.classification is Classification.UNSYNCHRONIZED_DATA
    assert state.streak_since_ms is None
    # untrusted books never yield a signal, however long we wait
    assert h.heartbeats(20) == []
    # resubscription: a new detection that must re-earn the minimum duration
    h.seq["2"] = 0
    reopened = h.open_books(sid="2")
    assert _kinds(reopened) == ["OPENED"]
    assert reopened[0].record.ordinal == 2
    assert reopened[0].detection_id != inv[0].detection_id
    assert reopened[0].classification is Classification.DEPTH_SUPPORTED


def test_runner_loss_invalidates_and_replays_identically() -> None:
    h = Harness()
    _candidate(h)
    lost = h.mgr.connection_lost("c1", DesyncReason.CONNECTION_LOST)
    from consistency_pipeline.journal import ControlEvent

    control = ControlEvent(
        action="connection_lost",
        now_ms=h.now,
        reason=DesyncReason.CONNECTION_LOST,
        connection_ids=("c1",),
    )
    entry = JournalEntry(ordinal=len(h.journal), control=control)
    h.journal.append(entry)
    out = h.engine.on_message(entry.ordinal, control.now_ms, lost)
    h.events += out
    assert _kinds(out) == ["INVALIDATED"]
    assert out[0].close_reason is CloseReason.BOOK_UNSYNCHRONIZED
    assert DesyncReason.CONNECTION_LOST in (out[0].close_detail or "")

    mgr = BookManager(h.catalog.markets, source="unit")
    engine = DetectionEngine(mgr, h.rels, fee_calculator(), session_id="unit")
    replayed = JournalApplier(mgr, engine).apply_all(h.journal)
    assert [e.comparable() for e in replayed] == [e.comparable() for e in h.events]
    assert mgr.state_digest() == h.mgr.state_digest()


def test_stale_books_expire_the_detection_via_sweep() -> None:
    h = Harness()
    _candidate(h)
    # no more heartbeats for c1; unrelated traffic advances the pipeline clock
    h.send(snap(Z3, 1, sid="z", conn="c2", yes=[("0.50", "10")]), dt=2500)
    exp = [e for e in h.events if e.kind is EventKind.EXPIRED]
    assert len(exp) == 1
    assert exp[0].close_reason is CloseReason.STALE_DATA
    assert exp[0].timing.trigger == "sweep"
    state = h.engine.state(h.key())
    assert state is not None and state.classification is Classification.STALE_DATA


def test_tick_drives_expiry_without_messages() -> None:
    h = Harness()
    _candidate(h)
    out = h.engine.tick(len(h.journal), h.now + 2500)
    assert _kinds(out) == ["EXPIRED"]


def test_interrupted_update_invalidates_even_if_books_look_fine() -> None:
    h = Harness()
    _candidate(h)
    msg_updates = h.mgr.process(h.feed.msg(snap(GE3, h.next_seq(), yes=[("0.70", "100")])))
    interrupted = [dataclasses.replace(u, interrupted=True) for u in msg_updates]
    out = h.engine.on_message(99, h.now, interrupted)
    assert _kinds(out)[0] == "INVALIDATED"
    assert out[0].close_reason is CloseReason.BOOK_INTERRUPTED
    # the duration streak restarted: the follow-up detection is not yet a candidate
    assert _kinds(out)[1:] == ["OPENED"]
    assert out[1].classification is Classification.DEPTH_SUPPORTED


def test_market_status_change_expires_detection() -> None:
    h = Harness()
    _candidate(h)
    out = h.send(MarketStatusEvent(market_id=GE2, status=MarketStatus.CLOSED))
    assert _kinds(out) == ["EXPIRED"]
    assert out[0].close_reason is CloseReason.MARKET_NOT_OPEN


def test_review_rejection_invalidates_and_stops_scanning() -> None:
    h = Harness()
    _candidate(h)
    rel = h.relationship()
    rejected = rel.invalidate("review: rejected", T0 + timedelta(hours=1), needs_review=False)
    rels = [rejected if r.relationship_id == rel.relationship_id else r for r in h.rels]
    out = h.engine.set_relationships(rels, len(h.journal), h.now)
    assert _kinds(out) == ["INVALIDATED"]
    assert out[0].close_reason is CloseReason.RELATIONSHIP_INVALIDATED
    assert out[0].close_detail == "review: rejected"
    assert h.engine.strategy_keys_for_market(GE3) == []  # no longer scanned on book changes
    states = [s for s in h.engine.states() if s.relationship_id == rel.relationship_id]
    assert [s.classification for s in states] == [Classification.INVALID_RELATIONSHIP]
    assert states[0].reason_codes == ("RELATIONSHIP_NOT_VERIFIED",)
    before = h.engine.stats.samples
    assert h.heartbeats(3) == []
    assert h.send(delta(GE3, h.next_seq(), Side.YES, "0.75", "5")) == []
    assert h.engine.stats.samples == before


def test_rule_change_invalidates_detection() -> None:
    h = Harness()
    _candidate(h)
    changed = _catalog(ge2_rules="Amended rules for GOLD-A-GE2")
    rels = revalidate(h.rels, changed, as_of=T0 + timedelta(hours=1))
    out = h.engine.set_relationships(rels, len(h.journal), h.now)
    assert _kinds(out) == ["INVALIDATED"]
    assert out[0].close_reason is CloseReason.RELATIONSHIP_INVALIDATED
    assert "rules changed" in (out[0].close_detail or "")


def test_relationship_definition_change_reopens_cleanly() -> None:
    h = Harness()
    _candidate(h)
    rel = h.relationship()
    edited = rel.model_copy(update={"reasoning": rel.reasoning + " (re-reviewed)"})
    rels = [edited if r.relationship_id == rel.relationship_id else r for r in h.rels]
    out = h.engine.set_relationships(rels, len(h.journal), h.now)
    assert _kinds(out) == ["INVALIDATED", "OPENED"]
    assert out[0].close_reason is CloseReason.RELATIONSHIP_CHANGED
    assert out[1].classification is Classification.DEPTH_SUPPORTED


def test_unchanged_relationship_set_is_a_no_op() -> None:
    h = Harness()
    _candidate(h)
    assert h.engine.set_relationships(list(h.rels), len(h.journal), h.now) == []


def test_timing_separates_source_and_internal_latency() -> None:
    clock = iter(range(0, 10**12, 1_000))
    h = Harness(clock_ns=lambda: next(clock))
    opened = h.open_books()
    t = opened[0].timing
    assert t.trigger == "book_update"
    assert t.received_ts_ms is not None and t.exchange_ts_ms is not None
    assert t.source_latency_ms == t.received_ts_ms - t.exchange_ts_ms
    assert t.internal_latency_ns is not None and t.internal_latency_ns > 0
    assert "processing_started_ns" not in t.deterministic()


def test_duration_tracks_minimum_candidate_duration_semantics() -> None:
    h = Harness()
    h.open_books()
    start = h.now
    for _ in range(9):
        h.send(HeartbeatEvent(connection_id="c1"), dt=100)
        assert h.engine.active_detections()[0].classification is Classification.DEPTH_SUPPORTED
    out = h.send(HeartbeatEvent(connection_id="c1"), dt=100)
    assert h.now - start == 1000
    assert _kinds(out) == ["UPDATED"]
    assert out[0].evaluation is not None
    assert out[0].evaluation.certificate.timing.observed_duration_ms == 1000


@pytest.mark.parametrize("cut", [5, 17, 26])
def test_engine_state_round_trip_continues_identically(cut: int) -> None:
    h = Harness()
    h.open_books()
    h.heartbeats(30)
    h.send(delta(GE3, h.next_seq(), Side.YES, "0.70", "-100"))
    h.open_books(sid="2")
    full = [e.comparable() for e in h.events]
    # replay up to `cut`, export/restore, continue
    mgr = BookManager(h.catalog.markets, source="unit")
    engine = DetectionEngine(mgr, h.rels, fee_calculator(), session_id="unit")
    app = JournalApplier(mgr, engine)
    first = app.apply_all(h.journal[: cut + 1])
    state_m, state_e = mgr.export_state(), engine.export_state()
    mgr2 = BookManager.from_state(state_m)
    engine2 = DetectionEngine.from_state(state_e, mgr2, fee_calculator())
    app2 = JournalApplier(mgr2, engine2)
    app2.last_ordinal = cut
    rest = app2.apply_all(h.journal[cut + 1 :])
    assert [e.comparable() for e in first + rest] == full
    assert mgr2.state_digest() == h.mgr.state_digest()


def test_book_update_kind_desync_resets_streak() -> None:
    h = Harness()
    h.open_books()
    h.heartbeats(5)
    st = h.engine.state(h.key())
    assert st is not None and st.streak_since_ms is not None
    updates = h.mgr.connection_lost("c1", DesyncReason.HEARTBEAT_TIMEOUT)
    assert all(isinstance(u, BookUpdate) and u.kind == "desync" for u in updates)
    out = h.engine.on_message(len(h.journal), h.now, updates)
    assert _kinds(out) == ["INVALIDATED"]
    st = h.engine.state(h.key())
    assert st is not None and st.streak_since_ms is None

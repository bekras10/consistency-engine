"""Incremental detection engine (spec 10.1 / 10.2).

On each processed message the engine receives the ``BookUpdate`` notifications the
``BookManager`` produced and re-evaluates only the strategies that can be affected, found via
two indexes:

* ``market -> strategies`` over **leg** markets of **verified** relationships: book changes
  (snapshot, delta, desync) only affect evaluations whose legs read that book;
* ``market -> relationships`` over **members** of every relationship: market lifecycle changes
  (created / removed / status) re-evaluate every strategy of those relationships, verified or
  not. Unverified relationships are evaluated once at start, on lifecycle changes and on
  relationship-set changes, never on book changes: their result (INVALID_RELATIONSHIP, step 1)
  cannot depend on book state.

Gates run in the evaluator's precedence order (sync, freshness, constraint, portfolio,
classification); the engine never re-implements any of that math. It only decides *when* to
evaluate, tracks the duration condition, and folds evaluations into detection lifecycles.

Duration (``minimum_candidate_duration_ms``, mathematical-model §6.3 and golden I): the time a
strategy has continuously passed every gate except the duration gate, on the pipeline clock.
A strategy is sampled when one of its legs changes and, while it is *time-sensitive* (a
duration streak is running, it has an active detection, or it is STALE_DATA), at least every
``sweep_interval_ms`` of pipeline clock: a frozen book emits no deltas, but its condition still
persists (or ages out). Any break, including a desync or an interrupted (desync-then-resync)
update, resets the streak, so the minimum duration must elapse again on trusted data.

The engine is synchronous and takes the clock explicitly (``now_ms``); identical input
sequences give identical outputs, which is what makes journal replay reproducible.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from consistency_connectors.ingestion import BookManager, BookUpdate
from consistency_core.fees import FeeCalculator
from consistency_core.models.common import MarketStatus, SyncStatus
from consistency_core.models.detection import Classification
from consistency_core.models.market import Market
from consistency_core.models.orderbook import OrderBook
from consistency_core.models.relationship import Relationship
from consistency_core.pricing.certificate import EvaluationConfig
from consistency_core.pricing.evaluator import Evaluation, evaluate
from consistency_core.pricing.portfolio import Portfolio, canonical_portfolios
from consistency_pipeline.lifecycle import (
    CLOSE_STATUS,
    EVENT_FOR_STATUS,
    CloseReason,
    DetectionEvent,
    DetectionMetrics,
    DetectionRecord,
    DetectionStatus,
    EventKind,
    StrategyState,
    TimingMeta,
    close_reason_for,
    detection_id,
    is_signal,
    passes_all_but_duration,
    strategy_key,
)

DEFAULT_SWEEP_INTERVAL_MS = 100
_RANK = {c: i for i, c in enumerate(Classification)}
_LIFECYCLE_KINDS = frozenset({"status", "removed"})


@dataclass
class _Slot:
    key: str
    relationship: Relationship
    portfolio: Portfolio
    legs: tuple[str, ...]
    streak_since: int | None = None
    last_eval_ms: int | None = None
    state: StrategyState | None = None
    detection: DetectionRecord | None = None
    evaluation: Evaluation | None = None
    """Latest evaluation (a cache for read APIs; not part of the checkpointed state)."""


@dataclass
class EngineStats:
    messages: int = 0
    evaluations: int = 0
    samples: int = 0
    sweep_samples: int = 0
    events: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "messages": self.messages,
            "evaluations": self.evaluations,
            "samples": self.samples,
            "sweep_samples": self.sweep_samples,
            "events": dict(sorted(self.events.items())),
        }


class DetectionEngine:
    def __init__(
        self,
        manager: BookManager,
        relationships: Iterable[Relationship],
        fees: FeeCalculator,
        *,
        session_id: str,
        config: EvaluationConfig | None = None,
        sweep_interval_ms: int = DEFAULT_SWEEP_INTERVAL_MS,
        clock_ns: Callable[[], int] = time.perf_counter_ns,
    ) -> None:
        if sweep_interval_ms <= 0:
            raise ValueError("sweep_interval_ms must be positive")
        self.manager = manager
        self.fees = fees
        self.session_id = session_id
        self.config = config or EvaluationConfig()
        self.sweep_interval_ms = sweep_interval_ms
        self._clock_ns = clock_ns
        self._relationships: dict[str, Relationship] = {}
        self._slots: dict[str, _Slot] = {}
        self._by_leg: dict[str, list[str]] = {}
        self._by_member: dict[str, list[str]] = {}
        self._rel_slots: dict[str, list[str]] = {}
        self._time_sensitive: set[str] = set()
        self._ordinals: dict[str, int] = {}
        self._market_cache: dict[tuple[str, MarketStatus], tuple[Market, Market]] = {}
        self._primed = False
        self.stats = EngineStats()
        self._install({r.relationship_id: r for r in relationships}, keep=False)

    # ------------------------------------------------------------------ read API
    @property
    def relationships(self) -> list[Relationship]:
        return [self._relationships[k] for k in sorted(self._relationships)]

    def relationship_ids_for_market(self, market_id: str) -> list[str]:
        return list(self._by_member.get(market_id, ()))

    def strategy_keys_for_market(self, market_id: str) -> list[str]:
        return list(self._by_leg.get(market_id, ()))

    def strategy_keys(self) -> list[str]:
        return sorted(self._slots)

    def states(self) -> list[StrategyState]:
        return [s.state for _, s in sorted(self._slots.items()) if s.state is not None]

    def strategy_key_for(self, relationship_id: str, portfolio: Portfolio) -> str:
        """Key of the canonical strategy holding the same legs as ``portfolio`` (any order)."""
        want = (portfolio.template, sorted(portfolio.legs, key=repr))
        for key in self._rel_slots.get(relationship_id, ()):
            pf = self._slots[key].portfolio
            if (pf.template, sorted(pf.legs, key=repr)) == want:
                return key
        raise KeyError(f"{relationship_id}: no canonical strategy {portfolio.strategy_id}")

    def state(self, key: str) -> StrategyState | None:
        return self._slots[key].state

    def active_detections(self) -> list[DetectionRecord]:
        return [
            s.detection
            for _, s in sorted(self._slots.items())
            if s.detection is not None and s.detection.status.active
        ]

    # ------------------------------------------------------------------ inputs
    def on_message(
        self, position: int, now_ms: int, updates: Sequence[BookUpdate]
    ) -> list[DetectionEvent]:
        """Process the updates one message (or runner loss) produced, then sweep."""
        self.stats.messages += 1
        events: list[DetectionEvent] = []
        if not self._primed:
            self._primed = True
            for key in sorted(self._slots):
                if not self._slots[key].relationship.is_verified:
                    events += self._sample(
                        self._slots[key], now_ms, position, TimingMeta(trigger="book_update")
                    )
        pending: dict[str, TimingMeta] = {}
        for u in updates:
            leg_keys = self._by_leg.get(u.market_id, ())
            if u.kind == "desync" or u.sync_status is not SyncStatus.SYNCHRONIZED or u.interrupted:
                for key in leg_keys:
                    self._slots[key].streak_since = None
            timing = _timing_of(u)
            if u.interrupted:
                for key in leg_keys:
                    slot = self._slots[key]
                    if slot.detection is not None and slot.detection.status.active:
                        events.append(
                            self._close(
                                slot,
                                CloseReason.BOOK_INTERRUPTED,
                                f"{u.market_id}: desynchronized and resynchronized before "
                                "evaluation",
                                now_ms,
                                position,
                                timing,
                                None,
                            )
                        )
            keys = list(leg_keys)
            if u.kind in _LIFECYCLE_KINDS:
                for rid in self._by_member.get(u.market_id, ()):
                    keys.extend(self._rel_slots[rid])
            for key in keys:
                pending.setdefault(key, timing)
        for key, timing in pending.items():
            events += self._sample(self._slots[key], now_ms, position, timing)
        events += self._sweep(now_ms, position, exclude=pending.keys())
        return events

    def tick(self, position: int, now_ms: int) -> list[DetectionEvent]:
        """Clock-only progress (live sources): sweep time-sensitive strategies."""
        return self._sweep(now_ms, position, exclude=())

    def probe(
        self, key: str, position: int, now_ms: int
    ) -> tuple[StrategyState, list[DetectionEvent]]:
        """Sample one strategy now (exactly like a timer firing for it); returns its state and
        any lifecycle events the sample produced (callers must forward them)."""
        slot = self._slots[key]
        events = self._sample(
            slot, now_ms, position, TimingMeta(trigger="probe", received_ts_ms=now_ms)
        )
        assert slot.state is not None
        return slot.state, events

    def peek(self, key: str, now_ms: int) -> Evaluation:
        """What a sample at ``now_ms`` would evaluate, without changing any state."""
        slot = self._slots[key]
        if slot.streak_since is not None:
            ev = self._evaluate(slot, now_ms, now_ms - slot.streak_since)
            if passes_all_but_duration(ev):
                return ev
        ev = self._evaluate(slot, now_ms, None)
        return self._evaluate(slot, now_ms, 0) if passes_all_but_duration(ev) else ev

    def probe_evaluation(
        self, key: str, position: int, now_ms: int
    ) -> tuple[Evaluation, list[DetectionEvent]]:
        """Like :meth:`probe`, returning the evaluation itself and any lifecycle events."""
        slot = self._slots[key]
        events = self._sample(
            slot, now_ms, position, TimingMeta(trigger="probe", received_ts_ms=now_ms)
        )
        assert slot.evaluation is not None
        return slot.evaluation, events

    def set_relationships(
        self, relationships: Iterable[Relationship], position: int, now_ms: int
    ) -> list[DetectionEvent]:
        """Install a new relationship set (review decisions, rule changes). Active detections of
        relationships that lost verification, changed or disappeared are invalidated at once;
        every strategy of a changed or new relationship is re-evaluated immediately."""
        new = {r.relationship_id: r for r in relationships}
        old = self._relationships
        changed = sorted(rid for rid in set(old) | set(new) if old.get(rid) != new.get(rid))
        events: list[DetectionEvent] = []
        timing = TimingMeta(trigger="relationship_change", received_ts_ms=now_ms)
        for rid in changed:
            n = new.get(rid)
            for key in self._rel_slots.get(rid, ()):
                slot = self._slots[key]
                slot.streak_since = None
                if slot.detection is not None and slot.detection.status.active:
                    if n is None or not n.is_verified:
                        reason = CloseReason.RELATIONSHIP_INVALIDATED
                        detail = (
                            "relationship removed"
                            if n is None
                            else (n.invalidation_reason or f"status {n.verification_status}")
                        )
                    else:
                        reason = CloseReason.RELATIONSHIP_CHANGED
                        detail = "relationship definition changed"
                    events.append(self._close(slot, reason, detail, now_ms, position, timing, None))
        self._install(new, keep=True, changed=set(changed))
        for rid in changed:
            for key in self._rel_slots.get(rid, ()):
                events += self._sample(self._slots[key], now_ms, position, timing)
        return events

    def close_all(
        self, reason: CloseReason, position: int, now_ms: int, *, detail: str | None = None
    ) -> list[DetectionEvent]:
        timing = TimingMeta(
            trigger="restart" if reason is CloseReason.SERVICE_RESTART else "session_end",
            received_ts_ms=now_ms,
        )
        events = []
        for key in sorted(self._slots):
            slot = self._slots[key]
            slot.streak_since = None
            if slot.detection is not None and slot.detection.status.active:
                events.append(
                    self._close(
                        slot, reason, detail or reason.value, now_ms, position, timing, None
                    )
                )
            self._refresh_time_sensitive(slot)
        return events

    # ------------------------------------------------------------------ internals
    def _install(
        self, rels: dict[str, Relationship], *, keep: bool, changed: set[str] | None = None
    ) -> None:
        old_slots = self._slots
        self._relationships = dict(rels)
        self._slots = {}
        self._rel_slots = {}
        by_leg: dict[str, set[str]] = {}
        by_member: dict[str, set[str]] = {}
        for rid in sorted(rels):
            rel = rels[rid]
            keys = []
            for pf in canonical_portfolios(rel):
                key = strategy_key(rid, pf.strategy_id)
                prev = old_slots.get(key) if keep and rid not in (changed or set()) else None
                slot = prev or _Slot(
                    key=key,
                    relationship=rel,
                    portfolio=pf,
                    legs=tuple(leg.market_id for leg in pf.legs),
                )
                self._slots[key] = slot
                keys.append(key)
                if rel.is_verified:
                    for mid in slot.legs:
                        by_leg.setdefault(mid, set()).add(key)
            self._rel_slots[rid] = keys
            for mid in rel.members:
                by_member.setdefault(mid, set()).add(rid)
        self._by_leg = {m: sorted(v) for m, v in sorted(by_leg.items())}
        self._by_member = {m: sorted(v) for m, v in sorted(by_member.items())}
        self._time_sensitive = set()
        for slot in self._slots.values():
            self._refresh_time_sensitive(slot)

    def _refresh_time_sensitive(self, slot: _Slot) -> None:
        sensitive = slot.relationship.is_verified and (
            slot.streak_since is not None
            or (slot.detection is not None and slot.detection.status.active)
            or (slot.state is not None and slot.state.classification is Classification.STALE_DATA)
        )
        if sensitive:
            self._time_sensitive.add(slot.key)
        else:
            self._time_sensitive.discard(slot.key)

    def _sweep(self, now_ms: int, position: int, *, exclude: Iterable[str]) -> list[DetectionEvent]:
        skip = set(exclude)
        events: list[DetectionEvent] = []
        for key in sorted(self._time_sensitive - skip):
            slot = self._slots[key]
            if slot.last_eval_ms is None or now_ms - slot.last_eval_ms >= self.sweep_interval_ms:
                self.stats.sweep_samples += 1
                events += self._sample(
                    slot, now_ms, position, TimingMeta(trigger="sweep", received_ts_ms=now_ms)
                )
        return events

    def _market(self, market_id: str) -> Market:
        m = self.manager.market(market_id)
        status = self.manager.market_status(market_id)
        if m.status is status:
            return m
        cached = self._market_cache.get((market_id, status))
        if cached is not None and cached[0] is m:
            return cached[1]
        copy = m.model_copy(update={"status": status})
        self._market_cache[(market_id, status)] = (m, copy)
        return copy

    def _evaluate(self, slot: _Slot, now_ms: int, duration: int | None) -> Evaluation:
        rel = slot.relationship
        has = self.manager.has_market
        markets = {mid: self._market(mid) for mid in rel.members if has(mid)}
        books: dict[str, OrderBook | None] = {
            mid: self.manager.book(mid) if has(mid) else None for mid in slot.legs
        }
        self.stats.evaluations += 1
        return evaluate(
            rel,
            slot.portfolio,
            markets=markets,
            books=books,
            now_ms=now_ms,
            fees=self.fees,
            config=self.config,
            observed_duration_ms=duration,
        )

    def _sample(
        self, slot: _Slot, now_ms: int, position: int, timing: TimingMeta
    ) -> list[DetectionEvent]:
        self.stats.samples += 1
        started = timing.processing_started_ns or self._clock_ns()
        if slot.streak_since is None:
            ev = self._evaluate(slot, now_ms, None)
            if passes_all_but_duration(ev):
                slot.streak_since = now_ms
                ev = self._evaluate(slot, now_ms, 0)
        else:
            ev = self._evaluate(slot, now_ms, now_ms - slot.streak_since)
            if not passes_all_but_duration(ev):
                slot.streak_since = None
                ev = self._evaluate(slot, now_ms, None)
        slot.last_eval_ms = now_ms
        slot.evaluation = ev
        timing = timing.model_copy(
            update={"processing_started_ns": started, "detection_completed_ns": self._clock_ns()}
        )
        events = self._apply(slot, ev, now_ms, position, timing)
        det = slot.detection
        slot.state = StrategyState(
            key=slot.key,
            relationship_id=slot.relationship.relationship_id,
            strategy_id=slot.portfolio.strategy_id,
            classification=ev.classification,
            reason_codes=ev.reason_codes,
            signal=is_signal(ev),
            evaluated_at_ms=now_ms,
            position=position,
            certificate_hash=ev.certificate_hash,
            streak_since_ms=slot.streak_since,
            active_detection_id=det.detection_id if det is not None and det.status.active else None,
        )
        self._refresh_time_sensitive(slot)
        return events

    def _apply(
        self, slot: _Slot, ev: Evaluation, now_ms: int, position: int, timing: TimingMeta
    ) -> list[DetectionEvent]:
        signal = is_signal(ev)
        det = slot.detection
        streak_ms = (
            ev.certificate.timing.observed_duration_ms if slot.streak_since is not None else None
        )
        if det is None or not det.status.active:
            if not signal:
                return []
            ordinal = self._ordinals.get(slot.key, 0) + 1
            self._ordinals[slot.key] = ordinal
            metrics = DetectionMetrics.of(ev)
            rec = DetectionRecord(
                detection_id=detection_id(
                    self.session_id,
                    slot.relationship.relationship_id,
                    slot.portfolio.strategy_id,
                    position,
                    ordinal,
                ),
                session_id=self.session_id,
                relationship_id=slot.relationship.relationship_id,
                strategy_id=slot.portfolio.strategy_id,
                template=slot.portfolio.template.value,
                ordinal=ordinal,
                status=DetectionStatus.OPEN,
                classification=ev.classification,
                reason_codes=ev.reason_codes,
                peak_classification=ev.classification,
                first_observed_ms=now_ms,
                last_observed_ms=now_ms,
                first_position=position,
                last_position=position,
                max_deviation=metrics.theoretical_deviation,
                max_capacity=metrics.depth_supported_quantity,
                max_net_edge=metrics.net_edge,
                max_candidate_duration_ms=streak_ms,
                certificate_hash=ev.certificate_hash,
                certificate_json=ev.certificate_json(),
                certificate_version=ev.certificate.certificate_version,
                metrics=metrics,
                timing=timing,
            )
            slot.detection = rec
            return [self._event(rec, EventKind.OPENED, now_ms, position, ev, timing)]
        if not signal:
            reason = close_reason_for(ev)
            detail = self._close_detail(slot, ev, reason)
            return [self._close(slot, reason, detail, now_ms, position, timing, ev)]
        metrics = DetectionMetrics.of(ev)
        max_dev = _max(det.max_deviation, metrics.theoretical_deviation)
        max_cap = _max(det.max_capacity, metrics.depth_supported_quantity)
        max_net = _max(det.max_net_edge, metrics.net_edge)
        max_dur = det.max_candidate_duration_ms
        if streak_ms is not None:
            max_dur = streak_ms if max_dur is None else max(max_dur, streak_ms)
        base: dict[str, Any] = {
            "last_observed_ms": now_ms,
            "last_position": position,
            "max_candidate_duration_ms": max_dur,
        }
        changed = (ev.classification, ev.reason_codes) != (det.classification, det.reason_codes)
        grew = (max_dev, max_cap, max_net) != (
            det.max_deviation,
            det.max_capacity,
            det.max_net_edge,
        )
        # Clock fields (duration, freshness, skew) move on every sample. They are not a new
        # observation of the candidate. A repeated current quote emits nothing; a real change,
        # including a smaller net edge, does.
        current_changed = _quoted(metrics) != _quoted(det.metrics)
        if not (changed or grew or current_changed):
            slot.detection = det.model_copy(update=base)
            return []
        peak = max(det.peak_classification, ev.classification, key=lambda c: _RANK[c])
        rec = det.model_copy(
            update={
                **base,
                "status": DetectionStatus.UPDATED,
                "classification": ev.classification,
                "reason_codes": ev.reason_codes,
                "peak_classification": peak,
                "max_deviation": max_dev,
                "max_capacity": max_cap,
                "max_net_edge": max_net,
                "update_count": det.update_count + 1,
                "event_count": det.event_count + 1,
                "certificate_hash": ev.certificate_hash,
                "certificate_json": ev.certificate_json(),
                "certificate_version": ev.certificate.certificate_version,
                "metrics": metrics,
                "timing": timing,
            }
        )
        slot.detection = rec
        return [self._event(rec, EventKind.UPDATED, now_ms, position, ev, timing)]

    def _close(
        self,
        slot: _Slot,
        reason: CloseReason,
        detail: str,
        now_ms: int,
        position: int,
        timing: TimingMeta,
        ev: Evaluation | None,
    ) -> DetectionEvent:
        det = slot.detection
        assert det is not None and det.status.active
        status = CLOSE_STATUS[reason]
        rec = det.model_copy(
            update={
                "status": status,
                "closed_at_ms": now_ms,
                "close_reason": reason,
                "close_detail": detail,
                "event_count": det.event_count + 1,
                "timing": timing,
            }
        )
        slot.detection = rec
        self._refresh_time_sensitive(slot)
        return self._event(rec, EVENT_FOR_STATUS[status], now_ms, position, ev, timing)

    def _event(
        self,
        rec: DetectionRecord,
        kind: EventKind,
        now_ms: int,
        position: int,
        ev: Evaluation | None,
        timing: TimingMeta,
    ) -> DetectionEvent:
        self.stats.events[kind.value] = self.stats.events.get(kind.value, 0) + 1
        return DetectionEvent(
            detection_id=rec.detection_id,
            seq=rec.event_count,
            kind=kind,
            at_ms=now_ms,
            position=position,
            classification=None if ev is None else ev.classification,
            reason_codes=() if ev is None else ev.reason_codes,
            close_reason=rec.close_reason if not rec.status.active else None,
            close_detail=rec.close_detail if not rec.status.active else None,
            certificate_hash=None if ev is None else ev.certificate_hash,
            record=rec,
            evaluation=ev,
            timing=timing,
        )

    def _close_detail(self, slot: _Slot, ev: Evaluation, reason: CloseReason) -> str:
        codes = ",".join(ev.reason_codes)
        if reason is CloseReason.BOOK_UNSYNCHRONIZED:
            parts = []
            for mid in slot.legs:
                if not self.manager.has_market(mid):
                    parts.append(f"{mid}:missing")
                elif self.manager.sync_status(mid) is not SyncStatus.SYNCHRONIZED:
                    parts.append(
                        f"{mid}:{self.manager.sync_status(mid).value}:"
                        f"{self.manager.sync_reason(mid) or 'unknown'}"
                    )
            return f"{codes}; " + "; ".join(parts)
        if reason is CloseReason.STALE_DATA:
            t = ev.certificate.timing
            return f"{codes}; max_book_age_ms={t.max_book_age_ms}; skew_ms={t.cross_market_skew_ms}"
        return codes

    # ------------------------------------------------------------------ checkpoints
    def export_state(self) -> dict[str, Any]:
        """JSON-serialisable engine state; with the manager state it fully determines future
        outputs (wall-clock telemetry inside records is carried but never compared)."""
        return {
            "session_id": self.session_id,
            "sweep_interval_ms": self.sweep_interval_ms,
            "config": self.config.model_dump(mode="json"),
            "relationships": [r.model_dump(mode="json") for r in self.relationships],
            "ordinals": dict(sorted(self._ordinals.items())),
            "primed": self._primed,
            "slots": {
                key: {
                    "streak_since": s.streak_since,
                    "last_eval_ms": s.last_eval_ms,
                    "state": None if s.state is None else s.state.model_dump(mode="json"),
                    "detection": None
                    if s.detection is None
                    else s.detection.model_dump(mode="json"),
                }
                for key, s in sorted(self._slots.items())
            },
        }

    @classmethod
    def from_state(
        cls,
        state: dict[str, Any],
        manager: BookManager,
        fees: FeeCalculator,
        *,
        clock_ns: Callable[[], int] = time.perf_counter_ns,
    ) -> DetectionEngine:
        engine = cls(
            manager,
            [Relationship.model_validate(r) for r in state["relationships"]],
            fees,
            session_id=state["session_id"],
            config=EvaluationConfig.model_validate(state["config"]),
            sweep_interval_ms=state["sweep_interval_ms"],
            clock_ns=clock_ns,
        )
        engine._ordinals = dict(state["ordinals"])
        engine._primed = state["primed"]
        for key, s in state["slots"].items():
            slot = engine._slots[key]
            slot.streak_since = s["streak_since"]
            slot.last_eval_ms = s["last_eval_ms"]
            slot.state = None if s["state"] is None else StrategyState.model_validate(s["state"])
            slot.detection = (
                None if s["detection"] is None else DetectionRecord.model_validate(s["detection"])
            )
        engine._time_sensitive = set()
        for slot in engine._slots.values():
            engine._refresh_time_sensitive(slot)
        return engine


def _timing_of(u: BookUpdate) -> TimingMeta:
    return TimingMeta(
        trigger="book_update",
        market_id=u.market_id,
        connection_id=u.connection_id,
        subscription_id=u.subscription_id,
        source_sequence=u.source_sequence,
        exchange_ts_ms=u.timing.exchange_ts_ms,
        received_ts_ms=u.timing.received_ts_ms,
        processing_started_ns=u.timing.processing_started_ns,
    )


def _quoted(metrics: DetectionMetrics) -> tuple[object, ...]:
    """Latest economic figures. Historical maxima live on the detection, not here."""
    return (
        metrics.theoretical_deviation,
        metrics.gross_edge,
        metrics.total_fees,
        metrics.net_edge,
        metrics.execution_adjusted_edge,
        metrics.reported_quantity,
        metrics.depth_supported_quantity,
        metrics.worst_case_payoff,
    )


def _max[T: Any](a: T | None, b: T | None) -> T | None:
    if a is None:
        return b
    if b is None:
        return a
    return b if b > a else a

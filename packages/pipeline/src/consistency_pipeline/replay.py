"""Deterministic replay (spec 12.2 / 12.4).

Replay order is the journal ``ordinal``: the order the live pipeline processed entries.
Restoring the latest checkpoint whose clock is at or before a timestamp, then applying every
later entry that is still at or before that timestamp, matches replaying from the start up to
the same timestamp. Wall-clock telemetry (``processing_started_ns``,
``detection_completed_ns``) is excluded from every comparison.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from consistency_connectors.ingestion import BookManager
from consistency_core.fees import FeeCalculator
from consistency_core.models.market import Catalog, Market
from consistency_core.models.relationship import Relationship
from consistency_pipeline import checkpoint as cp
from consistency_pipeline.checkpoint import Checkpoint
from consistency_pipeline.driver import JournalApplier
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.journal import JournalEntry
from consistency_pipeline.lifecycle import DetectionEvent

TELEMETRY_EXCLUDED: tuple[str, ...] = ("processing_started_ns", "detection_completed_ns")


@dataclass
class ReplayOutcome:
    events: list[DetectionEvent]
    manager: BookManager
    engine: DetectionEngine

    @property
    def book_digest(self) -> str:
        return self.manager.state_digest()

    @property
    def state_digest(self) -> str:
        return cp.state_digest(self.manager.export_state(), self.engine.export_state())

    def comparable_events(self) -> list[dict[str, object]]:
        return [ev.comparable() for ev in self.events]

    def classifications(self) -> list[tuple[str, str, str]]:
        return [
            (state.key, state.classification.value, state.certificate_hash)
            for state in self.engine.states()
        ]


@dataclass
class ReplayComparison:
    identical: bool
    event_count: int
    book_match: bool
    classification_mismatches: list[str] = field(default_factory=list)
    certificate_mismatches: list[str] = field(default_factory=list)
    event_mismatches: int = 0
    telemetry_excluded: tuple[str, ...] = TELEMETRY_EXCLUDED


def replay_entries(
    entries: list[JournalEntry],
    markets: list[Market] | tuple[Market, ...],
    relationships: list[Relationship],
    fees: FeeCalculator,
    *,
    session_id: str,
    checkpoint: Checkpoint | None = None,
    through_ms: int | None = None,
) -> ReplayOutcome:
    """Apply ``entries`` in ordinal order.

    ``through_ms`` keeps entries whose pipeline clock is ``<= through_ms``. With a checkpoint,
    only entries after the checkpoint ordinal are applied (the checkpoint already includes
    everything up to its ordinal).
    """
    ordered = sorted(entries, key=lambda e: e.ordinal)
    if checkpoint is None:
        manager = BookManager(markets, source="replay")
        engine = DetectionEngine(manager, relationships, fees, session_id=session_id)
        applier = JournalApplier(manager, engine)
        selected = ordered
    else:
        manager, engine = cp.restore(checkpoint, fees)
        applier = JournalApplier(manager, engine)
        applier.last_ordinal = checkpoint.ordinal
        selected = [e for e in ordered if e.ordinal > checkpoint.ordinal]
    if through_ms is not None:
        selected = [e for e in selected if e.now_ms <= through_ms]
    events = applier.apply_all(selected)
    return ReplayOutcome(events=events, manager=manager, engine=engine)


def replay_catalog(
    entries: list[JournalEntry],
    catalog: Catalog,
    relationships: list[Relationship],
    fees: FeeCalculator,
    *,
    session_id: str,
    checkpoint: Checkpoint | None = None,
    through_ms: int | None = None,
) -> ReplayOutcome:
    return replay_entries(
        entries,
        catalog.markets,
        relationships,
        fees,
        session_id=session_id,
        checkpoint=checkpoint,
        through_ms=through_ms,
    )


def compare_outcomes(left: ReplayOutcome, right: ReplayOutcome) -> ReplayComparison:
    left_cls = {key: (cls, cert) for key, cls, cert in left.classifications()}
    right_cls = {key: (cls, cert) for key, cls, cert in right.classifications()}
    class_bad: list[str] = []
    cert_bad: list[str] = []
    for key in sorted(set(left_cls) | set(right_cls)):
        a = left_cls.get(key)
        b = right_cls.get(key)
        if a is None or b is None or a[0] != b[0]:
            class_bad.append(key)
        elif a[1] != b[1]:
            cert_bad.append(key)
    left_events = left.comparable_events()
    right_events = right.comparable_events()
    mismatches = sum(1 for a, b in zip(left_events, right_events, strict=False) if a != b)
    mismatches += abs(len(left_events) - len(right_events))
    book_match = left.book_digest == right.book_digest
    identical = book_match and not class_bad and not cert_bad and mismatches == 0
    return ReplayComparison(
        identical=identical,
        event_count=len(left_events),
        book_match=book_match,
        classification_mismatches=class_bad,
        certificate_mismatches=cert_bad,
        event_mismatches=mismatches,
    )


def latest_checkpoint_through(checkpoints: list[Checkpoint], through_ms: int) -> Checkpoint | None:
    eligible = [c for c in checkpoints if c.now_ms <= through_ms]
    if not eligible:
        return None
    return max(eligible, key=lambda c: c.ordinal)

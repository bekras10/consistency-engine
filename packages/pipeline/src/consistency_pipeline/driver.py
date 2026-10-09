"""Deterministic, synchronous application of journal entries.

:class:`JournalApplier` performs exactly the state transitions the live runner + listener
performed for each entry, in the same order: ``BookManager.process`` then draining recovery
requests (as the runner does before notifying its listener), runner-originated losses via
``connection_lost`` / ``recovery_failed``, relationship changes, clock ticks and session end.
It is used by replay, by checkpoint resume and by tests; the live path produces the journal.
"""

from __future__ import annotations

from collections.abc import Iterable

from consistency_connectors.ingestion import BookManager, BookUpdate
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.journal import JournalEntry
from consistency_pipeline.lifecycle import CloseReason, DetectionEvent


class JournalApplier:
    def __init__(self, manager: BookManager, engine: DetectionEngine) -> None:
        self.manager = manager
        self.engine = engine
        self.last_ordinal = -1
        self.now_ms: int | None = None

    def apply(self, entry: JournalEntry) -> list[DetectionEvent]:
        if entry.ordinal <= self.last_ordinal:
            raise ValueError(f"journal ordinal {entry.ordinal} after {self.last_ordinal}")
        self.last_ordinal = entry.ordinal
        self.now_ms = entry.now_ms
        if entry.message is not None:
            updates = self.manager.process(entry.message)
            self.manager.drain_recovery_requests()
            return self.engine.on_message(entry.ordinal, entry.message.received_ts_ms, updates)
        c = entry.control
        assert c is not None
        match c.action:
            case "connection_lost":
                assert c.reason is not None
                lost: list[BookUpdate] = []
                for conn in c.connection_ids:
                    lost.extend(self.manager.connection_lost(conn, c.reason))
                return self.engine.on_message(entry.ordinal, c.now_ms, lost)
            case "recovery_failed":
                assert c.reason is not None and c.recovery_request is not None
                failed = self.manager.recovery_failed(c.recovery_request, c.reason)
                return self.engine.on_message(entry.ordinal, c.now_ms, failed)
            case "relationships_changed":
                assert c.relationships is not None
                return self.engine.set_relationships(c.relationships, entry.ordinal, c.now_ms)
            case "tick":
                return self.engine.tick(entry.ordinal, c.now_ms)
            case "session_end":
                return self.engine.close_all(
                    CloseReason(c.reason or CloseReason.SESSION_ENDED.value),
                    entry.ordinal,
                    c.now_ms,
                )

    def apply_all(self, entries: Iterable[JournalEntry]) -> list[DetectionEvent]:
        out: list[DetectionEvent] = []
        for e in entries:
            out += self.apply(e)
        return out

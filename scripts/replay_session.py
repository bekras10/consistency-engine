"""Replay the bundled inconsistent session and print the comparison.

Deterministic replay order is the journal ordinal (processing order), not wall-clock time.
``processing_started_ns`` and ``detection_completed_ns`` are telemetry and are excluded.
"""

from __future__ import annotations

import sys
from pathlib import Path

from consistency_connectors.ingestion import BookManager
from consistency_core.fees import FeeCalculator
from consistency_core.models.market import Catalog
from consistency_core.models.relationship import Relationship
from consistency_core.pricing.portfolio import parse_strategy_id
from consistency_pipeline import checkpoint as cp
from consistency_pipeline.driver import JournalApplier
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.journal import JournalEntry
from consistency_pipeline.replay import (
    TELEMETRY_EXCLUDED,
    compare_outcomes,
    latest_checkpoint_through,
    replay_catalog,
)
from consistency_simulation.datasets import load
from consistency_simulation.families import SIM_EPOCH
from consistency_worker.bootstrap import fee_calculator, relationships_for

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    dataset = ROOT / "fixtures/datasets/inconsistent"
    reviews = ROOT / "fixtures/relationships/manual-reviews.yaml"
    loaded = load(dataset)
    relationships = relationships_for(loaded.catalog, reviews, as_of=SIM_EPOCH)
    fees = fee_calculator(ROOT / "fixtures/fees")
    entries = [JournalEntry(ordinal=i, message=msg) for i, msg in enumerate(loaded.messages)]
    first = replay_catalog(entries, loaded.catalog, relationships, fees, session_id="replay-a")
    second = replay_catalog(entries, loaded.catalog, relationships, fees, session_id="replay-a")
    compared = compare_outcomes(first, second)
    print(f"dataset: {dataset.name} entries: {len(entries)}")
    print(f"same recording twice: {'IDENTICAL' if compared.identical else 'DIFFERENT'}")
    print(f"  events: {compared.event_count}")
    print(f"  books: {'match' if compared.book_match else 'DIFFER'}")
    print(f"  classification mismatches: {len(compared.classification_mismatches)}")
    print(f"  certificate mismatches: {len(compared.certificate_mismatches)}")
    print(f"  event mismatches: {compared.event_mismatches}")
    print(f"  telemetry excluded: {', '.join(TELEMETRY_EXCLUDED)}")
    seek_ok = _seek_matches(entries, loaded.catalog, relationships, fees)
    print(f"seek via checkpoint: {'IDENTICAL' if seek_ok else 'DIFFERENT'}")
    print("scenario classifications at evaluate_at_ms:")
    ok = True
    for scenario_id, expected, actual in _scenarios(entries, loaded.catalog, relationships, fees):
        flag = "ok" if actual == expected else "DIFFERENT"
        if actual != expected:
            ok = False
        print(f"  {scenario_id}: {actual} (expected {expected}) {flag}")
    return 0 if compared.identical and seek_ok and ok else 1


def _seek_matches(
    entries: list[JournalEntry],
    catalog: Catalog,
    relationships: list[Relationship],
    fees: FeeCalculator,
) -> bool:
    mid = entries[len(entries) // 2]
    manager = BookManager(catalog.markets, source="replay")
    engine = DetectionEngine(manager, relationships, fees, session_id="seek")
    applier = JournalApplier(manager, engine)
    for entry in entries:
        if entry.ordinal > mid.ordinal:
            break
        applier.apply(entry)
    position = None if mid.message is None else mid.message.position
    checkpoint = cp.take("seek", mid.ordinal, mid.now_ms, position, manager, engine)
    chosen = latest_checkpoint_through([checkpoint], mid.now_ms)
    via = replay_catalog(
        entries,
        catalog,
        relationships,
        fees,
        session_id="seek",
        checkpoint=chosen,
        through_ms=mid.now_ms,
    )
    direct = replay_catalog(
        entries, catalog, relationships, fees, session_id="seek", through_ms=mid.now_ms
    )
    return via.state_digest == direct.state_digest


def _scenarios(
    entries: list[JournalEntry],
    catalog: Catalog,
    relationships: list[Relationship],
    fees: FeeCalculator,
) -> list[tuple[str, str, str]]:
    dataset = load(ROOT / "fixtures/datasets/inconsistent")
    manager = BookManager(catalog.markets, source="replay")
    engine = DetectionEngine(manager, relationships, fees, session_id="peek")
    loaded_findings: list[tuple[int, str, str, str]] = []
    for finding in dataset.metadata["expected_findings"]:
        members = set(finding["members"])
        hits = [
            rel
            for rel in relationships
            if rel.relationship_type.value == finding["relationship_type"]
            and set(rel.members) == members
        ]
        key = engine.strategy_key_for(
            hits[0].relationship_id, parse_strategy_id(str(finding["strategy_id"]))
        )
        loaded_findings.append(
            (
                int(finding["evaluate_at_ms"]),
                str(finding["scenario_id"]),
                str(finding["expected_classification"]),
                key,
            )
        )
    loaded_findings.sort()
    applier = JournalApplier(manager, engine)
    pending = loaded_findings
    out: list[tuple[str, str, str]] = []

    def read(item: tuple[int, str, str, str]) -> None:
        at, scenario_id, expected, key = item
        evaluation = engine.peek(key, at)
        out.append((scenario_id, expected, evaluation.classification.value))

    for entry in entries:
        while pending and entry.now_ms > pending[0][0]:
            read(pending.pop(0))
        applier.apply(entry)
    for item in pending:
        read(item)
    return out


if __name__ == "__main__":
    sys.exit(main())

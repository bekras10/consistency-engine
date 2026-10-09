"""Golden fixture I (ground-truth replay) through the real detection pipeline.

The recorded stream is applied entry by entry with ``JournalApplier`` (``BookManager`` ->
``DetectionEngine``, exactly what the live worker journals and replays). Each labelled strategy
is read with ``DetectionEngine.peek`` at its ``evaluate_at_ms``, just before the first message
received after that instant; ``peek`` never mutates state, so the assertions do not perturb the
pipeline. Duration is the engine's: the time the strategy has *continuously* passed every gate
except the duration gate, sampled on leg updates and at least every ``sweep_interval_ms``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from consistency_connectors.ingestion import BookManager
from consistency_core.models.relationship import Relationship
from consistency_core.pricing.evaluator import Evaluation
from consistency_core.pricing.portfolio import parse_strategy_id
from consistency_core.relationships.discovery import discover
from consistency_core.relationships.review import apply_reviews, load_reviews
from consistency_pipeline.driver import JournalApplier
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.journal import JournalEntry
from consistency_pipeline.lifecycle import DetectionEvent
from consistency_simulation.datasets import load
from consistency_simulation.families import SIM_EPOCH
from tests.golden.support import fee_calculator


@dataclass
class ScenarioResult:
    scenario_id: str
    expected_classification: str
    expected_reason_codes: list[str]
    evaluation: Evaluation
    observed_duration_ms: int | None
    samples: int


def _find(rels: list[Relationship], finding: dict[str, object]) -> Relationship:
    members = set(finding["members"])  # type: ignore[call-overload]
    hits = [
        r
        for r in rels
        if r.relationship_type.value == finding["relationship_type"] and set(r.members) == members
    ]
    assert len(hits) == 1, (finding["scenario_id"], [r.relationship_id for r in hits])
    return hits[0]


def replay_pipeline(
    dataset_dir: Path, reviews_path: Path
) -> tuple[list[ScenarioResult], list[DetectionEvent], DetectionEngine]:
    ds = load(dataset_dir)
    rels = discover(ds.catalog, as_of=SIM_EPOCH)
    rels, _ = apply_reviews(rels, load_reviews(reviews_path), ds.catalog, as_of=SIM_EPOCH)
    mgr = BookManager(ds.catalog.markets, source=f"replay:{dataset_dir.name}")
    engine = DetectionEngine(mgr, rels, fee_calculator(), session_id=f"golden-I:{dataset_dir.name}")
    applier = JournalApplier(mgr, engine)
    pending = sorted(
        (
            (
                int(f["evaluate_at_ms"]),
                i,
                f,
                engine.strategy_key_for(
                    _find(rels, f).relationship_id, parse_strategy_id(f["strategy_id"])
                ),
            )
            for i, f in enumerate(ds.metadata["expected_findings"])
        ),
        key=lambda p: (p[0], p[1]),
    )
    results: dict[int, ScenarioResult] = {}

    def read(at: int, i: int, f: dict[str, object], key: str) -> None:
        ev = engine.peek(key, at)
        results[i] = ScenarioResult(
            scenario_id=str(f["scenario_id"]),
            expected_classification=str(f["expected_classification"]),
            expected_reason_codes=list(f["expected_reason_codes"]),  # type: ignore[call-overload]
            evaluation=ev,
            observed_duration_ms=ev.certificate.timing.observed_duration_ms,
            samples=engine.stats.samples,
        )

    events: list[DetectionEvent] = []
    for ordinal, msg in enumerate(ds.messages):
        while pending and msg.received_ts_ms > pending[0][0]:
            read(*pending.pop(0))
        events += applier.apply(JournalEntry(ordinal=ordinal, message=msg))
    for p in pending:
        read(*p)
    return [results[i] for i in sorted(results)], events, engine


def replay_scenarios(dataset_dir: Path, reviews_path: Path) -> list[ScenarioResult]:
    return replay_pipeline(dataset_dir, reviews_path)[0]

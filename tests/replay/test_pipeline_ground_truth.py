"""The bundled ``inconsistent`` dataset through the real detection pipeline (Phase 7).

Stronger than golden I: exact reason codes, the durations the former test-only scan loop
measured (3976 / 3975 / 6975 / 425 ms), lifecycle expectations per scenario and no alert spam.
"""

from __future__ import annotations

from collections import Counter
from functools import cache

from consistency_core.models.detection import Classification
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.lifecycle import DetectionEvent, EventKind
from tests.conftest import FIXTURES
from tests.golden.replay_support import ScenarioResult, replay_pipeline

EXPECTED_DURATIONS = {
    "S1-overpriced-narrow": 3976,
    "S2-underpriced-basket": 3975,
    "S6-static-genuine": 6975,
    "S8-short-lived": 425,
}


@cache
def _run() -> tuple[list[ScenarioResult], list[DetectionEvent], DetectionEngine]:
    return replay_pipeline(
        FIXTURES / "datasets" / "inconsistent", FIXTURES / "relationships" / "manual-reviews.yaml"
    )


def test_scenarios_exact_classification_reasons_and_duration() -> None:
    results, _, _ = _run()
    assert len(results) == 8
    for r in results:
        assert r.evaluation.classification.value == r.expected_classification, r.scenario_id
        assert list(r.evaluation.reason_codes) == r.expected_reason_codes, r.scenario_id
        assert r.observed_duration_ms == EXPECTED_DURATIONS.get(r.scenario_id), r.scenario_id


def test_lifecycle_has_no_duplicates_or_spam() -> None:
    _, events, engine = _run()
    assert dict(Counter(e.kind.value for e in events)) == {
        "OPENED": 12,
        "UPDATED": 120,
        "EXPIRED": 7,
        "RESOLVED": 5,
    }
    previous: dict[str, DetectionEvent] = {}
    for event in events:
        if event.kind is EventKind.UPDATED:
            prior = previous[event.detection_id]
            metrics = event.record.metrics
            prior_metrics = prior.record.metrics
            quote = (
                metrics.theoretical_deviation,
                metrics.gross_edge,
                metrics.total_fees,
                metrics.net_edge,
                metrics.execution_adjusted_edge,
                metrics.reported_quantity,
                metrics.depth_supported_quantity,
                metrics.worst_case_payoff,
                event.record.classification,
                event.record.reason_codes,
            )
            prior_quote = (
                prior_metrics.theoretical_deviation,
                prior_metrics.gross_edge,
                prior_metrics.total_fees,
                prior_metrics.net_edge,
                prior_metrics.execution_adjusted_edge,
                prior_metrics.reported_quantity,
                prior_metrics.depth_supported_quantity,
                prior_metrics.worst_case_payoff,
                prior.record.classification,
                prior.record.reason_codes,
            )
            assert quote != prior_quote
        previous[event.detection_id] = event
    active: dict[str, str] = {}
    seqs: dict[str, int] = {}
    for e in events:
        key = f"{e.record.relationship_id}::{e.record.strategy_id}"
        assert e.seq == seqs.get(e.detection_id, 0) + 1  # contiguous per detection
        seqs[e.detection_id] = e.seq
        if e.kind is EventKind.OPENED:
            assert key not in active, "two active detections for one strategy"
            active[key] = e.detection_id
        else:
            assert active.get(key) == e.detection_id
            if not e.record.status.active:
                del active[key]
    assert active == {}
    assert engine.active_detections() == []


def test_unverified_scenario_never_opens_a_detection() -> None:
    results, events, engine = _run()
    s7 = next(r for r in results if r.scenario_id.startswith("S7"))
    rid = s7.evaluation.certificate.relationship.relationship_id
    assert not [e for e in events if e.record.relationship_id == rid]
    assert {s.classification for s in engine.states() if s.relationship_id == rid} == {
        Classification.INVALID_RELATIONSHIP
    }


def test_candidates_reached_fee_adjusted_peak() -> None:
    results, events, _ = _run()
    for r in results:
        if r.expected_classification != "FEE_ADJUSTED_CANDIDATE":
            continue
        rid = r.evaluation.certificate.relationship.relationship_id
        peaks = {e.record.peak_classification for e in events if e.record.relationship_id == rid}
        assert Classification.FEE_ADJUSTED_CANDIDATE in peaks, r.scenario_id

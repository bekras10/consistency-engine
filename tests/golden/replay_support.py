"""Test-only scan loop for golden fixture I (ground-truth replay).

This is deliberately minimal scaffolding standing in for the detection pipeline (a later
milestone): ingest the recorded stream through ``BookManager`` and evaluate each labelled
strategy inside its scenario window. Duration is measured on the local (received) clock as the
time the strategy has *continuously* passed every gate except the duration gate.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from consistency_connectors.ingestion import BookManager
from consistency_core.models import Market
from consistency_core.models.detection import Classification
from consistency_core.models.relationship import Relationship
from consistency_core.pricing.evaluator import Evaluation, Reason, evaluate
from consistency_core.pricing.portfolio import Portfolio, parse_strategy_id
from consistency_core.relationships.discovery import discover
from consistency_core.relationships.review import apply_reviews, load_reviews
from consistency_simulation.datasets import load
from consistency_simulation.families import SIM_EPOCH
from tests.golden.support import fee_calculator

SAMPLE_EVERY_MS = 100


@dataclass
class ScenarioResult:
    scenario_id: str
    expected_classification: str
    expected_reason_codes: list[str]
    evaluation: Evaluation
    observed_duration_ms: int | None
    samples: int


def _waiting_only_on_duration(ev: Evaluation) -> bool:
    return ev.classification is Classification.DEPTH_SUPPORTED and ev.reason_codes == (
        Reason.DURATION_UNKNOWN.value,
    )


def _find(rels: list[Relationship], finding: dict[str, object]) -> Relationship:
    members = set(finding["members"])  # type: ignore[call-overload]
    hits = [
        r
        for r in rels
        if r.relationship_type.value == finding["relationship_type"] and set(r.members) == members
    ]
    assert len(hits) == 1, (finding["scenario_id"], [r.relationship_id for r in hits])
    return hits[0]


class _Tracker:
    def __init__(
        self, finding: dict[str, object], rel: Relationship, pf: Portfolio, mgr: BookManager
    ) -> None:
        self.f = finding
        self.rel = rel
        self.pf = pf
        self.mgr = mgr
        self.legs = {leg.market_id for leg in pf.legs}
        self.start = int(finding["window_start_ms"])  # type: ignore[call-overload]
        self.at = int(finding["evaluate_at_ms"])  # type: ignore[call-overload]
        self.active_since: int | None = None
        self.last_sample: int | None = None
        self.samples = 0
        self.result: ScenarioResult | None = None

    def _eval(self, now_ms: int, duration: int | None) -> Evaluation:
        markets: dict[str, Market] = {}
        for mid in self.rel.members:
            m = self.mgr.market(mid)
            status = self.mgr.market_status(mid)
            markets[mid] = m if m.status is status else m.model_copy(update={"status": status})
        return evaluate(
            self.rel,
            self.pf,
            markets=markets,
            books={mid: self.mgr.book(mid) for mid in self.rel.members},
            now_ms=now_ms,
            fees=fee_calculator(),
            observed_duration_ms=duration,
        )

    def _sample(self, now_ms: int) -> None:
        self.samples += 1
        self.last_sample = now_ms
        if _waiting_only_on_duration(self._eval(now_ms, None)):
            if self.active_since is None:
                self.active_since = now_ms
        else:
            self.active_since = None

    def before(self, received_ts_ms: int) -> None:
        """Called before a message with this local timestamp is applied."""
        if self.result is None and received_ts_ms > self.at:
            self._finish()

    def after(self, received_ts_ms: int, touched: set[str]) -> None:
        """Re-evaluate when a leg changes, and at least every ``SAMPLE_EVERY_MS`` of local time
        otherwise: a frozen book emits no deltas but its condition still persists."""
        if self.result is not None or not self.start <= received_ts_ms <= self.at:
            return
        due = self.last_sample is None or received_ts_ms - self.last_sample >= SAMPLE_EVERY_MS
        if touched & self.legs or due:
            self._sample(received_ts_ms)

    def _finish(self) -> None:
        self._sample(self.at)
        duration = None if self.active_since is None else self.at - self.active_since
        ev = self._eval(self.at, duration)
        self.result = ScenarioResult(
            scenario_id=str(self.f["scenario_id"]),
            expected_classification=str(self.f["expected_classification"]),
            expected_reason_codes=list(self.f["expected_reason_codes"]),  # type: ignore[call-overload]
            evaluation=ev,
            observed_duration_ms=duration,
            samples=self.samples,
        )


def replay_scenarios(dataset_dir: Path, reviews_path: Path) -> list[ScenarioResult]:
    ds = load(dataset_dir)
    rels = discover(ds.catalog, as_of=SIM_EPOCH)
    rels, _ = apply_reviews(rels, load_reviews(reviews_path), ds.catalog, as_of=SIM_EPOCH)
    mgr = BookManager(ds.catalog.markets, source=f"replay:{dataset_dir.name}")
    trackers = [
        _Tracker(f, _find(rels, f), parse_strategy_id(f["strategy_id"]), mgr)
        for f in ds.metadata["expected_findings"]
    ]
    for msg in ds.messages:
        for t in trackers:
            t.before(msg.received_ts_ms)
        touched = {u.market_id for u in mgr.process(msg)}
        for t in trackers:
            t.after(msg.received_ts_ms, touched)
    for t in trackers:
        if t.result is None:
            t._finish()
    return [t.result for t in trackers if t.result is not None]

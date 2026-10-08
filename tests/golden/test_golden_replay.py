"""Golden fixture I: replaying the bundled ``inconsistent`` dataset is deterministic, and every
injected scenario receives the classification its ground-truth label states.

Expected labels (from the dataset metadata, written by the generator before any evaluation):
  S1 overpriced narrow threshold   -> FEE_ADJUSTED_CANDIDATE
  S2 underpriced complete basket   -> FEE_ADJUSTED_CANDIDATE
  S3 stale feed (5 s delay)        -> STALE_DATA [BOOK_TOO_OLD]
  S4 1c basket edge                -> THEORETICAL_ONLY [FEES_EXCEED_EDGE]
  S5 1-contract-deep basket        -> INSUFFICIENT_LIQUIDITY [EDGE_EXHAUSTED_BY_DEPTH]
  S6 frozen genuine inconsistency  -> FEE_ADJUSTED_CANDIDATE
  S7 ambiguous settlement rules    -> INVALID_RELATIONSHIP [RELATIONSHIP_NOT_VERIFIED]
  S8 ~600 ms inconsistency         -> DEPTH_SUPPORTED [DURATION_BELOW_MINIMUM]
"""

from __future__ import annotations

from functools import cache

import pytest

from tests.conftest import FIXTURES
from tests.golden.replay_support import ScenarioResult, replay_scenarios
from tests.golden.support import load

pytestmark = pytest.mark.golden


@cache
def _run() -> tuple[ScenarioResult, ...]:
    fx = load("I")
    return tuple(
        replay_scenarios(FIXTURES / "datasets" / fx["dataset"], FIXTURES.parent / fx["reviews"])
    )


def test_I_every_scenario_matches_ground_truth() -> None:
    results = _run()
    assert len(results) == 8
    mismatches = [
        (
            r.scenario_id,
            r.evaluation.classification.value,
            r.evaluation.reason_codes,
            r.observed_duration_ms,
        )
        for r in results
        if r.evaluation.classification.value != r.expected_classification
        or not set(r.expected_reason_codes) <= set(r.evaluation.reason_codes)
    ]
    assert mismatches == []


def test_I_replay_is_byte_identical() -> None:
    fx = load("I")
    first = _run()
    second = replay_scenarios(
        FIXTURES / "datasets" / fx["dataset"], FIXTURES.parent / fx["reviews"]
    )
    assert [r.evaluation.certificate_hash for r in first] == [
        r.evaluation.certificate_hash for r in second
    ]
    assert [r.evaluation.certificate_json() for r in first] == [
        r.evaluation.certificate_json() for r in second
    ]

"""Large-domain quantity search vs exhaustive search on small domains (hardening pass, P2).

* ``BREAKPOINT_APPROXIMATE`` (forced here by disabling bounded refinement) is always labelled
  approximate; its reported figures are an exact re-evaluation at its quantity; its objective
  never exceeds the exhaustive optimum; false negatives are counted and surfaced with
  ``QUANTITY_SEARCH_APPROXIMATE``.
* ``BOUNDED_EXACT`` (the default above ``exhaustive_search_limit``) must agree with exhaustive
  search exactly: classification, reason codes, reported quantity and every figure.
"""

from __future__ import annotations

import random
from collections.abc import Iterator

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from consistency_core.pricing import evaluator
from tests.property.breakpoint_support import (
    Case,
    Stats,
    _cfg,
    compare,
    random_case,
    sweep,
)
from tests.unit.test_evaluator import run_a

FALSE_NEGATIVES = [
    Case((("0.67", "123"), ("0.66", "120"), ("0.65", "4")), (("0.37", "98"),), "0.0070"),
    Case(
        (("0.74", "100"), ("0.70", "56"), ("0.69", "28"), ("0.66", "84")),
        (("0.30", "74"), ("0.29", "91"), ("0.26", "28")),
        "0.0095",
    ),
    Case((("0.75", "73"), ("0.74", "7")), (("0.29", "83"),), "0.0100"),
]
"""Found by the seeded sweep: exhaustive search finds an edge-qualifying quantity, the pure
breakpoint search does not (classification FEE_ADJUSTED_CANDIDATE vs DEPTH_SUPPORTED)."""


@pytest.fixture
def breakpoint_only() -> Iterator[None]:
    old = evaluator.BOUNDED_REFINEMENT_LIMIT
    evaluator.BOUNDED_REFINEMENT_LIMIT = 0
    yield
    evaluator.BOUNDED_REFINEMENT_LIMIT = old


def _bounded_matches_exhaustive(case: Case) -> None:
    ex = run_a(fx=case.fixture(), config=_cfg(case))
    bd = run_a(fx=case.fixture(), config=_cfg(case, exhaustive_search_limit=1))
    cap_ex, cap_bd = ex.certificate.capacity, bd.certificate.capacity
    if (
        cap_bd is not None
        and cap_ex is not None
        and cap_ex.method == "EXHAUSTIVE"
        and (cap_ex.domain_points or 0) > 1
    ):
        assert cap_bd.method in ("BOUNDED_EXACT", "NONE"), cap_bd.method
        if cap_bd.method == "BOUNDED_EXACT":
            assert cap_bd.optimal_quantity_is_exact is True
            assert cap_bd.points_evaluated <= cap_ex.points_evaluated
    assert bd.classification is ex.classification
    assert bd.reason_codes == ex.reason_codes
    assert bd.certificate.evaluation == ex.certificate.evaluation
    if cap_ex is not None and cap_bd is not None:
        for f in (
            "best_gross_quantity",
            "best_net_quantity",
            "best_execution_quantity",
            "max_profitable_quantity",
        ):
            assert getattr(cap_bd, f) == getattr(cap_ex, f), f


# ---------------------------------------------------------------- pure breakpoint search
@pytest.mark.usefixtures("breakpoint_only")
@settings(
    max_examples=60, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(seed=st.integers(0, 2**32 - 1))
def test_breakpoint_search_invariants(seed: int) -> None:
    compare(random_case(random.Random(seed)))


@pytest.mark.usefixtures("breakpoint_only")
@pytest.mark.parametrize("case", FALSE_NEGATIVES)
def test_breakpoint_false_negatives_are_surfaced(case: Case) -> None:
    bp = run_a(fx=case.fixture(), config=_cfg(case, exhaustive_search_limit=1))
    ex = run_a(fx=case.fixture(), config=_cfg(case))
    assert ex.classification.value == "FEE_ADJUSTED_CANDIDATE"
    cap = bp.certificate.capacity
    assert cap is not None
    assert cap.method == "BREAKPOINT_APPROXIMATE"
    assert cap.optimal_quantity_is_exact is False
    assert bp.classification.value == "DEPTH_SUPPORTED"
    assert "QUANTITY_SEARCH_APPROXIMATE" in bp.reason_codes
    stats = Stats()
    compare(case, stats)
    assert stats.false_negatives_edge == 1


@pytest.mark.usefixtures("breakpoint_only")
def test_breakpoint_sweep_statistics() -> None:
    """Deterministic sweep; the measured numbers are reported in PROGRESS.md."""
    s = sweep(150, seed=20261008)
    print("breakpoint-vs-exhaustive", s.as_dict())
    assert s.cases == 150
    assert s.breakpoint_used + s.bounded_exact >= 140
    assert s.false_negatives_net == 0


# ---------------------------------------------------------------- bounded exact search
@pytest.mark.parametrize("case", FALSE_NEGATIVES)
def test_bounded_search_fixes_breakpoint_false_negatives(case: Case) -> None:
    _bounded_matches_exhaustive(case)


@settings(max_examples=80, deadline=None)
@given(seed=st.integers(0, 2**32 - 1))
def test_bounded_search_equals_exhaustive(seed: int) -> None:
    _bounded_matches_exhaustive(random_case(random.Random(seed)))


def test_bounded_search_sweep_has_no_discrepancy() -> None:
    rng = random.Random(7)
    for _ in range(120):
        _bounded_matches_exhaustive(random_case(rng))

"""Breakpoint vs exhaustive quantity search on the same books (hardening pass, P2)."""

from __future__ import annotations

import copy
import random
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from consistency_core.pricing.evaluator import Evaluation
from tests.golden.support import load
from tests.unit.test_evaluator import run_a

D = Decimal


@dataclass(frozen=True)
class Case:
    ge3_yes_bids: tuple[tuple[str, str], ...]
    ge2_no_bids: tuple[tuple[str, str], ...]
    minimum_net_edge: str

    def fixture(self) -> dict[str, Any]:
        fx = copy.deepcopy(load("A"))
        fx["books"] = {
            "GOLD-A-GE3": {"yes_bids": [list(x) for x in self.ge3_yes_bids]},
            "GOLD-A-GE2": {"no_bids": [list(x) for x in self.ge2_no_bids]},
        }
        return fx


def random_case(rng: random.Random) -> Case:
    """Marginal books: the top-of-book pre-fee edge is 2-6 cents, about what fees (~2.2c),
    slippage (1c) and the minimum edge consume, so cent rounding decides qualification."""
    top_yes = rng.randint(50, 75)
    top_no = 100 + rng.randint(2, 6) - top_yes

    def side(top: int) -> tuple[tuple[str, str], ...]:
        prices = [top, *sorted(rng.sample(range(top - 8, top), rng.randint(0, 3)), reverse=True)]
        return tuple((f"0.{p:02d}", str(rng.randint(1, 150))) for p in prices)

    return Case(
        ge3_yes_bids=side(top_yes),
        ge2_no_bids=side(top_no),
        minimum_net_edge=str(D(rng.randint(0, 30)) * D("0.0005")),
    )


def _cfg(case: Case, **extra: Any) -> dict[str, Any]:
    return {"minimum_available_quantity": "1", "minimum_net_edge": case.minimum_net_edge, **extra}


def evaluate_both(case: Case) -> tuple[Evaluation, Evaluation]:
    fx = case.fixture()
    exhaustive = run_a(fx=fx, config=_cfg(case))
    breakpoint_ = run_a(fx=copy.deepcopy(fx), config=_cfg(case, exhaustive_search_limit=1))
    return exhaustive, breakpoint_


def reevaluate(case: Case, quantity: Decimal) -> Evaluation:
    return run_a(fx=case.fixture(), config=_cfg(case, target_quantity=str(quantity)))


def reached_gates(ev: Evaluation) -> bool:
    return ev.certificate.trace[-1].outcome != "not_reached"


@dataclass
class Stats:
    cases: int = 0
    breakpoint_used: int = 0
    bounded_exact: int = 0
    """Large-domain searches the bounds proved exact without evaluating any extra point."""
    reached_gates_both: int = 0
    same_classification: int = 0
    same_reported_quantity: int = 0
    objective_gap_cases: int = 0
    max_objective_gap: Decimal = Decimal(0)
    net_gap_cases: int = 0
    false_negatives_edge: int = 0
    """Exhaustive finds an edge-qualifying quantity, breakpoint search finds none."""
    false_negatives_net: int = 0
    """Exhaustive finds positive after-fee profit, breakpoint search finds none."""
    examples: list[Case] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            k: (str(v) if isinstance(v, Decimal) else v)
            for k, v in self.__dict__.items()
            if k != "examples"
        }


def compare(case: Case, stats: Stats | None = None) -> None:
    """Assert the cross-check invariants; record gaps and false negatives in ``stats``."""
    ex, bp = evaluate_both(case)
    s = stats if stats is not None else Stats()
    s.cases += 1
    cap_ex, cap_bp = ex.certificate.capacity, bp.certificate.capacity
    if cap_bp is not None and cap_bp.method == "BREAKPOINT_APPROXIMATE":
        s.breakpoint_used += 1
        assert cap_bp.optimal_quantity_is_exact is False
    if cap_bp is not None and cap_bp.method == "BOUNDED_EXACT":
        s.bounded_exact += 1
        assert cap_bp.optimal_quantity_is_exact is True
    if cap_ex is not None and cap_ex.method == "EXHAUSTIVE":
        assert cap_ex.optimal_quantity_is_exact is True
    s.same_classification += ex.classification is bp.classification
    e_bp, e_ex = bp.certificate.evaluation, ex.certificate.evaluation
    if e_bp is not None and e_bp.net_profit is not None:
        # the edge reported at the breakpoint quantity is exact
        again = reevaluate(case, e_bp.quantity).certificate.evaluation
        assert again is not None
        assert again.model_dump() == e_bp.model_dump()
    if e_ex is not None and e_bp is not None and e_ex.net_profit is not None:
        assert e_bp.net_profit is not None
        if not reached_gates(ex) or not reached_gates(bp):
            # stopped at step 9: compare the best after-fee profit
            if e_bp.net_profit < e_ex.net_profit:
                s.net_gap_cases += 1
            assert e_bp.net_profit <= e_ex.net_profit
            if e_ex.net_profit > 0 >= e_bp.net_profit:
                s.false_negatives_net += 1
                s.examples.append(case)
    if not (reached_gates(ex) and reached_gates(bp)):
        return
    s.reached_gates_both += 1
    assert e_ex is not None and e_bp is not None and cap_ex is not None and cap_bp is not None
    assert e_ex.execution_adjusted_profit is not None
    assert e_bp.execution_adjusted_profit is not None
    s.same_reported_quantity += e_ex.quantity == e_bp.quantity
    q_ex = bool(cap_ex.edge_qualifying_points)
    q_bp = bool(cap_bp.edge_qualifying_points)
    assert not (q_bp and not q_ex), "breakpoint points are a subset of the exhaustive domain"
    if q_ex and not q_bp:
        s.false_negatives_edge += 1
        s.examples.append(case)
        return
    # same qualification status: the breakpoint objective never beats the exhaustive optimum
    assert e_bp.execution_adjusted_profit <= e_ex.execution_adjusted_profit
    gap = e_ex.execution_adjusted_profit - e_bp.execution_adjusted_profit
    if gap > 0:
        s.objective_gap_cases += 1
        s.max_objective_gap = max(s.max_objective_gap, gap)


def sweep(n: int, seed: int) -> Stats:
    rng = random.Random(seed)
    stats = Stats()
    for _ in range(n):
        compare(random_case(rng), stats)
    return stats

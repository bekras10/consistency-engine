"""Property tests: constraint-based scenario minimisation equals brute force over {0,1}^n."""

from __future__ import annotations

import itertools
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from consistency_core.models.relationship import ScenarioKind, ScenarioSpec
from consistency_core.relationships.scenarios import ScenarioSpace


@st.composite
def spaces(draw: st.DrawFn) -> ScenarioSpace:
    n = draw(st.integers(1, 7))
    kind = draw(st.sampled_from(list(ScenarioKind)))
    if kind is ScenarioKind.CARDINALITY:
        lo = draw(st.integers(0, n))
        hi = draw(st.integers(lo, n + 2))
        return ScenarioSpace(ScenarioSpec(kind=kind, min_yes=lo, max_yes=hi), n)
    if kind is ScenarioKind.CHAIN:
        return ScenarioSpace(ScenarioSpec(kind=kind), n)
    all_states = list(itertools.product((0, 1), repeat=n))
    picked = draw(st.lists(st.sampled_from(all_states), min_size=1, max_size=12, unique=True))
    return ScenarioSpace(ScenarioSpec(kind=kind, explicit_states=tuple(picked)), n)


weights_st = st.integers(-500, 500).map(lambda i: Decimal(i) / 100)


@given(spaces(), st.data())
def test_min_linear_equals_brute_force(sp: ScenarioSpace, data: st.DataObject) -> None:
    w = data.draw(st.lists(weights_st, min_size=sp.n, max_size=sp.n))
    c = data.draw(weights_st)
    admissible = [s for s in itertools.product((0, 1), repeat=sp.n) if sp.contains(s)]
    assert set(admissible) == set(sp.iter_states())
    assert len(admissible) == sp.count()
    brute = min(
        c + sum((wi for wi, b in zip(w, s, strict=True) if b), Decimal(0)) for s in admissible
    )
    v, arg = sp.min_linear(c, w)
    assert v == brute
    assert sp.contains(arg)
    assert c + sum((wi for wi, b in zip(w, arg, strict=True) if b), Decimal(0)) == v
    vmax, _ = sp.max_linear(c, w)
    assert vmax == max(
        c + sum((wi for wi, b in zip(w, s, strict=True) if b), Decimal(0)) for s in admissible
    )

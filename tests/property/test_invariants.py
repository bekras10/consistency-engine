"""Property tests for the spec invariants 1-10."""

from __future__ import annotations

import copy
import itertools
import math
from decimal import Decimal
from fractions import Fraction
from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from consistency_connectors.ingestion import BookManager
from consistency_core.events import OrderBookDeltaEvent, OrderBookSnapshotEvent, StreamMessage
from consistency_core.fees import (
    FeeCalculator,
    FeeScheduleRegistry,
    MemberClass,
    buy_order_fees,
    load_schedule,
)
from consistency_core.models import Side
from consistency_core.models.detection import Classification
from consistency_core.models.market import Catalog, Event
from consistency_core.models.orderbook import AskLevel, ask_curve, build_side
from consistency_core.models.relationship import (
    RelationshipType,
    ScenarioKind,
    ScenarioSpec,
    VerificationStatus,
)
from consistency_core.models.settlement import (
    Comparator,
    IntervalTerms,
    OutcomeTerms,
    RoundingConvention,
    RoundingMode,
    SettlementSpec,
    ThresholdTerms,
)
from consistency_core.pricing.certificate import EvaluationConfig
from consistency_core.pricing.depth import walk_asks
from consistency_core.pricing.evaluator import evaluate
from consistency_core.relationships.discovery import discover
from consistency_core.relationships.scenarios import ScenarioSpace
from tests.conftest import FIXTURES
from tests.factories import PROV, T0, market
from tests.golden.support import NOW_MS, books, catalog, find_relationship, load, portfolio
from tests.unit.test_evaluator import A_PF, A_REL, run_a

D = Decimal
SETTINGS = settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])

# ---------------------------------------------------------------- strategies
prices = st.integers(1, 99).map(lambda c: D(c) / 100)
qtys = st.integers(1, 500).map(D)


@st.composite
def ask_curves(draw: st.DrawFn, side: Side = Side.YES) -> list[AskLevel]:
    ps = sorted(draw(st.sets(st.integers(1, 99), min_size=1, max_size=6)))
    bids = build_side(side.opposite, [(D(100 - p) / 100, draw(qtys)) for p in ps])
    return list(ask_curve(bids, side))


# ---------------------------------------------------------------- 1. non-negative quantities
def _msg(pos: int, event: OrderBookSnapshotEvent | OrderBookDeltaEvent) -> StreamMessage:
    t = 1_784_124_000_000 + pos
    return StreamMessage(position=pos, emitted_ts_ms=t, received_ts_ms=t, event=event)


@st.composite
def message_scripts(draw: st.DrawFn) -> list[StreamMessage]:
    yes = draw(st.dictionaries(st.integers(1, 40), st.integers(1, 50), max_size=5))
    no = draw(st.dictionaries(st.integers(1, 40), st.integers(1, 50), max_size=5))
    base = {"market_id": "SYN-P", "sid": "1", "connection_id": "c", "exchange_ts_ms": 0}
    msgs = [
        _msg(
            0,
            OrderBookSnapshotEvent(
                **base,  # type: ignore[arg-type]
                seq=1,
                yes_bids=tuple((D(p) / 100, D(q)) for p, q in sorted(yes.items())),
                no_bids=tuple((D(p) / 100, D(q)) for p, q in sorted(no.items())),
            ),
        )
    ]
    n = draw(st.integers(0, 25))
    for i in range(n):
        side = draw(st.sampled_from([Side.YES, Side.NO]))
        price = D(draw(st.integers(1, 40))) / 100
        delta = D(draw(st.integers(-60, 60)))
        seq = i + 2 if not draw(st.booleans()) or i == 0 else i + 2 + draw(st.integers(0, 1))
        msgs.append(
            _msg(
                i + 1,
                OrderBookDeltaEvent(
                    **base,  # type: ignore[arg-type]
                    seq=seq,
                    side=side,
                    price=price,
                    delta=delta if delta != 0 else D(1),
                ),
            )
        )
    return msgs


def _manager() -> BookManager:
    return BookManager([market("SYN-P")], source="prop", clock_ns=lambda: 0)


@SETTINGS
@given(message_scripts())
def test_inv1_book_quantities_never_negative(msgs: list[StreamMessage]) -> None:
    mgr = _manager()
    for m in msgs:
        mgr.process(m)
        book = mgr.book("SYN-P")
        for lv in (*book.yes_bids, *book.no_bids):
            assert lv.quantity > 0
        for side in Side:
            assert all(a.quantity > 0 for a in book.asks(side))


# ---------------------------------------------------------------- 8. replay determinism
@SETTINGS
@given(message_scripts())
def test_inv8_replay_same_messages_same_state(msgs: list[StreamMessage]) -> None:
    a, b = _manager(), _manager()
    for m in msgs:
        a.process(m)
    for m in msgs:
        b.process(m)
    assert a.state_digest() == b.state_digest()
    assert a.state_view() == b.state_view()


# ---------------------------------------------------------------- 2. buying more costs more
@SETTINGS
@given(ask_curves(), st.integers(0, 3000), st.integers(0, 3000))
def test_inv2_more_quantity_never_cheaper(asks: list[AskLevel], q1: int, q2: int) -> None:
    lo, hi = sorted((D(q1), D(q2)))
    a = walk_asks("M", Side.YES, asks, lo)
    b = walk_asks("M", Side.YES, asks, hi)
    assert a.total_premium <= b.total_premium
    assert a.filled_quantity <= b.filled_quantity


# ---------------------------------------------------------------- 3. higher fees, worse edge
@SETTINGS
@given(
    st.lists(st.tuples(prices, st.integers(1, 300).map(D)), min_size=1, max_size=5),
    st.integers(0, 300),
    st.integers(0, 300),
    st.sampled_from(list(MemberClass)),
)
def test_inv3_higher_fees_never_improve_edge(
    fills: list[tuple[Decimal, Decimal]], m1: int, m2: int, mc: MemberClass
) -> None:
    """A larger fee multiplier never lowers an order's total net fee, for one fill or for several
    fills sharing the rebate accumulator."""
    lo, hi = sorted((D(m1) / 100, D(m2) / 100))
    for fl in ([fills[0]], fills):
        a = buy_order_fees(fl, coefficient=D("0.07"), multiplier=lo, member_class=mc)
        b = buy_order_fees(fl, coefficient=D("0.07"), multiplier=hi, member_class=mc)
        assert a.total_trade_fee <= b.total_trade_fee
        assert a.total_net_fee <= b.total_net_fee


@SETTINGS
@given(st.integers(0, 300), st.integers(0, 300), st.integers(10, 100))
def test_inv3_evaluator_net_profit_monotone_in_fees(m1: int, m2: int, q: int) -> None:
    """End to end on fixture A at a fixed quantity: a larger fee multiplier never raises net
    profit (each leg is a single fill), and never turns a non-candidate into a candidate."""
    base = load_schedule(FIXTURES / "fees" / "synthetic-fictional-v1.yaml")
    fx = load("A")
    cat = catalog(fx)
    rel = find_relationship(discover(cat, as_of=T0), A_REL)
    evs = []
    for m in sorted((D(m1) / 100, D(m2) / 100)):
        sched = base.model_copy(update={"default_taker_multiplier": m})
        evs.append(
            evaluate(
                rel,
                portfolio(A_PF),
                markets=cat.markets_by_id(),
                books=books(fx, NOW_MS),
                now_ms=NOW_MS,
                fees=FeeCalculator(FeeScheduleRegistry([sched])),
                config=EvaluationConfig(target_quantity=D(q)),
                observed_duration_ms=5000,
            )
        )
    lo, hi = (e.certificate.evaluation for e in evs)
    assert lo is not None and hi is not None
    assert lo.net_profit is not None and hi.net_profit is not None
    assert hi.net_profit <= lo.net_profit
    if evs[1].classification is Classification.FEE_ADJUSTED_CANDIDATE:
        assert evs[0].classification is Classification.FEE_ADJUSTED_CANDIDATE


# ---------------------------------------------------------------- 4. more scenarios, lower min
@SETTINGS
@given(st.integers(1, 6), st.data())
def test_inv4_adding_scenarios_never_raises_min_payoff(n: int, data: st.DataObject) -> None:
    states = list(itertools.product((0, 1), repeat=n))
    small = data.draw(st.lists(st.sampled_from(states), min_size=1, max_size=8, unique=True))
    extra = data.draw(st.lists(st.sampled_from(states), max_size=8, unique=True))
    big = list(dict.fromkeys([*small, *extra]))
    w = [D(data.draw(st.integers(-300, 300))) / 100 for _ in range(n)]
    c = D(data.draw(st.integers(-300, 300))) / 100
    s1 = ScenarioSpace(ScenarioSpec(kind=ScenarioKind.EXPLICIT, explicit_states=tuple(small)), n)
    s2 = ScenarioSpace(ScenarioSpec(kind=ScenarioKind.EXPLICIT, explicit_states=tuple(big)), n)
    assert s2.min_linear(c, w)[0] <= s1.min_linear(c, w)[0]


# ---------------------------------------------------------------- 5-7. verified relationships
def _round(x: Fraction, rc: RoundingConvention | None) -> Fraction:
    """Independent settlement oracle (does not use the engine's preimage code)."""
    if rc is None or rc.mode is RoundingMode.NONE:
        return x
    assert rc.increment is not None
    r = Fraction(rc.increment)
    k = x / r
    match rc.mode:
        case RoundingMode.HALF_UP:
            n = math.floor(k + Fraction(1, 2))
        case RoundingMode.HALF_DOWN:
            n = math.ceil(k - Fraction(1, 2))
        case RoundingMode.FLOOR:
            n = math.floor(k)
        case _:
            n = math.ceil(k)
    return n * r


def _settles_yes(spec: SettlementSpec, x: Fraction) -> bool:
    y = _round(x, spec.rounding)
    t = spec.terms
    if isinstance(t, ThresholdTerms):
        v = Fraction(t.value)
        return {
            Comparator.GE: y >= v,
            Comparator.GT: y > v,
            Comparator.LE: y <= v,
            Comparator.LT: y < v,
        }[t.comparator]
    assert isinstance(t, IntervalTerms)
    if t.lower is not None and (y < t.lower or (y == t.lower and not t.lower_inclusive)):
        return False
    return not (t.upper is not None and (y > t.upper or (y == t.upper and not t.upper_inclusive)))


COMMON: dict[str, Any] = {
    "underlying_id": "prop-index",
    "measurement": "level",
    "location": "synthetic",
    "observation_window": "2026-08",
    "settlement_source": "Synthetic Bureau",
    "methodology": "published",
    "unit": "points",
    "early_close_policy": "none",
    "exceptional_resolution": [],
}
roundings = st.sampled_from(
    [
        None,
        RoundingConvention(mode=RoundingMode.HALF_UP, increment=D("0.1")),
        RoundingConvention(mode=RoundingMode.FLOOR, increment=D("0.05")),
        RoundingConvention(mode=RoundingMode.CEIL, increment=D("0.25")),
    ]
)
raw_points = st.lists(
    st.integers(-200, 1200).map(lambda i: Fraction(i, 1000)), min_size=20, max_size=60
)


@st.composite
def threshold_catalogs(draw: st.DrawFn) -> Catalog:
    rc = draw(roundings)
    n = draw(st.integers(2, 5))
    vals = draw(st.lists(st.integers(1, 19), min_size=n, max_size=n, unique=True))
    ms = []
    for i, v in enumerate(vals):
        comp = draw(st.sampled_from([Comparator.GE, Comparator.GT]))
        own_rc = rc if draw(st.integers(0, 4)) else draw(roundings)
        spec = SettlementSpec.model_validate(
            {
                **COMMON,
                "rounding": own_rc.model_dump(mode="json") if own_rc else None,
                "terms": {"kind": "threshold", "comparator": comp.value, "value": str(D(v) / 20)},
            }
        )
        ms.append(market(f"SYN-T{i}", settlement=spec))
    return Catalog(markets=tuple(ms))


def _check_semantics(cat: Catalog, xs: list[Fraction]) -> None:
    by_id = cat.markets_by_id()
    for rel in discover(cat, as_of=T0):
        if rel.verification_status is not VerificationStatus.VERIFIED:
            continue
        space = ScenarioSpace(rel.scenario_spec, len(rel.members))
        for x in xs:
            state = tuple(int(_settles_yes(by_id[m].settlement, x)) for m in rel.members)
            assert space.contains(state), (rel.relationship_type, rel.members, x, state)
        all_states = list(space.iter_states())
        match rel.relationship_type:
            case RelationshipType.IMPLICATION:
                assert (1, 0) not in all_states
            case RelationshipType.NESTED_THRESHOLDS:
                for s in all_states:
                    assert list(s) == sorted(s)  # narrow YES => every broader YES
            case RelationshipType.MUTUALLY_EXCLUSIVE | RelationshipType.DISJOINT_INTERVALS:
                assert all(sum(s) <= 1 for s in all_states)
                if rel.exhaustive:
                    assert all(sum(s) == 1 for s in all_states)
            case RelationshipType.EXHAUSTIVE_PARTITION:
                assert all(sum(s) == 1 for s in all_states)
            case _:
                pass


@SETTINGS
@given(threshold_catalogs(), raw_points)
def test_inv5_verified_implication_never_a1_b0(cat: Catalog, xs: list[Fraction]) -> None:
    _check_semantics(cat, xs)


@st.composite
def interval_catalogs(draw: st.DrawFn) -> Catalog:
    cuts = sorted(draw(st.sets(st.integers(1, 19), min_size=1, max_size=4)))
    bounds: list[tuple[Decimal | None, Decimal | None]] = []
    edges: list[Decimal | None] = [None, *(D(c) / 20 for c in cuts), None]
    for lo, hi in itertools.pairwise(edges):
        bounds.append((lo, hi))
    if draw(st.booleans()) and len(bounds) > 2:
        bounds = bounds[1:]  # drop a tail -> not exhaustive
    rc = draw(roundings)
    ms = []
    for i, (lo, hi) in enumerate(bounds):
        terms: dict[str, Any] = {"kind": "interval"}
        if lo is not None:
            terms["lower"] = str(lo)
        if hi is not None:
            terms["upper"] = str(hi)
        spec = SettlementSpec.model_validate(
            {**COMMON, "rounding": rc.model_dump(mode="json") if rc else None, "terms": terms}
        )
        ms.append(market(f"SYN-I{i}", event_id="SYN-IV", settlement=spec))
    return Catalog(markets=tuple(ms))


@SETTINGS
@given(interval_catalogs(), raw_points)
def test_inv6_7_verified_interval_groups(cat: Catalog, xs: list[Fraction]) -> None:
    _check_semantics(cat, xs)


@SETTINGS
@given(st.integers(2, 6), st.booleans(), st.data())
def test_inv6_7_categorical_groups(n: int, complete: bool, data: st.DataObject) -> None:
    universe = [f"O{i}" for i in range(n + (0 if complete else 1))]
    ev = Event(
        event_id="SYN-CAT",
        series_id="SYN",
        title="cat",
        mutually_exclusive=True,
        outcome_set_complete=complete,
        outcome_universe=tuple(universe),
        provenance=PROV,
    )
    common = {k: v for k, v in COMMON.items() if k != "unit"}
    ms = tuple(
        market(
            f"SYN-C{i}",
            event_id="SYN-CAT",
            settlement=SettlementSpec.model_validate(
                {**common, "terms": OutcomeTerms(outcome_id=universe[i]).model_dump()}
            ),
        )
        for i in range(n)
    )
    rels = [
        r
        for r in discover(Catalog(markets=ms, events=(ev,)), as_of=T0)
        if r.verification_status is VerificationStatus.VERIFIED
    ]
    assert rels
    winner = data.draw(st.sampled_from(universe))
    for rel in rels:
        space = ScenarioSpace(rel.scenario_spec, len(rel.members))
        realized = tuple(
            int(by.settlement.terms.outcome_id == winner)  # type: ignore[union-attr]
            for by in (next(m for m in ms if m.market_id == mid) for mid in rel.members)
        )
        assert space.contains(realized)
        states = list(space.iter_states())
        assert all(sum(s) <= 1 for s in states)
        assert rel.exhaustive is complete
        if complete:
            assert all(sum(s) == 1 for s in states)


# ---------------------------------------------------------------- 9. invalidation
@SETTINGS
@given(
    st.sampled_from(
        [
            {"verification_status": VerificationStatus.CANDIDATE_REVIEW},
            {"verification_status": VerificationStatus.REJECTED},
            {"rules_hashes": {}},
        ]
    ),
    st.integers(1, 99),
    st.integers(1, 99),
)
def test_inv9_invalidation_blocks_verified_classes(
    update: dict[str, Any], p_no: int, p_yes: int
) -> None:
    fx = copy.deepcopy(load("A"))
    fx["books"] = {
        "GOLD-A-GE3": {"yes_bids": [[str(D(100 - p_no) / 100), "100"]]},
        "GOLD-A-GE2": {"no_bids": [[str(D(100 - p_yes) / 100), "100"]]},
    }
    ev = run_a(fx=fx, rel_update=update)
    assert ev.classification is Classification.INVALID_RELATIONSHIP


# ---------------------------------------------------------------- 10. slippage
@SETTINGS
@given(
    st.integers(0, 50),
    st.integers(0, 50),
    st.integers(10, 100),
    st.integers(20, 45),
    st.integers(20, 45),
)
def test_inv10_more_slippage_never_increases_conservative_edge(
    s1: int, s2: int, q: int, p_no: int, p_yes: int
) -> None:
    fx = copy.deepcopy(load("A"))
    fx["books"] = {
        "GOLD-A-GE3": {"yes_bids": [[str(D(100 - p_no) / 100), "100"]]},
        "GOLD-A-GE2": {"no_bids": [[str(D(100 - p_yes) / 100), "100"]]},
    }
    evs = [
        run_a(
            fx=fx,
            config={
                "assumed_extra_slippage_per_leg": str(D(s) / 1000),
                "target_quantity": str(q),
            },
        )
        for s in sorted((s1, s2))
    ]
    lo, hi = (e.certificate.evaluation for e in evs)
    assert lo is not None and hi is not None
    if lo.execution_adjusted_profit is not None and hi.execution_adjusted_profit is not None:
        assert hi.execution_adjusted_profit <= lo.execution_adjusted_profit
    candidate = Classification.FEE_ADJUSTED_CANDIDATE
    if evs[1].classification is candidate:
        assert evs[0].classification is candidate

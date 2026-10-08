"""Controlled inconsistency injections (spec 3.3), each with a known expected classification.

An injection overrides how specific markets are quoted during a window. The fair value used for
quoting is replaced by a function of the family's *current* latent state, so injected
mispricings move coherently with the rest of the market. Expected classifications assume the
default ``EvaluationConfig`` and the fictional synthetic fee schedule.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from fractions import Fraction

from consistency_core.models.common import Side
from consistency_core.models.relationship import RelationshipType
from consistency_core.pricing.portfolio import Leg, Template, strategy_id
from consistency_simulation.families import Family


class ScenarioKind(StrEnum):
    OVERPRICED_NARROW_THRESHOLD = "overpriced_narrow_threshold"
    UNDERPRICED_COMPLETE_BASKET = "underpriced_complete_basket"
    STALE_DATA_DISCREPANCY = "stale_data_discrepancy"
    ELIMINATED_BY_FEES = "eliminated_by_fees"
    ELIMINATED_BY_DEPTH = "eliminated_by_depth"
    STATIC_GENUINE_INCONSISTENCY = "static_genuine_inconsistency"
    AMBIGUOUS_SETTLEMENT = "ambiguous_settlement"
    SHORT_LIVED_INCONSISTENCY = "short_lived_inconsistency"


@dataclass(frozen=True)
class Override:
    market_id: str
    fair: Callable[[], Fraction]
    """Fair YES probability used for quoting while the injection is active."""
    thin_side: Side | None = None
    """If set, only the top level of this *bid* side uses ``fair`` (with ``top_qty``); deeper
    levels are quoted from ``depth_fair``."""
    top_qty: Decimal | None = None
    depth_fair: Callable[[], Fraction] | None = None


@dataclass(frozen=True)
class ExpectedFinding:
    scenario_id: str
    kind: ScenarioKind
    description: str
    relationship_type: RelationshipType
    members: tuple[str, ...]
    strategy_id: str
    expected_classification: str
    expected_reason_codes: tuple[str, ...]
    window_start_ms: int
    window_end_ms: int
    evaluate_at_ms: int

    def to_json(self) -> dict[str, object]:
        return {
            "scenario_id": self.scenario_id,
            "kind": self.kind.value,
            "description": self.description,
            "relationship_type": self.relationship_type.value,
            "members": list(self.members),
            "strategy_id": self.strategy_id,
            "expected_classification": self.expected_classification,
            "expected_reason_codes": list(self.expected_reason_codes),
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "evaluate_at_ms": self.evaluate_at_ms,
        }


@dataclass(frozen=True)
class Injection:
    finding: ExpectedFinding
    family_id: str
    start_ms: int
    end_ms: int
    overrides: tuple[Override, ...]
    lag_extra_ms: int = 0
    freeze_family: bool = False
    extra: dict[str, str] = field(default_factory=dict)


def _clip(p: Fraction) -> Fraction:
    return min(Fraction(97, 100), max(Fraction(3, 100), p))


FairFn = Callable[[], Fraction]


def scaled(f: FairFn, k: Fraction) -> FairFn:
    return lambda: _clip(f() * k)


def shifted(f: FairFn, d: Fraction) -> FairFn:
    return lambda: _clip(f() + d)


def offset_ask(asks: Callable[[], dict[str, Fraction]], market_id: str, hs: Fraction) -> FairFn:
    return lambda: asks()[market_id] - hs


def _yes(m: str) -> Leg:
    return Leg(market_id=m, side=Side.YES)


def _no(m: str) -> Leg:
    return Leg(market_id=m, side=Side.NO)


def standard_injections(
    families: dict[str, Family], *, t0_ms: int, epoch_ms: int, spacing_ms: int = 14_000
) -> list[Injection]:
    """The eight spec-3.3 scenarios, laid out sequentially from ``t0_ms`` (relative)."""
    econ, elec, impl = families["econ"], families["election"], families["implication"]
    weather, sports = families["weather"], families["sports"]
    out: list[Injection] = []
    t = t0_ms

    def add(
        sid: str,
        kind: ScenarioKind,
        desc: str,
        fam: Family,
        rtype: RelationshipType,
        members: tuple[str, ...],
        template: Template,
        legs: tuple[Leg, ...],
        expected: str,
        reasons: tuple[str, ...],
        overrides: tuple[Override, ...],
        *,
        window: int,
        eval_after: int,
        lag: int = 0,
        freeze: bool = False,
    ) -> None:
        nonlocal t
        finding = ExpectedFinding(
            scenario_id=sid,
            kind=kind,
            description=desc,
            relationship_type=rtype,
            members=members,
            strategy_id=strategy_id(template, legs),
            expected_classification=expected,
            expected_reason_codes=reasons,
            window_start_ms=epoch_ms + t,
            window_end_ms=epoch_ms + t + window,
            evaluate_at_ms=epoch_ms + t + eval_after,
        )
        out.append(
            Injection(
                finding=finding,
                family_id=fam.family_id,
                start_ms=t,
                end_ms=t + window,
                overrides=overrides,
                lag_extra_ms=lag,
                freeze_family=freeze,
            )
        )
        t += spacing_ms

    # 1. overpriced narrow threshold: YES(>=0.3) quoted 9 points above YES(>=0.2)
    chain = tuple(m.market.market_id for m in econ.markets[:4])  # GE0.4, GE0.3, GE0.2, GE0.1
    ge04, ge03, ge02 = chain[0], chain[1], chain[2]
    f02 = econ.by_id(ge02).fair
    f03 = econ.by_id(ge03).fair
    add(
        "S1-overpriced-narrow",
        ScenarioKind.OVERPRICED_NARROW_THRESHOLD,
        "YES(CPI>=0.3) quoted ~9c above YES(CPI>=0.2); NO(>=0.3)+YES(>=0.2) costs < $1",
        econ,
        RelationshipType.NESTED_THRESHOLDS,
        chain,
        Template.IMPLICATION,
        (_no(ge03), _yes(ge02)),
        "FEE_ADJUSTED_CANDIDATE",
        (),
        (Override(ge03, shifted(f02, Fraction(9, 100))),),
        window=6000,
        eval_after=4000,
    )
    # 2. underpriced complete basket: every election outcome quoted at 80% of fair
    pres = tuple(m.market.market_id for m in elec.markets[:5])
    add(
        "S2-underpriced-basket",
        ScenarioKind.UNDERPRICED_COMPLETE_BASKET,
        "all five outcomes of a verified exhaustive election quoted at 80% of fair value",
        elec,
        RelationshipType.EXHAUSTIVE_PARTITION,
        pres,
        Template.YES_BASKET,
        tuple(_yes(m) for m in pres),
        "FEE_ADJUSTED_CANDIDATE",
        (),
        tuple(Override(m, scaled(elec.by_id(m).fair, Fraction(8, 10))) for m in pres),
        window=6000,
        eval_after=4000,
    )
    # 3. stale data: implication pair mispriced while its feed lags by 5 s
    champ, finals = impl.market_ids()
    ffin = impl.by_id(finals).fair
    add(
        "S3-stale-feed",
        ScenarioKind.STALE_DATA_DISCREPANCY,
        "champion/finals pair appears mispriced, but the feed is delayed 5 s (books stale)",
        impl,
        RelationshipType.IMPLICATION,
        (champ, finals),
        Template.IMPLICATION,
        (_no(champ), _yes(finals)),
        "STALE_DATA",
        ("BOOK_TOO_OLD",),
        (Override(champ, shifted(ffin, Fraction(6, 100))),),
        window=8000,
        eval_after=6500,
        lag=5000,
    )
    # 4. eliminated by fees: weather basket YES asks sum to exactly $0.99
    bins = tuple(weather.market_ids())
    add(
        "S4-fees-eliminate",
        ScenarioKind.ELIMINATED_BY_FEES,
        "temperature-bin YES asks sum to $0.99: 1c pre-fee edge, negative after fees",
        weather,
        RelationshipType.DISJOINT_INTERVALS,
        bins,
        Template.YES_BASKET,
        tuple(_yes(m) for m in bins),
        "THEORETICAL_ONLY",
        ("FEES_EXCEED_EDGE",),
        _basket_with_ask_total(weather, bins, Decimal("0.99")),
        window=6000,
        eval_after=4000,
    )
    # 5. eliminated by depth: 3-way soccer basket cheap for a single contract only
    ev2 = tuple(m.market.market_id for m in sports.markets[2:5])
    add(
        "S5-depth-eliminates",
        ScenarioKind.ELIMINATED_BY_DEPTH,
        "3-way soccer YES basket ~15% cheap at top of book but only 1 contract deep",
        sports,
        RelationshipType.EXHAUSTIVE_PARTITION,
        ev2,
        Template.YES_BASKET,
        tuple(_yes(m) for m in ev2),
        "INSUFFICIENT_LIQUIDITY",
        ("EDGE_EXHAUSTED_BY_DEPTH",),
        tuple(
            Override(
                m,
                scaled(sports.by_id(m).fair, Fraction(85, 100)),
                thin_side=Side.NO,
                top_qty=Decimal(1),
                depth_fair=shifted(sports.by_id(m).fair, Fraction(2, 100)),
            )
            for m in ev2
        ),
        window=6000,
        eval_after=4000,
    )
    # 6. static genuine inconsistency: frozen 3-way soccer book, basket at 82% of fair
    add(
        "S6-static-genuine",
        ScenarioKind.STATIC_GENUINE_INCONSISTENCY,
        "frozen (static) 3-way soccer book whose YES basket costs well under $1 with depth",
        sports,
        RelationshipType.EXHAUSTIVE_PARTITION,
        ev2,
        Template.YES_BASKET,
        tuple(_yes(m) for m in ev2),
        "FEE_ADJUSTED_CANDIDATE",
        (),
        tuple(Override(m, scaled(sports.by_id(m).fair, Fraction(82, 100))) for m in ev2),
        window=10_000,
        eval_after=7000,
        freeze=True,
    )
    # 7. ambiguous settlement: primary NO basket looks cheap but relationship is unverifiable
    prim = tuple(m.market.market_id for m in elec.markets[5:8])
    add(
        "S7-ambiguous-settlement",
        ScenarioKind.AMBIGUOUS_SETTLEMENT,
        "primary nominee NO basket looks cheap, but withdrawal/no-nominee rules are unspecified",
        elec,
        RelationshipType.MUTUALLY_EXCLUSIVE,
        prim,
        Template.NO_BASKET,
        tuple(_no(m) for m in prim),
        "INVALID_RELATIONSHIP",
        ("RELATIONSHIP_NOT_VERIFIED",),
        tuple(Override(m, scaled(elec.by_id(m).fair, Fraction(5, 4))) for m in prim),
        window=6000,
        eval_after=4000,
    )
    # 8. short-lived: >=0.4 overpriced vs >=0.3 for ~600 ms (a couple of updates)
    add(
        "S8-short-lived",
        ScenarioKind.SHORT_LIVED_INCONSISTENCY,
        "YES(CPI>=0.4) overpriced vs YES(CPI>=0.3) for ~600 ms only",
        econ,
        RelationshipType.NESTED_THRESHOLDS,
        chain,
        Template.IMPLICATION,
        (_no(ge04), _yes(ge03)),
        "DEPTH_SUPPORTED",
        ("DURATION_BELOW_MINIMUM",),
        (Override(ge04, shifted(f03, Fraction(9, 100))),),
        window=600,
        eval_after=450,
    )
    return out


def _basket_with_ask_total(
    fam: Family, members: tuple[str, ...], total: Decimal
) -> tuple[Override, ...]:
    """Quote each member so its best YES ask is a cent price and the asks sum to ``total``.

    Setting fair' = ask - h makes the NO bid exactly 1 - ask (on the cent grid), hence YES ask
    exactly ``ask``. Cents are allocated by largest remainder of the current fair values.
    """
    hs = Fraction(fam.by_id(members[0]).ladder.half_spread)
    cents_total = int(total * 100)

    def asks() -> dict[str, Fraction]:
        fairs = [fam.by_id(m).fair() for m in members]
        s = sum(fairs)
        raw = [f / s * cents_total for f in fairs]
        base = [max(2, int(r)) for r in raw]
        rem = cents_total - sum(base)
        order = sorted(range(len(members)), key=lambda i: (raw[i] - int(raw[i]), -i), reverse=True)
        i = 0
        while rem != 0:
            j = order[i % len(order)]
            step = 1 if rem > 0 else -1
            if base[j] + step >= 2:
                base[j] += step
                rem -= step
            i += 1
        return {m: Fraction(c, 100) for m, c in zip(members, base, strict=True)}

    return tuple(Override(m, offset_ask(asks, m, hs)) for m in members)

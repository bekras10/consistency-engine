"""The six synthetic market families (spec 3.1) and their ground-truth relationships.

All tickers start with ``SYN`` so they can never be confused with real exchange series.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from fractions import Fraction
from typing import Any

from consistency_core.models.common import DataSourceKind, MarketStatus, Provenance
from consistency_core.models.market import Event, Market, Series
from consistency_core.models.relationship import RelationshipType, VerificationStatus
from consistency_core.models.settlement import (
    Comparator,
    ContractTerms,
    IntervalTerms,
    OutcomeTerms,
    PropositionTerms,
    RoundingConvention,
    RoundingMode,
    SettlementSpec,
    ThresholdTerms,
)
from consistency_core.money import dec
from consistency_core.relationships.numeric import settles_yes
from consistency_core.ticks import parse_price_grid
from consistency_simulation import GENERATOR_VERSION
from consistency_simulation.latent import (
    BernoulliLatent,
    CategoricalLatent,
    ChainLatent,
    CompositeLatent,
    GridLatent,
    LatentModel,
)
from consistency_simulation.quoting import GridCache, LadderSpec

SIM_EPOCH = datetime(2026, 7, 15, 14, 0, tzinfo=UTC)
SIM_EPOCH_MS = int(SIM_EPOCH.timestamp() * 1000)

GRID_CENT: dict[str, Any] = {"tick_size": 1}
GRID_MILLI: dict[str, Any] = {"tick_size_dollars": "0.001"}
GRID_TAPERED: dict[str, Any] = {
    "price_ranges": [
        {"start": "0.0000", "end": "0.1000", "step": "0.0010"},
        {"start": "0.1000", "end": "0.9000", "step": "0.0100"},
        {"start": "0.9000", "end": "1.0000", "step": "0.0010"},
    ]
}


@dataclass(frozen=True)
class ExpectedRelationship:
    relationship_type: RelationshipType
    members: tuple[str, ...]
    exhaustive: bool
    expected_status: VerificationStatus
    note: str


@dataclass
class SimMarket:
    market: Market
    ladder: LadderSpec
    fair: Callable[[], Fraction]
    grid: GridCache


@dataclass
class Family:
    family_id: str
    kind: str
    series: list[Series]
    events: list[Event]
    markets: list[SimMarket]
    latent: LatentModel
    volatility: int
    expected_relationships: list[ExpectedRelationship] = field(default_factory=list)

    def market_ids(self) -> list[str]:
        return [m.market.market_id for m in self.markets]

    def by_id(self, market_id: str) -> SimMarket:
        for m in self.markets:
            if m.market.market_id == market_id:
                return m
        raise KeyError(market_id)


@dataclass(frozen=True)
class BuildContext:
    seed: int
    dataset_id: str
    instance: int = 0

    @property
    def suffix(self) -> str:
        return "" if self.instance == 0 else f"-I{self.instance:04d}"

    def provenance(self) -> Provenance:
        return Provenance(
            source_kind=DataSourceKind.SYNTHETIC,
            source_id="synthetic-exchange",
            dataset_id=self.dataset_id,
            generator_version=GENERATOR_VERSION,
            seed=self.seed,
        )


def _outcome_prob(
    latent: CategoricalLatent, outcome: str, scale: Fraction = Fraction(1)
) -> Callable[[], Fraction]:
    return lambda: latent.prob(outcome) * scale


def _range_prob(latent: GridLatent, k_lo: int, k_hi: int) -> Callable[[], Fraction]:
    return lambda: latent.prob_range(k_lo, k_hi)


def _ladder(
    hs: str, levels: int, lo: int, hi: int, gaps: tuple[int, ...], frac: bool = False
) -> LadderSpec:
    return LadderSpec(
        half_spread=dec(hs), levels=levels, qty_lo=lo, qty_hi=hi, fractional=frac, gaps=gaps
    )


def _market(
    ctx: BuildContext,
    *,
    ticker: str,
    event_id: str,
    series_id: str,
    title: str,
    rules: str,
    source: str,
    settlement: SettlementSpec,
    grid_meta: dict[str, Any],
    quantity_increment: str = "1",
) -> Market:
    return Market(
        market_id=ticker,
        ticker=ticker,
        event_id=event_id,
        series_id=series_id,
        title=title,
        description=f"SYNTHETIC market. {title}",
        status=MarketStatus.OPEN,
        open_time=SIM_EPOCH - timedelta(days=1),
        close_time=SIM_EPOCH + timedelta(days=3),
        expiration_time=SIM_EPOCH + timedelta(days=4),
        settlement_source=source,
        settlement_rules=rules,
        rules_version="1",
        price_grid=parse_price_grid(grid_meta),
        quantity_increment=dec(quantity_increment),
        settlement=settlement,
        exchange_metadata={"grid": grid_meta, "synthetic": True},
        provenance=ctx.provenance(),
    )


def _series(ctx: BuildContext, sid: str, title: str, category: str) -> Series:
    return Series(series_id=sid, title=title, category=category, provenance=ctx.provenance())


# ----------------------------------------------------------------------------------- 1 election
def election_family(ctx: BuildContext) -> Family:
    s = ctx.suffix
    series_id = f"SYNPRES{s}"
    ev_main = f"SYNPRES-2026{s}"
    ev_primary = f"SYNPRIM-2026{s}"
    outcomes = ["CANDA", "CANDB", "CANDC", "CANDD", "OTHER"]
    latent = CategoricalLatent(
        weights={"CANDA": 380, "CANDB": 300, "CANDC": 160, "CANDD": 100, "OTHER": 60}
    )
    primary = CategoricalLatent(weights={"PX": 450, "PY": 350, "PZ": 200})
    src = "Synthetic Election Commission"
    markets: list[SimMarket] = []
    for o in outcomes:
        label = "any candidate not otherwise listed" if o == "OTHER" else f"candidate {o[-1]}"
        settlement = SettlementSpec(
            underlying_id=f"synthetic-presidential-2026-winner{s}",
            measurement="certified winner of the synthetic presidential election",
            location="Synthetica",
            observation_window="2026-11-03 election, certified by 2026-12-15",
            settlement_source=src,
            methodology="official certification",
            early_close_policy="none",
            exceptional_resolution=(),
            terms=OutcomeTerms(outcome_id=o),
        )
        mk = _market(
            ctx,
            ticker=f"{ev_main}-{o}",
            event_id=ev_main,
            series_id=series_id,
            title=f"Will {label} win the 2026 synthetic presidential election?",
            rules=(
                f"Resolves YES if {label} is certified winner by {src}. Exactly one listed "
                "outcome resolves YES; OTHER covers every candidate not listed."
            ),
            source=src,
            settlement=settlement,
            grid_meta=GRID_CENT,
        )
        markets.append(
            SimMarket(
                market=mk,
                ladder=_ladder("0.01", 5, 40, 400, (1, 1, 2)),
                fair=_outcome_prob(latent, o),
                grid=GridCache.of(GRID_CENT),
            )
        )
    primary_markets: list[SimMarket] = []
    for o in ["PX", "PY", "PZ"]:
        settlement = SettlementSpec(
            underlying_id=f"synthetic-primary-2026-nominee{s}",
            measurement="nominee of the synthetic primary",
            location="Synthetica",
            observation_window="2026 primary season",
            settlement_source=src,
            methodology="party announcement",
            exceptional_resolution=None,  # withdrawal / contested-convention handling unknown
            terms=OutcomeTerms(outcome_id=o),
        )
        mk = _market(
            ctx,
            ticker=f"{ev_primary}-{o}",
            event_id=ev_primary,
            series_id=series_id,
            title=f"Will {o} be the synthetic primary nominee?",
            rules=(
                f"Resolves YES if {o} is announced nominee. Rules do not specify what happens "
                "if no nominee is announced or a nominee withdraws."
            ),
            source=src,
            settlement=settlement,
            grid_meta=GRID_CENT,
        )
        primary_markets.append(
            SimMarket(
                market=mk,
                ladder=_ladder("0.01", 4, 30, 250, (1, 2)),
                fair=_outcome_prob(primary, o, Fraction(9, 10)),
                grid=GridCache.of(GRID_CENT),
            )
        )

    fam = Family(
        family_id=f"election{s}",
        kind="election",
        series=[_series(ctx, series_id, "Synthetic presidential election", "politics")],
        events=[
            Event(
                event_id=ev_main,
                series_id=series_id,
                title="2026 synthetic presidential election winner",
                mutually_exclusive=True,
                outcome_set_complete=True,
                outcome_universe=tuple(outcomes),
                provenance=ctx.provenance(),
            ),
            Event(
                event_id=ev_primary,
                series_id=series_id,
                title="2026 synthetic primary nominee",
                mutually_exclusive=True,
                outcome_set_complete=None,
                outcome_universe=None,
                provenance=ctx.provenance(),
            ),
        ],
        markets=markets + primary_markets,
        latent=CompositeLatent([latent, primary]),
        volatility=12,
    )
    fam.expected_relationships = [
        ExpectedRelationship(
            RelationshipType.EXHAUSTIVE_PARTITION,
            tuple(m.market.market_id for m in markets),
            True,
            VerificationStatus.VERIFIED,
            "complete outcome set with catch-all; exactly one resolves YES",
        ),
        ExpectedRelationship(
            RelationshipType.MUTUALLY_EXCLUSIVE,
            tuple(m.market.market_id for m in primary_markets),
            False,
            VerificationStatus.CANDIDATE_REVIEW,
            "exceptional resolution unspecified -> cannot be verified automatically",
        ),
    ]
    return fam


# ------------------------------------------------------------------------------ 2 econ thresholds
def econ_family(ctx: BuildContext) -> Family:
    s = ctx.suffix
    series_1d = f"SYNCPI{s}"
    series_2d = f"SYNCPI2D{s}"
    ev_1d = f"SYNCPI-26AUG{s}"
    ev_2d = f"SYNCPI2D-26AUG{s}"
    # raw MoM % change on a 0.005 lattice from -0.300 to 0.800
    latent = GridLatent(
        start=Fraction(-3, 10), step_size=Fraction(1, 200), n_states=221, center=110, half_width=40
    )
    src = "Synthetic Bureau of Statistics"
    base: dict[str, Any] = {
        "underlying_id": f"synthetic-cpi-mom-2026-08{s}",
        "measurement": "CPI-U month-over-month % change, seasonally adjusted",
        "location": "Synthetica",
        "observation_window": "2026-08",
        "settlement_source": src,
        "unit": "percent",
        "early_close_policy": "none",
        "exceptional_resolution": (),
    }
    markets: list[SimMarket] = []
    chain: list[str] = []
    for t in ["0.4", "0.3", "0.2", "0.1"]:
        rounding = RoundingConvention(mode=RoundingMode.HALF_UP, increment=dec("0.1"))
        terms = ThresholdTerms(comparator=Comparator.GE, value=dec(t))
        settlement = SettlementSpec(
            **base,
            methodology="headline figure as published (1 dp)",
            rounding=rounding,
            terms=terms,
        )
        ticker = f"{ev_1d}-GE{t}"
        chain.append(ticker)
        markets.append(
            _threshold_market(
                ctx,
                latent,
                ticker,
                ev_1d,
                series_1d,
                src,
                settlement,
                f"Will August 2026 synthetic CPI MoM be >= {t}%?",
                "published to one decimal place",
            )
        )
    rounding2 = RoundingConvention(mode=RoundingMode.HALF_UP, increment=dec("0.01"))
    terms2 = ThresholdTerms(comparator=Comparator.GT, value=dec("0.27"))
    settlement2 = SettlementSpec(
        **base, methodology="computed from index levels (2 dp)", rounding=rounding2, terms=terms2
    )
    decoy = f"{ev_2d}-GT0.27"
    markets.append(
        _threshold_market(
            ctx,
            latent,
            decoy,
            ev_2d,
            series_2d,
            src,
            settlement2,
            "Will August 2026 synthetic CPI MoM be above 0.27%?",
            "computed from index levels, rounded to two decimals",
        )
    )
    fam = Family(
        family_id=f"econ{s}",
        kind="economic_indicator",
        series=[
            _series(ctx, series_1d, "Synthetic CPI MoM (published, 1 dp)", "economics"),
            _series(ctx, series_2d, "Synthetic CPI MoM (index-derived, 2 dp)", "economics"),
        ],
        events=[
            Event(
                event_id=ev_1d,
                series_id=series_1d,
                title="Aug 2026 synthetic CPI",
                mutually_exclusive=False,
                provenance=ctx.provenance(),
            ),
            Event(
                event_id=ev_2d,
                series_id=series_2d,
                title="Aug 2026 synthetic CPI (2 dp)",
                mutually_exclusive=False,
                provenance=ctx.provenance(),
            ),
        ],
        markets=markets,
        latent=latent,
        volatility=2,
    )
    fam.expected_relationships = [
        ExpectedRelationship(
            RelationshipType.NESTED_THRESHOLDS,
            tuple(chain),
            False,
            VerificationStatus.VERIFIED,
            ">= 0.4 => >= 0.3 => >= 0.2 => >= 0.1 on the same 1-dp figure",
        ),
        ExpectedRelationship(
            RelationshipType.IMPLICATION,
            (f"{ev_1d}-GE0.3", decoy),
            False,
            VerificationStatus.REJECTED,
            "Test G: x=0.25 reports 0.3 (>= 0.3 YES) but 0.25 (> 0.27 NO)",
        ),
    ]
    return fam


def _threshold_market(
    ctx: BuildContext,
    latent: GridLatent,
    ticker: str,
    event_id: str,
    series_id: str,
    src: str,
    settlement: SettlementSpec,
    title: str,
    rounding_text: str,
) -> SimMarket:
    terms = settlement.terms
    assert isinstance(terms, ThresholdTerms | IntervalTerms)
    mk = _market(
        ctx,
        ticker=ticker,
        event_id=event_id,
        series_id=series_id,
        title=title,
        rules=f"{title} Settles on the {src} figure {rounding_text}.",
        source=src,
        settlement=settlement,
        grid_meta=GRID_TAPERED,
    )
    k_lo, k_hi = _yes_index_range(latent, terms, settlement.rounding)
    return SimMarket(
        market=mk,
        ladder=_ladder("0.01", 5, 25, 300, (1, 1, 2)),
        fair=_range_prob(latent, k_lo, k_hi),
        grid=GridCache.of(GRID_TAPERED),
    )


def _yes_index_range(
    latent: GridLatent, terms: ContractTerms, rounding: RoundingConvention | None
) -> tuple[int, int]:
    """Lattice indices where the contract settles YES; asserts the set is contiguous."""
    assert isinstance(terms, ThresholdTerms | IntervalTerms)
    hits = [k for k in range(latent.n_states) if settles_yes(terms, rounding, latent.value(k))]
    if not hits:
        return 0, -1
    assert hits == list(range(hits[0], hits[-1] + 1)), "YES set must be an interval"
    return hits[0], hits[-1]


# ----------------------------------------------------------------------------- 3 temperature bins
def weather_family(ctx: BuildContext) -> Family:
    s = ctx.suffix
    series_id = f"SYNHIGH{s}"
    ev = f"SYNHIGH-NYC-26JUL16{s}"
    latent = GridLatent(
        start=Fraction(70), step_size=Fraction(1, 10), n_states=301, center=160, half_width=50
    )
    src = "Synthetic Weather Service climate report"
    rounding = RoundingConvention(mode=RoundingMode.HALF_UP, increment=dec("1"))
    bins: list[tuple[str, IntervalTerms]] = [
        ("B-LT80", IntervalTerms(lower=None, upper=dec("80"), upper_inclusive=False)),
        ("B80-84", IntervalTerms(lower=dec("80"), upper=dec("85"), upper_inclusive=False)),
        ("B85-89", IntervalTerms(lower=dec("85"), upper=dec("90"), upper_inclusive=False)),
        ("B-GE90", IntervalTerms(lower=dec("90"), lower_inclusive=True, upper=None)),
    ]
    markets: list[SimMarket] = []
    for code, terms in bins:
        settlement = SettlementSpec(
            underlying_id=f"synthetic-nyc-central-park-high-2026-07-16{s}",
            measurement="daily maximum temperature",
            location="Synthetic Central Park station",
            observation_window="2026-07-16 local standard time",
            settlement_source=src,
            methodology="final climate report, whole degrees F",
            unit="degF",
            rounding=rounding,
            early_close_policy="none",
            exceptional_resolution=(),
            terms=terms,
        )
        lo = "-inf" if terms.lower is None else str(terms.lower)
        hi = "+inf" if terms.upper is None else str(terms.upper)
        title = f"Will the synthetic NYC high on Jul 16 be in [{lo}, {hi})°F?"
        mk = _market(
            ctx,
            ticker=f"{ev}-{code}",
            event_id=ev,
            series_id=series_id,
            title=title,
            rules=f"{title} Uses {src}, reported in whole degrees (half up).",
            source=src,
            settlement=settlement,
            grid_meta=GRID_CENT,
        )
        k_lo, k_hi = _yes_index_range(latent, terms, rounding)
        markets.append(
            SimMarket(
                market=mk,
                ladder=_ladder("0.01", 4, 30, 250, (1, 2)),
                fair=_range_prob(latent, k_lo, k_hi),
                grid=GridCache.of(GRID_CENT),
            )
        )
    fam = Family(
        family_id=f"weather{s}",
        kind="temperature_intervals",
        series=[_series(ctx, series_id, "Synthetic NYC daily high", "climate")],
        events=[
            Event(
                event_id=ev,
                series_id=series_id,
                title="Synthetic NYC high, Jul 16",
                mutually_exclusive=True,
                provenance=ctx.provenance(),
            )
        ],
        markets=markets,
        latent=latent,
        volatility=3,
    )
    fam.expected_relationships = [
        ExpectedRelationship(
            RelationshipType.DISJOINT_INTERVALS,
            tuple(fam.market_ids()),
            True,
            VerificationStatus.VERIFIED,
            "bins partition the real line on the whole-degree report",
        )
    ]
    return fam


# --------------------------------------------------------------------------------- 4 sports
def sports_family(ctx: BuildContext) -> Family:
    s = ctx.suffix
    series_id = f"SYNSOCCER{s}"
    ev1 = f"SYNSOCCER-26JUL16-NYCBOS{s}"
    ev2 = f"SYNSOCCER-26JUL16-LASEA{s}"
    m1 = CategoricalLatent(weights={"HOME": 420, "AWAY": 330, "DRAW": 250})
    m2 = CategoricalLatent(weights={"HOME": 380, "AWAY": 370, "DRAW": 250})
    src = "Synthetic Soccer League official result"
    markets: list[SimMarket] = []
    listings: list[tuple[str, CategoricalLatent, list[str]]] = [
        (ev1, m1, ["HOME", "AWAY"]),
        (ev2, m2, ["HOME", "AWAY", "DRAW"]),
    ]
    for ev, lat, outs in listings:
        for o in outs:
            settlement = SettlementSpec(
                underlying_id=f"{ev.lower()}-result",
                measurement="90-minute regulation result",
                location="Synthetic Soccer League",
                observation_window="2026-07-16 match",
                settlement_source=src,
                methodology="official result after 90 minutes plus stoppage",
                early_close_policy="none",
                exceptional_resolution=(),
                terms=OutcomeTerms(outcome_id=o),
            )
            mk = _market(
                ctx,
                ticker=f"{ev}-{o}",
                event_id=ev,
                series_id=series_id,
                title=f"{ev}: will the result be {o}?",
                rules=f"Resolves YES if the regulation result is {o} per {src}.",
                source=src,
                settlement=settlement,
                grid_meta=GRID_CENT,
            )
            markets.append(
                SimMarket(
                    market=mk,
                    ladder=_ladder("0.01", 4, 30, 300, (1, 1, 2)),
                    fair=_outcome_prob(lat, o),
                    grid=GridCache.of(GRID_CENT),
                )
            )

    fam = Family(
        family_id=f"sports{s}",
        kind="sports_match",
        series=[_series(ctx, series_id, "Synthetic soccer results", "sports")],
        events=[
            Event(
                event_id=ev1,
                series_id=series_id,
                title="NYC vs BOS (draw not listed)",
                mutually_exclusive=True,
                outcome_set_complete=False,
                outcome_universe=("HOME", "AWAY", "DRAW"),
                provenance=ctx.provenance(),
            ),
            Event(
                event_id=ev2,
                series_id=series_id,
                title="LA vs SEA (3-way)",
                mutually_exclusive=True,
                outcome_set_complete=True,
                outcome_universe=("HOME", "AWAY", "DRAW"),
                provenance=ctx.provenance(),
            ),
        ],
        markets=markets,
        latent=CompositeLatent([m1, m2]),
        volatility=10,
    )
    fam.expected_relationships = [
        ExpectedRelationship(
            RelationshipType.MUTUALLY_EXCLUSIVE,
            (f"{ev1}-HOME", f"{ev1}-AWAY"),
            False,
            VerificationStatus.VERIFIED,
            "draw is possible but unlisted: exclusive, NOT exhaustive",
        ),
        ExpectedRelationship(
            RelationshipType.EXHAUSTIVE_PARTITION,
            (f"{ev2}-HOME", f"{ev2}-AWAY", f"{ev2}-DRAW"),
            True,
            VerificationStatus.VERIFIED,
            "complete 3-way listing",
        ),
    ]
    return fam


# ---------------------------------------------------------------------------- 5 equivalent pair
def equivalent_family(ctx: BuildContext) -> Family:
    s = ctx.suffix
    latent = BernoulliLatent(permille=420)
    prop = f"synthetic-fomc-2026-09-hike{s}"
    src = "Synthetic Central Bank statement"
    defs = [
        (
            f"SYNFEDHIKE{s}",
            f"SYNFEDHIKE-26SEP{s}",
            "Will the synthetic central bank raise rates at its September 2026 meeting?",
            src,
        ),
        (
            f"SYNFEDUB{s}",
            f"SYNFEDUB-26SEP{s}",
            "Will the synthetic policy-rate upper bound exceed "
            "4.50% after the September 2026 meeting?",
            src,
        ),
        (
            f"SYNFEDALT{s}",
            f"SYNFEDALT-26SEP{s}",
            "Will the synthetic central bank raise rates at its September 2026 meeting?",
            "Synthetic Wire Service headline",
        ),
    ]
    markets: list[SimMarket] = []
    series: list[Series] = []
    events: list[Event] = []
    for series_id, ev, title, source in defs:
        settlement = SettlementSpec(
            underlying_id=prop,
            measurement="policy decision at the September 2026 meeting",
            location="Synthetica",
            observation_window="2026-09 meeting",
            settlement_source=source,
            methodology="official statement" if source == src else "first wire headline",
            early_close_policy="none",
            exceptional_resolution=(),
            terms=PropositionTerms(proposition_id=prop),
        )
        ticker = f"{ev}-T"
        mk = _market(
            ctx,
            ticker=ticker,
            event_id=ev,
            series_id=series_id,
            title=title,
            rules=f"{title} Resolves per {source}.",
            source=source,
            settlement=settlement,
            grid_meta=GRID_MILLI,
            quantity_increment="0.01",
        )
        markets.append(
            SimMarket(
                market=mk,
                ladder=_ladder("0.012", 4, 10, 120, (2, 3), frac=True),
                fair=latent.prob,
                grid=GridCache.of(GRID_MILLI),
            )
        )
        series.append(_series(ctx, series_id, title, "macro"))
        events.append(
            Event(event_id=ev, series_id=series_id, title=title, provenance=ctx.provenance())
        )
    fam = Family(
        family_id=f"equivalent{s}",
        kind="equivalent_contracts",
        series=series,
        events=events,
        markets=markets,
        latent=latent,
        volatility=8,
    )
    ids = fam.market_ids()
    fam.expected_relationships = [
        ExpectedRelationship(
            RelationshipType.EQUIVALENT,
            (ids[0], ids[1]),
            False,
            VerificationStatus.VERIFIED,
            "manual review: a hike is the only path above 4.50% at this meeting",
        ),
        ExpectedRelationship(
            RelationshipType.EQUIVALENT,
            (ids[0], ids[2]),
            False,
            VerificationStatus.REJECTED,
            "identical titles, different settlement source: false equivalence",
        ),
    ]
    return fam


# ------------------------------------------------------------------------------ 6 implication
def implication_family(ctx: BuildContext) -> Family:
    s = ctx.suffix
    latent = ChainLatent(a_permille=550, b_permille=480)
    src = "Synthetic Basketball League"
    defs = [
        (
            f"SYNCHAMP{s}",
            f"SYNCHAMP-26{s}",
            "Will the Synthetic Owls win the 2026 championship?",
            "synthetic-owls-2026-champion",
            lambda: latent.prob_a(),
        ),
        (
            f"SYNFINALS{s}",
            f"SYNFINALS-26{s}",
            "Will the Synthetic Owls reach the 2026 finals?",
            "synthetic-owls-2026-finalist",
            lambda: latent.prob_b(),
        ),
    ]
    markets: list[SimMarket] = []
    series: list[Series] = []
    events: list[Event] = []
    for series_id, ev, title, prop, fair in defs:
        settlement = SettlementSpec(
            underlying_id=f"synthetic-owls-2026-postseason{s}",
            measurement=prop,
            location="Synthetic Basketball League",
            observation_window="2026 postseason",
            settlement_source=src,
            methodology="official league standings",
            early_close_policy="none",
            exceptional_resolution=(),
            terms=PropositionTerms(proposition_id=f"{prop}{s}"),
        )
        mk = _market(
            ctx,
            ticker=f"{ev}-OWLS",
            event_id=ev,
            series_id=series_id,
            title=title,
            rules=f"{title} Resolves per {src} official results.",
            source=src,
            settlement=settlement,
            grid_meta=GRID_CENT,
        )
        markets.append(
            SimMarket(
                market=mk,
                ladder=_ladder("0.01", 5, 40, 350, (1, 1, 2)),
                fair=fair,
                grid=GridCache.of(GRID_CENT),
            )
        )
        series.append(_series(ctx, series_id, title, "sports"))
        events.append(
            Event(event_id=ev, series_id=series_id, title=title, provenance=ctx.provenance())
        )
    fam = Family(
        family_id=f"implication{s}",
        kind="implication_pair",
        series=series,
        events=events,
        markets=markets,
        latent=latent,
        volatility=10,
    )
    ids = fam.market_ids()
    fam.expected_relationships = [
        ExpectedRelationship(
            RelationshipType.IMPLICATION,
            (ids[0], ids[1]),
            False,
            VerificationStatus.VERIFIED,
            "manual review: winning the championship requires reaching finals",
        ),
    ]
    return fam


FAMILY_BUILDERS: dict[str, Callable[[BuildContext], Family]] = {
    "election": election_family,
    "econ": econ_family,
    "weather": weather_family,
    "sports": sports_family,
    "equivalent": equivalent_family,
    "implication": implication_family,
}

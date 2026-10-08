"""The deterministic synthetic exchange: latent dynamics -> books -> wire stream with faults.

Exchange-side truth (``RestingBook`` per market) always evolves; faults only affect what the
client receives. Sequence numbers are per subscription (``sid``), shared by all markets of a
family. After any delivery fault that a client can detect (gap, malformed message, reordering)
the recorded stream contains the client's resubscription: fresh snapshots on a new ``sid``
after ``recovery_delay_ms``. This is how a recording made by a correctly behaving client looks.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from fractions import Fraction

from consistency_core.events import (
    ConnectionEvent,
    HeartbeatEvent,
    MarketLifecycleEvent,
    MarketStatusEvent,
    OrderBookDeltaEvent,
    OrderBookSnapshotEvent,
    RawWireEvent,
    StreamMessage,
)
from consistency_core.models.common import MarketStatus, Side
from consistency_core.models.market import Catalog, Market
from consistency_core.models.settlement import (
    Comparator,
    RoundingConvention,
    RoundingMode,
    SettlementSpec,
    ThresholdTerms,
)
from consistency_core.money import dec, dec_str
from consistency_simulation.families import (
    FAMILY_BUILDERS,
    SIM_EPOCH_MS,
    BuildContext,
    Family,
    SimMarket,
    _threshold_market,
)
from consistency_simulation.latent import GridLatent
from consistency_simulation.quoting import RestingBook, ThinTop, requote
from consistency_simulation.scenarios import ExpectedFinding, Injection, standard_injections


class FaultKind(StrEnum):
    DUPLICATE = "duplicate"
    GAP = "gap"
    REORDER = "reorder"
    MALFORMED = "malformed"
    DISCONNECT = "disconnect"
    LAG = "lag"
    PAUSE = "pause"
    CREATE_MARKET = "create_market"
    REMOVE_MARKET = "remove_market"


@dataclass(frozen=True)
class FaultSpec:
    kind: FaultKind
    at_ms: int
    family: str
    duration_ms: int = 0
    extra_ms: int = 0
    market_index: int = 0


@dataclass(frozen=True)
class SessionConfig:
    name: str
    seed: int
    duration_ms: int
    description: str
    tick_ms: int = 250
    families: tuple[str, ...] = tuple(FAMILY_BUILDERS)
    instances: int = 1
    volatility_pct: int = 100
    step_permille: int = 150
    churn_permille: int = 30
    heartbeat_ms: int = 1000
    recovery_delay_ms: int = 250
    latency_ms: tuple[int, int] = (5, 25)
    faults: tuple[FaultSpec, ...] = ()
    inject_standard_scenarios: bool = False
    scenario_t0_ms: int = 4000

    def to_json(self) -> dict[str, object]:
        return {
            "name": self.name,
            "seed": self.seed,
            "duration_ms": self.duration_ms,
            "description": self.description,
            "tick_ms": self.tick_ms,
            "families": list(self.families),
            "instances": self.instances,
            "volatility_pct": self.volatility_pct,
            "step_permille": self.step_permille,
            "churn_permille": self.churn_permille,
            "heartbeat_ms": self.heartbeat_ms,
            "recovery_delay_ms": self.recovery_delay_ms,
            "latency_ms": list(self.latency_ms),
            "faults": [
                {
                    "kind": f.kind.value,
                    "at_ms": f.at_ms,
                    "family": f.family,
                    "duration_ms": f.duration_ms,
                    "extra_ms": f.extra_ms,
                    "market_index": f.market_index,
                }
                for f in self.faults
            ],
            "inject_standard_scenarios": self.inject_standard_scenarios,
            "scenario_t0_ms": self.scenario_t0_ms,
        }


@dataclass
class _Emission:
    idx: int
    emitted_ts: int
    conn: str
    event: object
    tag: str | None = None
    reorder_ms: int = 0
    duplicate: bool = False


@dataclass
class _FamilyRuntime:
    key: str
    family: Family
    conn_gen: int = 0
    sid: str = ""
    seq: int = 0
    books: dict[str, RestingBook] = field(default_factory=dict)
    active: list[str] = field(default_factory=list)
    paused_until: dict[str, int] = field(default_factory=dict)
    disconnected_until: int | None = None
    pending: list[FaultKind] = field(default_factory=list)
    recovery_at: int | None = None
    tag_next: str | None = None
    lag_windows: list[tuple[int, int, int]] = field(default_factory=list)

    @property
    def conn(self) -> str:
        return f"conn-{self.family.family_id}-{self.conn_gen}"


@dataclass
class SessionResult:
    config: SessionConfig
    catalog: Catalog
    messages: list[StreamMessage]
    expected_findings: list[ExpectedFinding]
    expected_relationships: list[dict[str, object]]
    final_books: dict[str, dict[str, list[list[str]]]]
    counts: dict[str, int]


class SyntheticExchange:
    def __init__(self, config: SessionConfig) -> None:
        self.cfg = config
        self.rng_latent = random.Random(f"{config.seed}:latent")
        self.rng_quote = random.Random(f"{config.seed}:quote")
        self.rng_net = random.Random(f"{config.seed}:net")
        self.emissions: list[_Emission] = []
        self.sid_counter = 0
        self.counts: dict[str, int] = {}
        self.runtimes: list[_FamilyRuntime] = []
        self.extra_markets: list[Market] = []
        for inst in range(config.instances):
            ctx = BuildContext(seed=config.seed, dataset_id=config.name, instance=inst)
            for key in config.families:
                fam = FAMILY_BUILDERS[key](ctx)
                rt = _FamilyRuntime(key=key, family=fam)
                rt.active = fam.market_ids()
                rt.books = {m: RestingBook() for m in rt.active}
                self.runtimes.append(rt)
        self.injections: list[Injection] = []
        if config.inject_standard_scenarios:
            first = {rt.key: rt.family for rt in self.runtimes[: len(config.families)]}
            self.injections = standard_injections(
                first, t0_ms=config.scenario_t0_ms, epoch_ms=SIM_EPOCH_MS
            )
            for inj in self.injections:
                if inj.lag_extra_ms:
                    rt = self._runtime_by_family_id(inj.family_id)
                    rt.lag_windows.append((inj.start_ms, inj.end_ms, inj.lag_extra_ms))
        for f in config.faults:
            if f.kind is FaultKind.LAG:
                rt = self._runtime(f.family)
                rt.lag_windows.append((f.at_ms, f.at_ms + f.duration_ms, f.extra_ms))

    # ----------------------------------------------------------------------------- helpers
    def _runtime(self, key: str) -> _FamilyRuntime:
        for rt in self.runtimes:
            if rt.key == key:
                return rt
        raise KeyError(key)

    def _runtime_by_family_id(self, family_id: str) -> _FamilyRuntime:
        for rt in self.runtimes:
            if rt.family.family_id == family_id:
                return rt
        raise KeyError(family_id)

    def _count(self, key: str) -> None:
        self.counts[key] = self.counts.get(key, 0) + 1

    def _emit(self, t: int, conn: str, event: object, tag: str | None = None, **kw: object) -> None:
        self.emissions.append(
            _Emission(len(self.emissions), SIM_EPOCH_MS + t, conn, event, tag, **kw)  # type: ignore[arg-type]
        )

    def _new_sid(self) -> str:
        self.sid_counter += 1
        return str(self.sid_counter)

    def _snapshot(self, rt: _FamilyRuntime, t: int, market_id: str, tag: str | None) -> None:
        rt.seq += 1
        b = rt.books[market_id]
        ev = OrderBookSnapshotEvent(
            market_id=market_id,
            sid=rt.sid,
            seq=rt.seq,
            connection_id=rt.conn,
            exchange_ts_ms=SIM_EPOCH_MS + t,
            yes_bids=tuple(b.levels(Side.YES)),
            no_bids=tuple(b.levels(Side.NO)),
        )
        self._emit(t, rt.conn, ev, tag)
        self._count("snapshots")

    def _subscribe(self, rt: _FamilyRuntime, t: int, tag: str | None) -> None:
        rt.sid = self._new_sid()
        rt.seq = 0
        for m in rt.active:
            self._snapshot(rt, t, m, tag)

    def _active_injection(self, family_id: str, t: int) -> list[Injection]:
        return [
            i for i in self.injections if i.family_id == family_id and i.start_ms <= t < i.end_ms
        ]

    # ------------------------------------------------------------------------------- deltas
    def _delta(
        self, rt: _FamilyRuntime, t: int, mid: str, side: Side, price: Decimal, delta: Decimal
    ) -> None:
        if rt.disconnected_until is not None:
            return  # nothing reaches the client; truth already updated
        rt.seq += 1
        ev = OrderBookDeltaEvent(
            market_id=mid,
            sid=rt.sid,
            seq=rt.seq,
            connection_id=rt.conn,
            exchange_ts_ms=SIM_EPOCH_MS + t,
            side=side,
            price=price,
            delta=delta,
        )
        tag = rt.tag_next
        if rt.pending:
            fault = rt.pending.pop(0)
            self._count(f"fault_{fault.value}")
            if fault is FaultKind.GAP:
                rt.tag_next = "after_gap"
                rt.recovery_at = t + self.cfg.recovery_delay_ms
                return
            if fault is FaultKind.MALFORMED:
                payload: dict[str, object] = {
                    "type": "orderbook_delta",
                    "sid": int(rt.sid),
                    "seq": rt.seq,
                    "msg": {
                        "market_ticker": mid,
                        "price_dollars": "1.2000",  # out of range: must be rejected
                        "delta_fp": dec_str(delta),
                        "side": side.value,
                    },
                }
                raw = RawWireEvent(connection_id=rt.conn, payload=payload)  # type: ignore[arg-type]
                self._emit(t, rt.conn, raw, "malformed")
                rt.recovery_at = t + self.cfg.recovery_delay_ms
                return
            if fault is FaultKind.DUPLICATE:
                self._emit(t, rt.conn, ev, tag)
                self._emit(t, rt.conn, ev, "duplicate", duplicate=True)
                rt.tag_next = None
                self._count("deltas")
                return
            if fault is FaultKind.REORDER:
                self._emit(t, rt.conn, ev, "reordered", reorder_ms=400)
                rt.recovery_at = t + 400 + self.cfg.recovery_delay_ms
                rt.tag_next = None
                self._count("deltas")
                return
        rt.tag_next = None
        self._emit(t, rt.conn, ev, tag)
        self._count("deltas")

    # ----------------------------------------------------------------------------- faults
    def _apply_fault(self, f: FaultSpec, t: int) -> None:
        rt = self._runtime(f.family)
        if f.kind in (FaultKind.GAP, FaultKind.DUPLICATE, FaultKind.REORDER, FaultKind.MALFORMED):
            rt.pending.append(f.kind)
        elif f.kind is FaultKind.DISCONNECT:
            self._emit(
                t,
                rt.conn,
                ConnectionEvent(state="disconnected", connection_id=rt.conn),
                "disconnect",
            )
            rt.disconnected_until = t + f.duration_ms
            self._count("fault_disconnect")
        elif f.kind is FaultKind.PAUSE:
            mid = rt.active[f.market_index]
            rt.paused_until[mid] = t + f.duration_ms
            self._emit(
                t,
                rt.conn,
                MarketStatusEvent(
                    market_id=mid, status=MarketStatus.PAUSED, exchange_ts_ms=SIM_EPOCH_MS + t
                ),
                "pause",
            )
            self._count("status_changes")
        elif f.kind is FaultKind.CREATE_MARKET:
            self._create_threshold_market(rt, t)
        elif f.kind is FaultKind.REMOVE_MARKET:
            mid = rt.active[f.market_index]
            self._emit(
                t,
                rt.conn,
                MarketStatusEvent(
                    market_id=mid, status=MarketStatus.CLOSED, exchange_ts_ms=SIM_EPOCH_MS + t
                ),
                "close",
            )
            self._emit(
                t,
                rt.conn,
                MarketLifecycleEvent(
                    action="removed", market_id=mid, exchange_ts_ms=SIM_EPOCH_MS + t
                ),
                "remove",
            )
            rt.active.remove(mid)
            self._count("markets_removed")

    def _create_threshold_market(self, rt: _FamilyRuntime, t: int) -> None:
        fam = rt.family
        latent = fam.latent
        assert isinstance(latent, GridLatent), "market creation is modelled on the econ family"
        template = fam.markets[0].market
        base = template.settlement
        settlement = SettlementSpec(
            **{
                **base.model_dump(exclude={"terms", "rounding"}),
                "rounding": RoundingConvention(mode=RoundingMode.HALF_UP, increment=dec("0.1")),
                "terms": ThresholdTerms(comparator=Comparator.GE, value=dec("0.5")),
            }
        )
        ticker = f"{template.event_id}-GE0.5"
        sm: SimMarket = _threshold_market(
            BuildContext(seed=self.cfg.seed, dataset_id=self.cfg.name),
            latent,
            ticker,
            template.event_id,
            template.series_id,
            template.settlement_source,
            settlement,
            "Will August 2026 synthetic CPI MoM be >= 0.5%?",
            "published to one decimal place",
        )
        fam.markets.append(sm)
        rt.active.append(ticker)
        rt.books[ticker] = RestingBook()
        self.extra_markets.append(sm.market)
        self._requote_market(rt, t, sm, emit=False)
        self._emit(
            t,
            rt.conn,
            MarketLifecycleEvent(
                action="created",
                market_id=ticker,
                market=sm.market,
                exchange_ts_ms=SIM_EPOCH_MS + t,
            ),
            "create",
        )
        if rt.disconnected_until is None:
            self._snapshot(rt, t, ticker, "create_snapshot")
        self._count("markets_created")

    # ----------------------------------------------------------------------------- quoting
    def _requote_market(self, rt: _FamilyRuntime, t: int, sm: SimMarket, *, emit: bool) -> None:
        mid = sm.market.market_id
        fair = sm.fair()
        thin: ThinTop | None = None
        frozen = False
        for inj in self._active_injection(rt.family.family_id, t):
            frozen = frozen or inj.freeze_family
            for o in inj.overrides:
                if o.market_id == mid:
                    fair = o.fair()
                    if o.thin_side is not None:
                        assert o.top_qty is not None and o.depth_fair is not None
                        thin = ThinTop(o.thin_side, o.top_qty, o.depth_fair())
        churn = 0 if frozen else self.cfg.churn_permille
        deltas = requote(
            rt.books[mid], fair, sm.ladder, sm.grid, self.rng_quote, thin=thin, churn_permille=churn
        )
        if emit:
            for side, price, d in deltas:
                self._delta(rt, t, mid, side, price, d)

    # -------------------------------------------------------------------------------- run
    def run(
        self, observer: Callable[[int, SyntheticExchange], None] | None = None
    ) -> SessionResult:
        """Simulate the session. ``observer(t_ms, self)`` is called after every tick."""
        cfg = self.cfg
        faults = sorted(cfg.faults, key=lambda f: (f.at_ms, f.family, f.kind.value))
        # initial books + subscriptions
        for rt in self.runtimes:
            for sm in rt.family.markets:
                self._requote_market(rt, 0, sm, emit=False)
            self._emit(
                0, rt.conn, ConnectionEvent(state="connected", connection_id=rt.conn), "connect"
            )
            self._subscribe(rt, 0, "initial_snapshot")
        fi = 0
        t = cfg.tick_ms
        while t <= cfg.duration_ms:
            while fi < len(faults) and faults[fi].at_ms <= t:
                self._apply_fault(faults[fi], t)
                fi += 1
            for rt in self.runtimes:
                fam = rt.family
                if rt.disconnected_until is not None and t >= rt.disconnected_until:
                    rt.disconnected_until = None
                    rt.conn_gen += 1
                    self._emit(
                        t,
                        rt.conn,
                        ConnectionEvent(state="connected", connection_id=rt.conn),
                        "reconnect",
                    )
                    self._subscribe(rt, t, "resubscribe_snapshot")
                    rt.pending.clear()
                    rt.recovery_at = None
                frozen = any(i.freeze_family for i in self._active_injection(fam.family_id, t))
                vol = max(1, fam.volatility * cfg.volatility_pct // 100)
                if not frozen and self.rng_latent.randint(0, 999) < cfg.step_permille:
                    fam.latent.step(self.rng_latent, vol)
                for mid in list(rt.active):
                    until = rt.paused_until.get(mid)
                    if until is not None:
                        if t < until:
                            continue
                        del rt.paused_until[mid]
                        self._emit(
                            t,
                            rt.conn,
                            MarketStatusEvent(
                                market_id=mid,
                                status=MarketStatus.OPEN,
                                exchange_ts_ms=SIM_EPOCH_MS + t,
                            ),
                            "resume",
                        )
                        self._count("status_changes")
                    self._requote_market(rt, t, fam.by_id(mid), emit=True)
                if rt.recovery_at is not None and t >= rt.recovery_at:
                    rt.recovery_at = None
                    if rt.disconnected_until is None:
                        self._subscribe(rt, t, "recovery_snapshot")
                        self._count("recoveries")
                if t % cfg.heartbeat_ms == 0 and rt.disconnected_until is None:
                    self._emit(t, rt.conn, HeartbeatEvent(connection_id=rt.conn))
                    self._count("heartbeats")
            if observer is not None:
                observer(t, self)
            t += cfg.tick_ms
        messages = self._deliver()
        created = {m.market_id for m in self.extra_markets}
        markets = [
            m.market
            for rt in self.runtimes
            for m in rt.family.markets
            if m.market.market_id not in created
        ]
        catalog = Catalog(
            series=tuple(s for rt in self.runtimes for s in rt.family.series),
            events=tuple(e for rt in self.runtimes for e in rt.family.events),
            markets=tuple(markets),
        )
        final_books = {
            mid: {
                "yes": [[dec_str(p), dec_str(q)] for p, q in rt.books[mid].levels(Side.YES)],
                "no": [[dec_str(p), dec_str(q)] for p, q in rt.books[mid].levels(Side.NO)],
            }
            for rt in self.runtimes
            for mid in rt.active
        }
        expected_rels: list[dict[str, object]] = [
            {
                "relationship_type": er.relationship_type.value,
                "members": list(er.members),
                "exhaustive": er.exhaustive,
                "expected_status": er.expected_status.value,
                "note": er.note,
            }
            for rt in self.runtimes
            for er in rt.family.expected_relationships
        ]
        self.counts["messages"] = len(messages)
        return SessionResult(
            config=cfg,
            catalog=catalog,
            messages=messages,
            expected_findings=[i.finding for i in self.injections],
            expected_relationships=expected_rels,
            final_books=final_books,
            counts=dict(sorted(self.counts.items())),
        )

    # ---------------------------------------------------------------------------- delivery
    def _lag(self, conn: str, emitted_rel: int) -> int:
        for rt in self.runtimes:
            if conn.startswith(f"conn-{rt.family.family_id}-"):
                return sum(x for a, b, x in rt.lag_windows if a <= emitted_rel < b)
        return 0

    def _deliver(self) -> list[StreamMessage]:
        lo, hi = self.cfg.latency_ms
        last: dict[str, int] = {}
        timed: list[tuple[int, int, int, _Emission]] = []
        for e in self.emissions:
            base = (
                e.emitted_ts
                + self.rng_net.randint(lo, hi)
                + self._lag(e.conn, e.emitted_ts - SIM_EPOCH_MS)
            )
            if e.duplicate:
                received = last.get(e.conn, base) + self.rng_net.randint(5, 60)
            elif e.reorder_ms:
                received = max(base, last.get(e.conn, base)) + e.reorder_ms
            else:
                received = max(base, last.get(e.conn, base))
                last[e.conn] = received
            timed.append((received, e.idx, 1 if e.duplicate else 0, e))
        timed.sort(key=lambda x: (x[0], x[1], x[2]))
        out: list[StreamMessage] = []
        for pos, (received, _idx, _dup, e) in enumerate(timed):
            out.append(
                StreamMessage(
                    position=pos,
                    emitted_ts_ms=e.emitted_ts,
                    received_ts_ms=received,
                    event=e.event,  # type: ignore[arg-type]
                    synthetic_tag=e.tag,
                )
            )
        return out


def fair_snapshot(exchange: SyntheticExchange) -> dict[str, Fraction]:
    """Current fair probabilities (diagnostics / tests)."""
    return {m.market.market_id: m.fair() for rt in exchange.runtimes for m in rt.family.markets}

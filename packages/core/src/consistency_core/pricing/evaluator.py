"""Portfolio evaluator: payoff proof, depth, fees, execution gates, classification (spec 6.x).

Classification precedence (first failing step decides; exactly one primary status):

 1. INVALID_RELATIONSHIP   relationship not VERIFIED, rules changed, unknown market, portfolio
                           not drawn from the members, unsupported leg ratio, or scenarios not
                           modelable
 2. UNSYNCHRONIZED_DATA    any leg book missing or not SYNCHRONIZED
 3. STALE_DATA             max book age > max_book_age_ms, or cross-market skew >
                           max_cross_market_skew_ms
 4. INSUFFICIENT_LIQUIDITY a leg market is not OPEN, or a leg has no asks
 5. NO_OPPORTUNITY         top-of-book worst-case edge (min payoff - premium) <= 0
 6. INSUFFICIENT_LIQUIDITY max supported basket < required size (DEPTH_BELOW_MINIMUM)
 7. INSUFFICIENT_LIQUIDITY best pre-fee worst-case profit over the quantity domain <= 0
                           (EDGE_EXHAUSTED_BY_DEPTH)
 8. FEE_UNVERIFIED         any leg's fee resolution is not verified
 9. THEORETICAL_ONLY       best worst-case profit after fees <= 0 (FEES_EXCEED_EDGE)
10. DEPTH_SUPPORTED        positive after fees but an execution gate fails: no quantity has
                           execution-adjusted profit >= minimum_net_edge x quantity, duration
                           unknown/short, or a non-default maker assumption. The reported
                           quantity maximizes execution-adjusted profit among the quantities
                           meeting the edge gate (over all quantities if none does).
11. FEE_ADJUSTED_CANDIDATE every check passed

Quantity domain: multiples of the basket step (lcm of the legs' quantity increments) from
``max(minimum_available_quantity, step)`` (or exactly ``target_quantity``) up to the maximum
depth-supported basket. Domains up to ``exhaustive_search_limit`` points are searched
exhaustively. Larger domains are searched at depth breakpoints (where any leg moves to its next
price level) +/- 3 steps plus the endpoints: between breakpoints the pre-rounding objective is
linear in the quantity, so its maximum lies at a breakpoint; only sub-cent rounding effects can
differ, and the method is recorded in the certificate.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_FLOOR, Decimal
from enum import StrEnum
from fractions import Fraction

from consistency_core.fees.calculator import FeeCalculator, FeeResolution
from consistency_core.fees.model import Role
from consistency_core.fees.rounding import order_net_fee
from consistency_core.models.common import FrozenModel, MarketStatus, SyncStatus
from consistency_core.models.detection import Classification, Detection, SnapshotRef
from consistency_core.models.market import Market
from consistency_core.models.orderbook import AskLevel, OrderBook
from consistency_core.models.relationship import Relationship
from consistency_core.money import ONE, QUANTITY_DP, VWAP_QUANTUM, ZERO, ceil_to, dec_str
from consistency_core.pricing.certificate import (
    BookInput,
    EvaluationConfig,
    LegExecution,
    PortfolioRef,
    ProofCertificate,
    QuantityEvaluation,
    QuantitySearch,
    RelationshipRef,
    TimingInfo,
    TopOfBook,
    TraceStep,
)
from consistency_core.pricing.depth import walk_asks
from consistency_core.pricing.payoff import (
    PayoffAnalysis,
    PortfolioNotInRelationshipError,
    analyse_payoff,
)
from consistency_core.pricing.portfolio import Portfolio
from consistency_core.relationships.scenarios import ScenarioSpaceError
from consistency_core.serialization import canonical_json, sha256_of

BREAKPOINT_RADIUS = 3
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class Reason(StrEnum):
    RELATIONSHIP_NOT_VERIFIED = "RELATIONSHIP_NOT_VERIFIED"
    RULES_CHANGED = "RULES_CHANGED"
    MARKET_UNKNOWN = "MARKET_UNKNOWN"
    PORTFOLIO_NOT_IN_RELATIONSHIP = "PORTFOLIO_NOT_IN_RELATIONSHIP"
    DUPLICATE_LEG = "DUPLICATE_LEG"
    UNSUPPORTED_LEG_RATIO = "UNSUPPORTED_LEG_RATIO"
    SCENARIOS_NOT_MODELABLE = "SCENARIOS_NOT_MODELABLE"
    BOOK_MISSING = "BOOK_MISSING"
    BOOK_UNSYNCHRONIZED = "BOOK_UNSYNCHRONIZED"
    BOOK_TOO_OLD = "BOOK_TOO_OLD"
    CROSS_MARKET_SKEW = "CROSS_MARKET_SKEW"
    MARKET_NOT_OPEN = "MARKET_NOT_OPEN"
    NO_ASKS = "NO_ASKS"
    NO_GUARANTEED_PAYOFF = "NO_GUARANTEED_PAYOFF"
    NO_PRE_FEE_EDGE = "NO_PRE_FEE_EDGE"
    TARGET_OFF_QUANTITY_GRID = "TARGET_OFF_QUANTITY_GRID"
    DEPTH_BELOW_MINIMUM = "DEPTH_BELOW_MINIMUM"
    EDGE_EXHAUSTED_BY_DEPTH = "EDGE_EXHAUSTED_BY_DEPTH"
    FEE_UNVERIFIED = "FEE_UNVERIFIED"
    FEES_EXCEED_EDGE = "FEES_EXCEED_EDGE"
    EDGE_BELOW_MINIMUM = "EDGE_BELOW_MINIMUM"
    DURATION_UNKNOWN = "DURATION_UNKNOWN"
    DURATION_BELOW_MINIMUM = "DURATION_BELOW_MINIMUM"
    NON_DEFAULT_MAKER_ASSUMPTION = "NON_DEFAULT_MAKER_ASSUMPTION"


CHECKS: tuple[tuple[int, str, Classification], ...] = (
    (1, "relationship_valid", Classification.INVALID_RELATIONSHIP),
    (2, "books_synchronized", Classification.UNSYNCHRONIZED_DATA),
    (3, "books_fresh", Classification.STALE_DATA),
    (4, "markets_open_with_asks", Classification.INSUFFICIENT_LIQUIDITY),
    (5, "top_of_book_pre_fee_edge", Classification.NO_OPPORTUNITY),
    (6, "depth_supports_required_size", Classification.INSUFFICIENT_LIQUIDITY),
    (7, "pre_fee_edge_survives_depth", Classification.INSUFFICIENT_LIQUIDITY),
    (8, "fees_verified", Classification.FEE_UNVERIFIED),
    (9, "positive_after_fees", Classification.THEORETICAL_ONLY),
    (10, "execution_gates", Classification.DEPTH_SUPPORTED),
)


class Evaluation(FrozenModel):
    classification: Classification
    reason_codes: tuple[str, ...]
    certificate: ProofCertificate
    certificate_hash: str
    processing_latency_ns: int | None = None
    """Telemetry; deliberately excluded from the certificate and its hash."""

    def certificate_json(self) -> str:
        return canonical_json(self.certificate)

    @property
    def net_profit(self) -> Decimal | None:
        ev = self.certificate.evaluation
        return None if ev is None else ev.net_profit

    def to_detection(self, detected_at: datetime) -> Detection:
        c = self.certificate
        ev = c.evaluation
        refs = tuple(
            SnapshotRef(
                market_id=b.market_id,
                source=b.source or "unknown",
                source_sequence=b.source_sequence,
                subscription_id=b.subscription_id,
                received_ts_ms=b.received_ts_ms or 0,
                exchange_ts_ms=b.exchange_ts_ms,
                book_hash=b.book_hash or "",
            )
            for b in c.books
            if b.book_hash is not None
        )
        return Detection(
            detection_id="det-" + self.certificate_hash.removeprefix("sha256:")[:24],
            relationship_id=c.relationship.relationship_id,
            strategy_id=c.portfolio.strategy_id,
            detected_at=detected_at,
            snapshot_refs=refs,
            theoretical_deviation=None
            if c.top_of_book is None
            else c.top_of_book.pre_fee_edge_per_unit,
            gross_portfolio_edge=None if ev is None else ev.gross_profit,
            total_estimated_fees=None if ev is None else ev.total_net_fees,
            net_theoretical_edge=None if ev is None else ev.net_profit,
            max_depth_supported_quantity=None
            if c.capacity is None
            else c.capacity.max_supported_quantity,
            worst_case_payoff=None if ev is None else ev.min_payoff,
            timestamp_skew_ms=c.timing.cross_market_skew_ms,
            data_freshness_ms=c.timing.max_book_age_ms,
            classification=self.classification,
            reason_codes=self.reason_codes,
            proof_certificate=c.model_dump(mode="json"),
        )


# ------------------------------------------------------------------------------ helpers
def ms_to_datetime(ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=ms)


def book_hash(book: OrderBook) -> str:
    return sha256_of(
        {
            "market_id": book.market_id,
            "source": book.source,
            "sync_status": book.sync_status,
            "source_sequence": book.source_sequence,
            "subscription_id": book.subscription_id,
            "connection_id": book.connection_id,
            "exchange_ts_ms": book.exchange_ts_ms,
            "received_ts_ms": book.received_ts_ms,
            "confirmed_through_ms": book.confirmed_through_ms,
            "yes_bids": [[lv.price, lv.quantity] for lv in book.yes_bids],
            "no_bids": [[lv.price, lv.quantity] for lv in book.no_bids],
        }
    )


def quantity_step(markets: list[Market]) -> Decimal:
    scale = 10**QUANTITY_DP
    ints = [int(m.quantity_increment * scale) for m in markets]
    return Decimal(math.lcm(*ints)) / Decimal(scale)


def _per_unit(total: Decimal | None, q: Decimal) -> Decimal | None:
    if total is None or q == ZERO:
        return None
    return (total / q).quantize(VWAP_QUANTUM, rounding=ROUND_FLOOR)


@dataclass
class _Leg:
    market: Market
    side_ratio: Decimal
    asks: tuple[AskLevel, ...]
    curve: list[tuple[Decimal, Decimal]]
    fee: FeeResolution | None = None

    def fills(self, need: Decimal) -> tuple[Decimal, list[tuple[Decimal, Decimal]], Decimal]:
        remaining = need
        premium = ZERO
        fills: list[tuple[Decimal, Decimal]] = []
        for price, qty in self.curve:
            if remaining == ZERO:
                break
            take = min(remaining, qty)
            fills.append((price, take))
            premium += price * take
            remaining -= take
        return premium, fills, need - remaining


@dataclass
class _Point:
    q: Decimal
    gross: Decimal
    net: Decimal | None = None
    exec_adj: Decimal | None = None


class _Evaluator:
    def __init__(
        self,
        relationship: Relationship,
        portfolio: Portfolio,
        markets: Mapping[str, Market],
        books: Mapping[str, OrderBook | None],
        now_ms: int,
        fees: FeeCalculator,
        config: EvaluationConfig,
        observed_duration_ms: int | None,
    ) -> None:
        self.rel = relationship
        self.pf = portfolio
        self.markets = markets
        self.books = books
        self.now_ms = now_ms
        self.fee_calc = fees
        self.cfg = config
        self.duration = observed_duration_ms
        self.trace: list[TraceStep] = []
        self.payoff: PayoffAnalysis | None = None
        self.top: TopOfBook | None = None
        self.search: QuantitySearch | None = None
        self.report: QuantityEvaluation | None = None
        self.resolutions: tuple[FeeResolution, ...] | None = None
        self.max_age: int | None = None
        self.skew: int | None = None
        self.legs: list[_Leg] = []
        self.step = ONE
        self.min_payoff = ZERO

    # ------------------------------------------------------------------ steps
    def run(self) -> tuple[Classification, list[str]]:
        steps: list[Callable[[], list[str]]] = [
            self._s1_relationship,
            self._s2_sync,
            self._s3_fresh,
            self._s4_open,
            self._s5_top,
            self._s6_depth,
            self._s7_depth_edge,
            self._s8_fees,
            self._s9_net,
            self._s10_gates,
        ]
        for (num, name, cls), fn in zip(CHECKS, steps, strict=True):
            reasons, detail = self._call(fn)
            if reasons:
                self.trace.append(TraceStep(step=num, check=name, outcome="fail", detail=detail))
                for later_num, later_name, _ in CHECKS[num:]:
                    self.trace.append(
                        TraceStep(
                            step=later_num, check=later_name, outcome="not_reached", detail=""
                        )
                    )
                return cls, reasons
            self.trace.append(TraceStep(step=num, check=name, outcome="pass", detail=detail))
        return Classification.FEE_ADJUSTED_CANDIDATE, []

    def _call(self, fn: Callable[[], list[str]]) -> tuple[list[str], str]:
        self._detail = ""
        reasons = fn()
        return reasons, self._detail

    def _note(self, text: str) -> None:
        self._detail = text

    def _s1_relationship(self) -> list[str]:
        r: list[str] = []
        if not self.rel.is_verified:
            r.append(Reason.RELATIONSHIP_NOT_VERIFIED)
        for m in self.rel.members:
            if m not in self.markets:
                r.append(Reason.MARKET_UNKNOWN)
            elif self.markets[m].rules_hash != self.rel.rules_hashes.get(m):
                r.append(Reason.RULES_CHANGED)
        keys = [(leg.market_id, leg.side) for leg in self.pf.legs]
        if len(set(keys)) != len(keys) or not keys:
            r.append(Reason.DUPLICATE_LEG)
        for leg in self.pf.legs:
            if leg.ratio < ONE or leg.ratio != leg.ratio.to_integral_value():
                r.append(Reason.UNSUPPORTED_LEG_RATIO)
            if leg.market_id not in self.markets:
                r.append(Reason.MARKET_UNKNOWN)
        try:
            self.payoff = analyse_payoff(self.rel, self.pf)
            self.min_payoff = self.payoff.min_payoff_per_unit
        except PortfolioNotInRelationshipError:
            r.append(Reason.PORTFOLIO_NOT_IN_RELATIONSHIP)
        except ScenarioSpaceError:
            r.append(Reason.SCENARIOS_NOT_MODELABLE)
        status = self.rel.verification_status.value
        self._note(
            f"relationship {self.rel.relationship_id} status={status}"
            + (
                f", invalidated: {self.rel.invalidation_reason}"
                if self.rel.invalidation_reason
                else ""
            )
        )
        return list(dict.fromkeys(r))

    def _s2_sync(self) -> list[str]:
        r: list[str] = []
        for leg in self.pf.legs:
            b = self.books.get(leg.market_id)
            if b is None:
                r.append(Reason.BOOK_MISSING)
            elif b.sync_status is not SyncStatus.SYNCHRONIZED:
                r.append(Reason.BOOK_UNSYNCHRONIZED)
        self._note("all leg books synchronized" if not r else "untrusted leg book(s)")
        return list(dict.fromkeys(r))

    def _s3_fresh(self) -> list[str]:
        observed = [self._book(leg.market_id).observed_ts_ms for leg in self.pf.legs]
        self.max_age = self.now_ms - min(observed)
        self.skew = max(observed) - min(observed)
        r: list[str] = []
        if self.max_age > self.cfg.max_book_age_ms:
            r.append(Reason.BOOK_TOO_OLD)
        if self.skew > self.cfg.max_cross_market_skew_ms:
            r.append(Reason.CROSS_MARKET_SKEW)
        self._note(
            f"max book age {self.max_age} ms (limit {self.cfg.max_book_age_ms}), "
            f"skew {self.skew} ms (limit {self.cfg.max_cross_market_skew_ms})"
        )
        return r

    def _s4_open(self) -> list[str]:
        r: list[str] = []
        for leg in self.pf.legs:
            m = self.markets[leg.market_id]
            b = self._book(leg.market_id)
            asks = b.asks(leg.side)
            self.legs.append(
                _Leg(
                    market=m,
                    side_ratio=leg.ratio,
                    asks=asks,
                    curve=[(a.price, a.quantity) for a in asks],
                )
            )
            if m.status is not MarketStatus.OPEN:
                r.append(Reason.MARKET_NOT_OPEN)
            if not asks:
                r.append(Reason.NO_ASKS)
        self._note("every leg market OPEN with displayed asks" if not r else "leg not tradable")
        return list(dict.fromkeys(r))

    def _s5_top(self) -> list[str]:
        best = {
            leg.market_id: lg.asks[0].price for leg, lg in zip(self.pf.legs, self.legs, strict=True)
        }
        premium = sum((lg.side_ratio * lg.asks[0].price for lg in self.legs), ZERO)
        edge = self.min_payoff - premium
        self.top = TopOfBook(
            best_asks=best,
            premium_per_unit=premium,
            min_payoff_per_unit=self.min_payoff,
            pre_fee_edge_per_unit=edge,
        )
        self._note(
            f"min payoff {dec_str(self.min_payoff)} - top-of-book premium {dec_str(premium)} "
            f"= {dec_str(edge)} per unit"
        )
        r: list[str] = []
        if self.min_payoff <= ZERO:
            r.append(Reason.NO_GUARANTEED_PAYOFF)
        if edge <= ZERO:
            r.insert(0, Reason.NO_PRE_FEE_EDGE)
            return r
        return []

    def _s6_depth(self) -> list[str]:
        self.step = quantity_step([lg.market for lg in self.legs])
        caps = [
            Fraction(sum((q for _, q in lg.curve), ZERO)) / Fraction(lg.side_ratio)
            for lg in self.legs
        ]
        fstep = Fraction(self.step)
        qmax = Decimal(math.floor(min(caps) / fstep)) * self.step
        target = self.cfg.target_quantity
        if target is not None:
            lo = target
        else:
            lo = ceil_to(max(self.cfg.minimum_available_quantity, self.step), self.step)
        self.qmax, self.q_lo = qmax, lo
        self.search = QuantitySearch(
            quantity_step=self.step,
            domain_lower=lo,
            max_supported_quantity=qmax,
            method="target" if target is not None else "none",
            points_evaluated=0,
            best_gross_quantity=None,
            best_net_quantity=None,
            best_execution_quantity=None,
            max_profitable_quantity=None,
        )
        self._note(
            f"max depth-supported basket {dec_str(qmax)} (step {dec_str(self.step)}); "
            f"required {dec_str(lo)}"
        )
        if target is not None and target % self.step != ZERO:
            self.report = self._evaluate_quantity(max(target, ZERO), fees=False)
            return [Reason.TARGET_OFF_QUANTITY_GRID]
        if qmax < lo:
            self.report = self._evaluate_quantity(lo, fees=False)
            return [Reason.DEPTH_BELOW_MINIMUM]
        return []

    def _candidates(self) -> tuple[list[Decimal], str]:
        if self.cfg.target_quantity is not None:
            return [self.cfg.target_quantity], "target"
        n = int((self.qmax - self.q_lo) / self.step) + 1
        if n <= self.cfg.exhaustive_search_limit:
            return [self.q_lo + k * self.step for k in range(n)], "exhaustive"
        pts: set[Decimal] = {self.q_lo, self.qmax}
        fstep = Fraction(self.step)
        for lg in self.legs:
            cum = ZERO
            for _, qty in lg.curve:
                cum += qty
                b = Fraction(cum) / Fraction(lg.side_ratio) / fstep
                for base in (math.floor(b), math.ceil(b)):
                    for d in range(-BREAKPOINT_RADIUS, BREAKPOINT_RADIUS + 1):
                        q = Decimal(base + d) * self.step
                        if self.q_lo <= q <= self.qmax:
                            pts.add(q)
        return sorted(pts), "breakpoints"

    def _gross(self, q: Decimal) -> Decimal:
        premium = sum((lg.fills(q * lg.side_ratio)[0] for lg in self.legs), ZERO)
        return self.min_payoff * q - premium

    def _s7_depth_edge(self) -> list[str]:
        cands, method = self._candidates()
        self.points = [_Point(q=q, gross=self._gross(q)) for q in cands]
        best = max(self.points, key=lambda p: (p.gross, -p.q))
        assert self.search is not None
        self.search = self.search.model_copy(
            update={
                "method": method,
                "points_evaluated": len(self.points),
                "best_gross_quantity": best.q,
            }
        )
        self.report = self._evaluate_quantity(best.q, fees=False)
        self._note(
            f"best pre-fee worst-case profit {dec_str(best.gross)} at quantity {dec_str(best.q)} "
            f"({method}, {len(self.points)} points)"
        )
        if best.gross <= ZERO:
            return [Reason.EDGE_EXHAUSTED_BY_DEPTH]
        return []

    def _s8_fees(self) -> list[str]:
        at = ms_to_datetime(self.now_ms)
        res = []
        for lg in self.legs:
            lg.fee = self.fee_calc.resolve(
                lg.market, at, self.cfg.fee_assumptions, role=self.cfg.execution_role
            )
            res.append(lg.fee)
        self.resolutions = tuple(res)
        unverified = [f for f in res if not f.verified]
        if all(f.can_estimate for f in res):
            self._compute_net()
            assert self.search is not None
            best = self._best_net()
            self.report = self._evaluate_quantity(best.q, fees=True)
        if unverified:
            codes = sorted({f"FEE:{r.value}" for f in unverified for r in f.reasons})
            self._note(
                "unverified fee resolution for "
                + ", ".join(f.market_id for f in unverified)
                + ("" if all(f.can_estimate for f in res) else "; no estimate possible")
            )
            return [Reason.FEE_UNVERIFIED, *codes]
        labels = sorted({f.schedule_id or "" for f in res})
        self._note(f"fee schedules {labels}" + (" (FICTIONAL)" if res[0].fictional else ""))
        return []

    def _compute_net(self) -> None:
        for p in self.points:
            fees = ZERO
            fee_buffer = ZERO
            slip = ZERO
            for lg in self.legs:
                need = p.q * lg.side_ratio
                _, fills, _ = lg.fills(need)
                assert lg.fee is not None
                assert lg.fee.coefficient is not None
                assert lg.fee.multiplier is not None
                fees += order_net_fee(
                    fills,
                    coefficient=lg.fee.coefficient,
                    multiplier=lg.fee.multiplier,
                    precision=lg.fee.member_class.precision,
                )
                fee_buffer += self._fee_buffer(lg)
                slip += need * self.cfg.assumed_extra_slippage_per_leg
            p.net = p.gross - fees
            p.exec_adj = p.net - slip - fee_buffer

    def _fee_buffer(self, lg: _Leg) -> Decimal:
        if self.cfg.fee_buffer_per_leg is not None:
            return self.cfg.fee_buffer_per_leg
        assert lg.fee is not None
        return lg.fee.member_class.precision

    def _best_net(self) -> _Point:
        best = max(self.points, key=lambda p: (p.net if p.net is not None else ZERO, -p.q))
        profitable = [p.q for p in self.points if p.net is not None and p.net > ZERO]
        assert self.search is not None
        self.search = self.search.model_copy(
            update={
                "best_net_quantity": best.q,
                "max_profitable_quantity": max(profitable) if profitable else None,
            }
        )
        return best

    def _s9_net(self) -> list[str]:
        best = self._best_net()
        assert best.net is not None
        self._note(f"best worst-case profit after fees {dec_str(best.net)} at {dec_str(best.q)}")
        if best.net <= ZERO:
            return [Reason.FEES_EXCEED_EDGE]
        return []

    def _s10_gates(self) -> list[str]:
        def meets_edge(p: _Point) -> bool:
            return p.exec_adj is not None and p.exec_adj >= self.cfg.minimum_net_edge * p.q

        qualifying = [p for p in self.points if meets_edge(p)]
        best = max(
            qualifying or self.points,
            key=lambda p: (p.exec_adj if p.exec_adj is not None else ZERO, -p.q),
        )
        assert best.exec_adj is not None
        assert self.search is not None
        self.search = self.search.model_copy(
            update={"best_execution_quantity": best.q, "edge_qualifying_points": len(qualifying)}
        )
        self.report = self._evaluate_quantity(best.q, fees=True)
        r: list[str] = []
        required = self.cfg.minimum_net_edge * best.q
        if not qualifying:
            r.append(Reason.EDGE_BELOW_MINIMUM)
        if self.duration is None:
            r.append(Reason.DURATION_UNKNOWN)
        elif self.duration < self.cfg.minimum_candidate_duration_ms:
            r.append(Reason.DURATION_BELOW_MINIMUM)
        if self.cfg.execution_role is not Role.TAKER:
            r.append(Reason.NON_DEFAULT_MAKER_ASSUMPTION)
        self._note(
            f"execution-adjusted profit {dec_str(best.exec_adj)} at {dec_str(best.q)} vs required "
            f"{dec_str(required)}; duration "
            + ("unknown" if self.duration is None else f"{self.duration} ms")
            + f" (min {self.cfg.minimum_candidate_duration_ms}); role {self.cfg.execution_role}"
        )
        return r

    # ------------------------------------------------------------------ reporting
    def _book(self, market_id: str) -> OrderBook:
        b = self.books.get(market_id)
        assert b is not None
        return b

    def _evaluate_quantity(self, q: Decimal, *, fees: bool) -> QuantityEvaluation:
        legs: list[LegExecution] = []
        total_premium = ZERO
        total_fees: Decimal | None = ZERO if fees else None
        slip = ZERO
        fee_buffer: Decimal | None = ZERO if fees else None
        for leg, lg in zip(self.pf.legs, self.legs, strict=True):
            need = q * lg.side_ratio
            walk = walk_asks(leg.market_id, leg.side, lg.asks, need)
            order = None
            cost = None
            if fees and lg.fee is not None:
                order = FeeCalculator.order_fees(lg.fee, walk.fills)
                if order is not None:
                    cost = walk.total_premium + order.total_net_fee
                    assert total_fees is not None
                    assert fee_buffer is not None
                    total_fees += order.total_net_fee
                    fee_buffer += self._fee_buffer(lg)
            legs.append(
                LegExecution(
                    market_id=leg.market_id,
                    side=leg.side,
                    ratio=leg.ratio,
                    quantity=need,
                    walk=walk,
                    fees=order,
                    leg_cost=cost,
                )
            )
            total_premium += walk.total_premium
            slip += need * self.cfg.assumed_extra_slippage_per_leg
        full = all(x.walk.fully_filled for x in legs)
        min_payoff = self.min_payoff * q
        gross = min_payoff - total_premium
        net = None if total_fees is None else gross - total_fees
        exec_adj = None if net is None or fee_buffer is None else net - slip - fee_buffer
        return QuantityEvaluation(
            quantity=q,
            fully_filled=full,
            legs=tuple(legs),
            min_payoff=min_payoff,
            total_premium=total_premium,
            gross_profit=gross,
            total_net_fees=total_fees,
            total_cost=None if total_fees is None else total_premium + total_fees,
            net_profit=net,
            slippage_buffer=slip,
            fee_buffer=fee_buffer,
            execution_adjusted_profit=exec_adj,
            per_unit_gross=_per_unit(gross, q),
            per_unit_net=_per_unit(net, q),
            per_unit_execution_adjusted=_per_unit(exec_adj, q),
        )

    def book_inputs(self) -> tuple[BookInput, ...]:
        out = []
        for leg in self.pf.legs:
            b = self.books.get(leg.market_id)
            m = self.markets.get(leg.market_id)
            out.append(
                BookInput(
                    market_id=leg.market_id,
                    side_bought=leg.side,
                    market_status=None if m is None else m.status,
                    sync_status=None if b is None else b.sync_status,
                    source=None if b is None else b.source,
                    source_sequence=None if b is None else b.source_sequence,
                    subscription_id=None if b is None else b.subscription_id,
                    connection_id=None if b is None else b.connection_id,
                    exchange_ts_ms=None if b is None else b.exchange_ts_ms,
                    received_ts_ms=None if b is None else b.received_ts_ms,
                    confirmed_through_ms=None if b is None else b.confirmed_through_ms,
                    observed_ts_ms=None if b is None else b.observed_ts_ms,
                    age_ms=None if b is None else self.now_ms - b.observed_ts_ms,
                    book_hash=None if b is None else book_hash(b),
                    asks=()
                    if b is None
                    else tuple((a.price, a.quantity) for a in b.asks(leg.side)),
                )
            )
        return tuple(out)


def evaluate(
    relationship: Relationship,
    portfolio: Portfolio,
    *,
    markets: Mapping[str, Market],
    books: Mapping[str, OrderBook | None],
    now_ms: int,
    fees: FeeCalculator,
    config: EvaluationConfig | None = None,
    observed_duration_ms: int | None = None,
    processing_latency_ns: int | None = None,
) -> Evaluation:
    cfg = config or EvaluationConfig()
    ev = _Evaluator(
        relationship, portfolio, markets, books, now_ms, fees, cfg, observed_duration_ms
    )
    classification, reasons = ev.run()
    rel = relationship
    resolutions = ev.resolutions
    cert = ProofCertificate(
        relationship=RelationshipRef(
            relationship_id=rel.relationship_id,
            relationship_type=rel.relationship_type,
            members=rel.members,
            verification_status=rel.verification_status,
            fingerprint=rel.fingerprint,
            rules_hashes=rel.rules_hashes,
            exhaustive=rel.exhaustive,
            scenario_spec=rel.scenario_spec,
            constraints=rel.constraints,
            invalidation_reason=rel.invalidation_reason,
        ),
        portfolio=PortfolioRef(
            strategy_id=portfolio.strategy_id, template=portfolio.template, legs=portfolio.legs
        ),
        config=cfg,
        books=ev.book_inputs(),
        timing=TimingInfo(
            now_ms=now_ms,
            max_book_age_ms=ev.max_age,
            cross_market_skew_ms=ev.skew,
            observed_duration_ms=observed_duration_ms,
        ),
        payoff=ev.payoff,
        top_of_book=ev.top,
        capacity=ev.search,
        evaluation=ev.report,
        fees=resolutions,
        fee_schedule_ids=tuple(
            sorted({r.schedule_id for r in resolutions or () if r.schedule_id is not None})
        ),
        fictional_fees=any(r.fictional for r in resolutions or ()),
        classification=classification,
        reason_codes=tuple(str(r) for r in reasons),
        trace=tuple(ev.trace),
    )
    return Evaluation(
        classification=classification,
        reason_codes=cert.reason_codes,
        certificate=cert,
        certificate_hash=sha256_of(cert),
        processing_latency_ns=processing_latency_ns,
    )

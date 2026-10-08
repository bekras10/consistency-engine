"""Loader for the golden fixture data files in ``fixtures/golden/``."""

from __future__ import annotations

import copy
import functools
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from consistency_core.fees import FeeCalculator, FeeScheduleRegistry
from consistency_core.models import Side, SyncStatus
from consistency_core.models.market import Catalog, Event, Market
from consistency_core.models.orderbook import OrderBook, build_side
from consistency_core.models.relationship import Relationship, RelationshipType
from consistency_core.models.settlement import SettlementSpec
from consistency_core.money import dec
from consistency_core.pricing.certificate import EvaluationConfig
from consistency_core.pricing.evaluator import Evaluation, evaluate
from consistency_core.pricing.portfolio import Leg, Portfolio, Template
from consistency_core.relationships.discovery import discover
from tests.conftest import FIXTURES
from tests.factories import PROV, T0, T0_MS, market

GOLDEN = FIXTURES / "golden"
NOW_MS = T0_MS + 60_000


def load(name: str) -> dict[str, Any]:
    """Parsed fixture (a fresh deep copy each call, so callers may mutate it)."""
    return copy.deepcopy(_parsed(name))


@functools.cache
def _parsed(name: str) -> dict[str, Any]:
    path: Path = GOLDEN / f"{name}.yaml"
    with path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    assert isinstance(data, dict)
    return data


@functools.cache
def fee_calculator() -> FeeCalculator:
    return FeeCalculator(FeeScheduleRegistry.from_directory(FIXTURES / "fees"))


def catalog(fx: dict[str, Any]) -> Catalog:
    common = fx.get("common_settlement", {})
    markets: list[Market] = []
    for m in fx["markets"]:
        overrides = {k: m[k] for k in ("rounding", "methodology") if k in m}
        spec = SettlementSpec.model_validate({**common, **overrides, "terms": m["terms"]})
        markets.append(
            market(
                m["id"],
                event_id=m.get("event_id", fx["fixture"]),
                series_id=m.get("series_id", "SYN-GOLDEN"),
                quantity_increment=m.get("quantity_increment", "1"),
                settlement=spec,
                source=common.get("settlement_source", "Synthetic Bureau"),
            )
        )
    events = tuple(
        Event.model_validate({"series_id": "SYN-GOLDEN", "provenance": PROV, **e})
        for e in fx.get("events", [])
    )
    return Catalog(markets=tuple(markets), events=events)


def books(fx: dict[str, Any], now_ms: int = NOW_MS, key: str = "books") -> dict[str, OrderBook]:
    out = {}
    for mid, b in fx[key].items():
        ts = now_ms - int(b.get("age_ms", 100))
        out[mid] = OrderBook(
            market_id=mid,
            source="golden",
            source_sequence=1,
            subscription_id="1",
            connection_id="golden",
            exchange_ts_ms=ts,
            received_ts_ms=ts,
            last_sync_ts_ms=ts,
            yes_bids=build_side(Side.YES, [(dec(p), dec(q)) for p, q in b.get("yes_bids", [])]),
            no_bids=build_side(Side.NO, [(dec(p), dec(q)) for p, q in b.get("no_bids", [])]),
            sync_status=SyncStatus(b.get("sync", "synchronized")),
        )
    return out


def find_relationship(rels: list[Relationship], spec: dict[str, Any]) -> Relationship:
    rtype = RelationshipType(spec["type"])
    members = tuple(spec["members"])
    for r in rels:
        if r.relationship_type is rtype and (
            r.members == members
            if rtype in (RelationshipType.IMPLICATION, RelationshipType.NESTED_THRESHOLDS)
            else set(r.members) == set(members)
        ):
            return r
    raise AssertionError(f"relationship {spec} not discovered; got {[r.members for r in rels]}")


def portfolio(spec: dict[str, Any]) -> Portfolio:
    return Portfolio(
        template=Template(spec["template"]),
        legs=tuple(Leg(market_id=m, side=Side(s)) for m, s in spec["legs"]),
    )


def run(
    fx: dict[str, Any],
    case: dict[str, Any],
    *,
    book_map: dict[str, OrderBook] | None = None,
    now_ms: int = NOW_MS,
) -> tuple[Evaluation, Relationship]:
    cat = catalog(fx)
    rel = find_relationship(discover(cat, as_of=T0), case["relationship"])
    cfg = EvaluationConfig.model_validate(case.get("config", {}))
    ev = evaluate(
        rel,
        portfolio(case["portfolio"]),
        markets=cat.markets_by_id(),
        books=book_map
        if book_map is not None
        else books(fx, now_ms, case.get("use_books", "books")),
        now_ms=now_ms,
        fees=fee_calculator(),
        config=cfg,
        observed_duration_ms=case.get("observed_duration_ms"),
    )
    return ev, rel


def D(x: object) -> Decimal:
    return dec(x)


def check_expected(ev: Evaluation, expected: dict[str, Any]) -> None:
    """Compare every listed expectation exactly (Decimal equality, not string equality)."""
    assert ev.classification.value == expected["classification"], (
        ev.classification,
        ev.reason_codes,
        [t.detail for t in ev.certificate.trace if t.outcome == "fail"],
    )
    if "reason_codes" in expected:
        assert list(ev.reason_codes) == expected["reason_codes"]
    c = ev.certificate
    for k, v in expected.get("top_of_book", {}).items():
        assert c.top_of_book is not None
        got = getattr(c.top_of_book, k)
        assert got == D(v), (k, got, v)
    for k, v in expected.get("capacity", {}).items():
        assert c.capacity is not None
        got = getattr(c.capacity, k)
        if v is None:
            assert got is None, (k, got)
        else:
            assert got is not None, k
            assert got == D(v), (k, got, v)
    if "payoff_min_per_unit" in expected:
        assert c.payoff is not None
        assert c.payoff.min_payoff_per_unit == D(expected["payoff_min_per_unit"])
    at = expected.get("at_quantity", {})
    for k, v in at.items():
        assert c.evaluation is not None
        if k == "legs":
            for leg_exp, leg in zip(v, c.evaluation.legs, strict=True):
                for lk, lv in leg_exp.items():
                    got = _leg_value(leg, lk)
                    assert got == D(lv), (leg.market_id, lk, got, lv)
            continue
        got = getattr(c.evaluation, k)
        if v is None:
            assert got is None, (k, got)
        elif isinstance(v, bool):
            assert got is v, (k, got)
        else:
            assert got == D(v), (k, got, v)


def _leg_value(leg: Any, key: str) -> Decimal:
    if key in ("filled_quantity", "unfilled_quantity", "total_premium", "available_quantity"):
        return getattr(leg.walk, key)  # type: ignore[no-any-return]
    if key in ("trade_fee", "rounding_fee", "rebate", "net_fee"):
        assert leg.fees is not None
        return getattr(leg.fees, f"total_{key}")  # type: ignore[no-any-return]
    return getattr(leg, key)  # type: ignore[no-any-return]


__all__ = ["NOW_MS", "T0", "catalog", "check_expected", "load", "run"]

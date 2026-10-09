"""Read models for the Phase 10 dashboard.

Values come from PostgreSQL. Money leaves this module as fixed-point strings.
There is no public ``/api/v1`` catalog here.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from consistency_connectors.ingestion import BookManager
from consistency_core.models.common import Side
from consistency_core.money import dec_str
from consistency_persistence.schema import (
    DataSourceRow,
    DetectionLegRow,
    DetectionRow,
    DetectionScenarioRow,
    IngestionSessionRow,
    MarketRow,
    RelationshipMemberRow,
    RelationshipRow,
    SeriesRow,
    SystemHealthRow,
)
from consistency_pipeline.journal import JournalEntry

_SORTS = frozenset(
    {
        "time",
        "relationship",
        "type",
        "theoretical_deviation",
        "gross_edge",
        "fees",
        "net_edge",
        "quantity",
        "duration",
        "classification",
    }
)


def money(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return dec_str(value)


def json_ready(value: object) -> object:
    """Convert a persisted value to JSON without turning money into floats."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        raise TypeError("dashboard JSON refuses float values")
    if isinstance(value, Decimal):
        return dec_str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(item) for item in value]
    raise TypeError(f"dashboard JSON cannot encode {type(value).__name__}")


def _dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items()}
    return {}


def _str_field(doc: object, *path: str) -> str | None:
    cur: object = doc
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    if isinstance(cur, str):
        return cur
    if isinstance(cur, Decimal):
        return dec_str(cur)
    return None


def latency_ns_from_record(record: object) -> int | None:
    timing = _dict(record).get("timing")
    body = _dict(timing)
    started = body.get("processing_started_ns")
    completed = body.get("detection_completed_ns")
    if type(started) is int and type(completed) is int:
        return completed - started
    return None


def percentile_nearest(values: list[int], percent: int) -> int | None:
    """Nearest-rank percentile. ``percent`` is 50 or 95. Empty input is None."""
    if not values:
        return None
    if percent <= 0 or percent > 100:
        raise ValueError("percent must be in 1..100")
    ordered = sorted(values)
    rank = (percent * len(ordered) + 99) // 100
    index = min(len(ordered), max(1, rank)) - 1
    return ordered[index]


def current_figures(record: object, row_max_net: Decimal | None = None) -> dict[str, str | None]:
    """Current quote from ``metrics``. Maxima stay in their own fields."""
    body = _dict(record)
    metrics = _dict(body.get("metrics"))
    stored_max = _str_field(body, "max_net_edge")
    return {
        "theoretical_deviation": _str_field(metrics, "theoretical_deviation"),
        "gross_edge": _str_field(metrics, "gross_edge"),
        "fees": _str_field(metrics, "total_fees"),
        "net_edge": _str_field(metrics, "net_edge"),
        "depth_supported_quantity": _str_field(metrics, "depth_supported_quantity"),
        "worst_case_payoff": _str_field(metrics, "worst_case_payoff"),
        "max_net_edge": money(row_max_net) if row_max_net is not None else stored_max,
        "max_deviation": _str_field(body, "max_deviation"),
        "max_capacity": _str_field(body, "max_capacity"),
    }


def at_least(value: str | None, minimum: Decimal | None) -> bool:
    if minimum is None:
        return True
    if value is None:
        return False
    return Decimal(value) >= minimum


def _decimal_key(value: str | None) -> Decimal:
    if value is None:
        return Decimal("-1e100")
    return Decimal(value)


@dataclass(frozen=True)
class DetectionQuery:
    classification: str | None = None
    relationship_type: str | None = None
    since_ms: int | None = None
    until_ms: int | None = None
    min_net_edge: Decimal | None = None
    min_quantity: Decimal | None = None
    market_id: str | None = None
    sort: str = "time"
    direction: str = "desc"


def _detection_item(
    row: DetectionRow,
    relationship_type: str | None,
    figures: dict[str, str | None],
) -> dict[str, object]:
    return {
        "detection_id": row.detection_id,
        "session_id": row.session_id,
        "relationship_id": row.relationship_id,
        "relationship_type": relationship_type,
        "template": row.template,
        "time_ms": row.last_observed_ms,
        "first_observed_ms": row.first_observed_ms,
        "last_observed_ms": row.last_observed_ms,
        "status": row.status,
        "classification": row.classification,
        "duration_ms": row.last_observed_ms - row.first_observed_ms,
        "theoretical_deviation": figures["theoretical_deviation"],
        "gross_edge": figures["gross_edge"],
        "fees": figures["fees"],
        "net_edge": figures["net_edge"],
        "max_net_edge": figures["max_net_edge"],
        "max_deviation": figures["max_deviation"],
        "depth_supported_quantity": figures["depth_supported_quantity"],
        "max_capacity": figures["max_capacity"],
    }


def _sort_items(items: list[dict[str, object]], query: DetectionQuery) -> list[dict[str, object]]:
    key_name = query.sort if query.sort in _SORTS else "time"
    money_keys = {
        "theoretical_deviation",
        "gross_edge",
        "fees",
        "net_edge",
        "quantity",
    }

    def sort_key(item: dict[str, object]) -> tuple[object, ...]:
        if key_name == "quantity":
            raw = item.get("depth_supported_quantity")
            primary: object = _decimal_key(raw if isinstance(raw, str) else None)
        elif key_name in money_keys:
            raw = item.get(key_name)
            primary = _decimal_key(raw if isinstance(raw, str) else None)
        elif key_name == "time":
            primary = item.get("time_ms")
        elif key_name == "duration":
            primary = item.get("duration_ms")
        elif key_name == "relationship":
            primary = item.get("relationship_id")
        elif key_name == "type":
            primary = item.get("relationship_type") or ""
        else:
            primary = item.get("classification")
        return (primary, item.get("detection_id"))

    return sorted(items, key=sort_key, reverse=query.direction != "asc")


def activity_buckets(timestamps_ms: list[int]) -> list[dict[str, object]]:
    if not timestamps_ms:
        return []
    span = max(timestamps_ms) - min(timestamps_ms)
    if span > 2 * 60 * 60 * 1000:
        width = 60 * 60 * 1000
    elif span > 2 * 60 * 1000:
        width = 60 * 1000
    else:
        width = 1000
    counts: Counter[int] = Counter()
    for stamp in timestamps_ms:
        counts[(stamp // width) * width] += 1
    return [{"start_ms": start, "count": counts[start]} for start in sorted(counts)]


def explain_detection(item: dict[str, object]) -> str:
    net = item.get("net_edge") or "unavailable"
    maximum = item.get("max_net_edge") or "unavailable"
    classification = item.get("classification")
    relationship = item.get("relationship_id")
    rel_type = item.get("relationship_type") or "unknown type"
    status = item.get("status")
    return (
        f"Relationship {relationship} ({rel_type}) is recorded as {classification} "
        f"with status {status}. The current net theoretical edge is {net}. "
        f"The historical maximum net theoretical edge on this detection is {maximum}. "
        "Those are different fields: a later smaller quote does not replace the maximum, "
        "and the maximum is not shown as the current edge. "
        "A fee-adjusted candidate is a worst-case theoretical result on separately quoted "
        "books. It is not a realized trade. Execution across markets is not atomic, and "
        "synthetic output is not evidence about a live venue."
    )


def technical_report(item: dict[str, object], certificate_json: str | None) -> str:
    lines = [
        f"detection_id: {item.get('detection_id')}",
        f"session_id: {item.get('session_id')}",
        f"relationship_id: {item.get('relationship_id')}",
        f"relationship_type: {item.get('relationship_type')}",
        f"classification: {item.get('classification')}",
        f"status: {item.get('status')}",
        f"current_net_theoretical_edge: {item.get('net_edge')}",
        f"maximum_net_theoretical_edge: {item.get('max_net_edge')}",
        f"current_theoretical_deviation: {item.get('theoretical_deviation')}",
        f"maximum_deviation: {item.get('max_deviation')}",
        f"gross_edge: {item.get('gross_edge')}",
        f"fees: {item.get('fees')}",
        f"depth_supported_quantity: {item.get('depth_supported_quantity')}",
        f"maximum_capacity: {item.get('max_capacity')}",
        f"certificate_hash: {item.get('certificate_hash')}",
        "",
        "certificate_json:",
        certificate_json if certificate_json else "(no certificate stored on this row)",
    ]
    return "\n".join(str(line) for line in lines)


def apply_book(manager: BookManager, entry: JournalEntry) -> None:
    """Apply one journal entry to books only, matching ``JournalApplier``'s book transitions."""
    if entry.message is not None:
        manager.process(entry.message)
        manager.drain_recovery_requests()
        return
    control = entry.control
    assert control is not None
    if control.action == "connection_lost":
        assert control.reason is not None
        for conn in control.connection_ids:
            manager.connection_lost(conn, control.reason)
    elif control.action == "recovery_failed":
        assert control.reason is not None and control.recovery_request is not None
        manager.recovery_failed(control.recovery_request, control.reason)


def _levels(levels: object) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if not isinstance(levels, tuple):
        return out
    for level in levels:
        price = getattr(level, "price", None)
        quantity = getattr(level, "quantity", None)
        if isinstance(price, Decimal) and isinstance(quantity, Decimal):
            out.append({"price": dec_str(price), "quantity": dec_str(quantity)})
    return out


def book_view(manager: BookManager, market_id: str) -> dict[str, object]:
    book = manager.book(market_id)

    def best(side: Side, ask: bool) -> str | None:
        price = book.best_ask(side) if ask else book.best_bid(side)
        return None if price is None else dec_str(price)

    yes_bid = best(Side.YES, False)
    yes_ask = best(Side.YES, True)
    spread = None
    if yes_bid is not None and yes_ask is not None:
        spread = dec_str(Decimal(yes_ask) - Decimal(yes_bid))
    return {
        "market_id": market_id,
        "sync_status": book.sync_status.value,
        "yes_bids": _levels(book.yes_bids),
        "no_bids": _levels(book.no_bids),
        "yes_asks": _levels(book.yes_asks),
        "no_asks": _levels(book.no_asks),
        "best_yes_bid": yes_bid,
        "best_yes_ask": yes_ask,
        "best_no_bid": best(Side.NO, False),
        "best_no_ask": best(Side.NO, True),
        "spread": spread,
        "yes_depth": dec_str(book.displayed_quantity(Side.YES)),
        "no_depth": dec_str(book.displayed_quantity(Side.NO)),
        "received_ts_ms": book.received_ts_ms,
        "source_sequence": book.source_sequence,
        "depth_note": (
            "Bid levels are persisted journal state. Ask prices are 1 minus the opposing bid, "
            "the same derivation the engine uses. They are not a separate stored ask feed."
        ),
    }


def sync_summary(manager: BookManager) -> dict[str, object]:
    counts: Counter[str] = Counter()
    for market_id in manager.market_ids:
        counts[manager.sync_status(market_id).value] += 1
    total = sum(counts.values())
    synchronized = counts.get("synchronized", 0)
    return {
        "total": total,
        "synchronized": synchronized,
        "unsynchronized": counts.get("unsynchronized", 0),
        "awaiting_snapshot": counts.get("awaiting_snapshot", 0),
        "healthy": total > 0 and synchronized == total,
    }


async def _relationship_types(session: AsyncSession) -> dict[str, str]:
    rows = (
        await session.execute(
            select(RelationshipRow.relationship_id, RelationshipRow.relationship_type)
        )
    ).all()
    return {str(rel_id): str(rel_type) for rel_id, rel_type in rows}


async def _latest_session(session: AsyncSession) -> IngestionSessionRow | None:
    return (
        await session.execute(
            select(IngestionSessionRow).order_by(IngestionSessionRow.started_at.desc()).limit(1)
        )
    ).scalar_one_or_none()


async def read_overview(session: AsyncSession, sync: dict[str, object] | None) -> dict[str, object]:
    latest = await _latest_session(session)
    source_kind = None
    source_name = None
    if latest is not None:
        source = await session.get(DataSourceRow, latest.data_source_id)
        if source is not None:
            source_kind = source.kind
            source_name = source.name
    health = (
        await session.execute(
            select(SystemHealthRow).order_by(SystemHealthRow.checked_at.desc()).limit(1)
        )
    ).scalar_one_or_none()
    market_count = len((await session.execute(select(MarketRow.market_id))).scalars().all())
    rel_rows = (await session.execute(select(RelationshipRow.verification_status))).scalars().all()
    verified = sum(1 for status in rel_rows if status == "verified")
    det_rows = (
        await session.execute(
            select(
                DetectionRow.detection_id,
                DetectionRow.status,
                DetectionRow.first_observed_ms,
                DetectionRow.record_json,
            )
        )
    ).all()
    open_count = sum(1 for _id, status, _ms, _rec in det_rows if status in ("OPEN", "UPDATED"))
    latencies = [
        value
        for value in (latency_ns_from_record(rec) for _i, _s, _m, rec in det_rows)
        if value is not None
    ]
    types = await _relationship_types(session)
    recent_rows = (
        (
            await session.execute(
                select(DetectionRow).order_by(DetectionRow.last_observed_ms.desc()).limit(8)
            )
        )
        .scalars()
        .all()
    )
    recent = [
        _detection_item(
            row,
            types.get(row.relationship_id),
            current_figures(row.record_json, row.max_net_edge),
        )
        for row in recent_rows
    ]
    synthetic = source_kind == "synthetic" or (
        latest is not None and latest.source_label.startswith("synthetic")
    )
    return {
        "data_source": {
            "kind": source_kind,
            "name": source_name,
            "label": None if latest is None else latest.source_label,
            "session_id": None if latest is None else latest.session_id,
            "session_status": None if latest is None else latest.status,
            "deterministic": None if latest is None else latest.deterministic,
            "synthetic": synthetic,
        },
        "source_health": None
        if health is None
        else {
            "component": health.component,
            "status": health.status,
            "checked_at": health.checked_at.isoformat(),
            "detail": json_ready(health.detail),
        },
        "markets_monitored": market_count,
        "relationships_total": len(rel_rows),
        "relationships_verified": verified,
        "detections_total": len(det_rows),
        "detections_open": open_count,
        "sync_health": sync,
        "internal_latency_ns": {
            "p50": percentile_nearest(latencies, 50),
            "p95": percentile_nearest(latencies, 95),
            "samples": len(latencies),
            "stored": len(latencies) > 0,
        },
        "recent_detections": recent,
        "activity": activity_buckets([int(stamp) for _i, _s, stamp, _r in det_rows]),
        "empty": market_count == 0 and len(det_rows) == 0,
        "seed_hint": (
            "PostgreSQL has no dashboard rows yet. From consistency-engine run "
            "`make seed` and `make dev` (Postgres, migrations, synthetic worker, dashboard). "
            "A fast synthetic session is loaded automatically when `make dev` finds no detections."
        ),
    }


async def _member_categories(session: AsyncSession) -> dict[str, list[str]]:
    rows = (
        await session.execute(
            select(RelationshipMemberRow.relationship_id, SeriesRow.category)
            .join(MarketRow, MarketRow.market_id == RelationshipMemberRow.market_id)
            .join(SeriesRow, SeriesRow.series_id == MarketRow.series_id)
        )
    ).all()
    grouped: dict[str, list[str]] = {}
    for rel_id, category in rows:
        grouped.setdefault(str(rel_id), [])
        if str(category) not in grouped[str(rel_id)]:
            grouped[str(rel_id)].append(str(category))
    return grouped


async def list_relationships(
    session: AsyncSession,
    *,
    relationship_type: str | None = None,
    status: str | None = None,
    category: str | None = None,
    market_id: str | None = None,
    min_members: int | None = None,
    max_members: int | None = None,
) -> dict[str, object]:
    rels = (
        (await session.execute(select(RelationshipRow).order_by(RelationshipRow.relationship_id)))
        .scalars()
        .all()
    )
    members = (
        (
            await session.execute(
                select(RelationshipMemberRow).order_by(RelationshipMemberRow.position)
            )
        )
        .scalars()
        .all()
    )
    by_rel: dict[str, list[str]] = {}
    for member in members:
        by_rel.setdefault(member.relationship_id, []).append(member.market_id)
    categories = await _member_categories(session)
    open_rows = (
        await session.execute(
            select(
                DetectionRow.relationship_id,
                DetectionRow.classification,
                DetectionRow.status,
            ).where(DetectionRow.status.in_(("OPEN", "UPDATED")))
        )
    ).all()
    open_by_rel = {
        str(rel_id): (str(classification), str(det_status))
        for rel_id, classification, det_status in open_rows
    }
    proof_rows = (
        await session.execute(
            select(
                DetectionRow.relationship_id,
                DetectionRow.detection_id,
                DetectionRow.last_observed_ms,
            ).order_by(DetectionRow.last_observed_ms.desc())
        )
    ).all()
    proof_by_rel: dict[str, str] = {}
    for rel_id, detection_id, _observed_ms in proof_rows:
        proof_by_rel.setdefault(str(rel_id), str(detection_id))
    items: list[dict[str, object]] = []
    for rel in rels:
        constituent = by_rel.get(rel.relationship_id, [])
        cats = categories.get(rel.relationship_id, [])
        if relationship_type and rel.relationship_type != relationship_type:
            continue
        if status and rel.verification_status != status:
            continue
        if category and category not in cats:
            continue
        if market_id and market_id not in constituent:
            continue
        count = len(constituent)
        if min_members is not None and count < min_members:
            continue
        if max_members is not None and count > max_members:
            continue
        doc = _dict(rel.document)
        evaluation = open_by_rel.get(rel.relationship_id)
        items.append(
            {
                "relationship_id": rel.relationship_id,
                "relationship_type": rel.relationship_type,
                "verification_status": rel.verification_status,
                "categories": cats,
                "members": constituent,
                "member_count": count,
                "constraints": doc.get("constraints")
                if isinstance(doc.get("constraints"), list)
                else [],
                "updated_at": rel.updated_at.isoformat(),
                "current_evaluation": None
                if evaluation is None
                else {"classification": evaluation[0], "status": evaluation[1]},
                "proof_detection_id": proof_by_rel.get(rel.relationship_id),
            }
        )
    return {
        "relationships": items,
        "types": sorted({rel.relationship_type for rel in rels}),
        "statuses": sorted({rel.verification_status for rel in rels}),
        "categories": sorted({cat for cats in categories.values() for cat in cats}),
    }


async def get_relationship(session: AsyncSession, relationship_id: str) -> dict[str, object] | None:
    rel = await session.get(RelationshipRow, relationship_id)
    if rel is None:
        return None
    members = (
        (
            await session.execute(
                select(RelationshipMemberRow)
                .where(RelationshipMemberRow.relationship_id == relationship_id)
                .order_by(RelationshipMemberRow.position)
            )
        )
        .scalars()
        .all()
    )
    market_ids = [member.market_id for member in members]
    markets = []
    if market_ids:
        rows = (
            (await session.execute(select(MarketRow).where(MarketRow.market_id.in_(market_ids))))
            .scalars()
            .all()
        )
        by_id = {row.market_id: row for row in rows}
        for market_id in market_ids:
            row = by_id.get(market_id)
            if row is None:
                markets.append({"market_id": market_id, "title": None, "rules": None})
                continue
            markets.append(
                {
                    "market_id": row.market_id,
                    "ticker": row.ticker,
                    "title": row.title,
                    "status": row.status,
                    "rules_hash": row.rules_hash,
                    "settlement_rules": _str_field(row.document, "settlement_rules"),
                }
            )
    proof = (
        await session.execute(
            select(DetectionRow.detection_id)
            .where(DetectionRow.relationship_id == relationship_id)
            .order_by(DetectionRow.last_observed_ms.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    categories = await _member_categories(session)
    return {
        "relationship_id": rel.relationship_id,
        "relationship_type": rel.relationship_type,
        "verification_status": rel.verification_status,
        "exhaustive": rel.exhaustive,
        "categories": categories.get(relationship_id, []),
        "updated_at": rel.updated_at.isoformat(),
        "created_at": rel.created_at.isoformat(),
        "members": markets,
        "document": json_ready(rel.document),
        "proof_detection_id": proof,
    }


async def list_detections(session: AsyncSession, query: DetectionQuery) -> dict[str, object]:
    stmt = select(DetectionRow)
    if query.classification:
        stmt = stmt.where(DetectionRow.classification == query.classification)
    if query.since_ms is not None:
        stmt = stmt.where(DetectionRow.last_observed_ms >= query.since_ms)
    if query.until_ms is not None:
        stmt = stmt.where(DetectionRow.last_observed_ms <= query.until_ms)
    if query.market_id:
        stmt = stmt.where(
            DetectionRow.detection_id.in_(
                select(DetectionLegRow.detection_id).where(
                    DetectionLegRow.market_id == query.market_id
                )
            )
        )
    rows = (await session.execute(stmt)).scalars().all()
    types = await _relationship_types(session)
    items: list[dict[str, object]] = []
    for row in rows:
        rel_type = types.get(row.relationship_id)
        if query.relationship_type and rel_type != query.relationship_type:
            continue
        figures = current_figures(row.record_json, row.max_net_edge)
        if not at_least(figures["net_edge"], query.min_net_edge):
            continue
        if not at_least(figures["depth_supported_quantity"], query.min_quantity):
            continue
        items.append(_detection_item(row, rel_type, figures))
    ordered = _sort_items(items, query)
    truncated = len(ordered) > 500
    return {
        "detections": ordered[:500],
        "total": len(ordered),
        "truncated": truncated,
        "classifications": sorted({row.classification for row in rows}),
        "relationship_types": sorted(set(types.values())),
    }


async def get_detection(session: AsyncSession, detection_id: str) -> dict[str, object] | None:
    row = await session.get(DetectionRow, detection_id)
    if row is None:
        return None
    types = await _relationship_types(session)
    figures = current_figures(row.record_json, row.max_net_edge)
    item = _detection_item(row, types.get(row.relationship_id), figures)
    item["certificate_hash"] = row.certificate_hash
    item["certificate_version"] = row.certificate_version
    item["close_reason"] = row.close_reason
    item["close_detail"] = row.close_detail
    item["reason_codes"] = list(row.reason_codes)
    item["first_position"] = row.first_position
    item["last_position"] = row.last_position
    item["event_count"] = row.event_count
    item["strategy_id"] = row.strategy_id
    legs = (
        (
            await session.execute(
                select(DetectionLegRow)
                .where(DetectionLegRow.detection_id == detection_id)
                .order_by(DetectionLegRow.leg_index)
            )
        )
        .scalars()
        .all()
    )
    scenarios = (
        (
            await session.execute(
                select(DetectionScenarioRow)
                .where(DetectionScenarioRow.detection_id == detection_id)
                .order_by(DetectionScenarioRow.scenario_index)
            )
        )
        .scalars()
        .all()
    )
    certificate: object = None
    if row.certificate_json:
        certificate = json_ready(json.loads(row.certificate_json))
    market_ids = [leg.market_id for leg in legs]
    rules: list[dict[str, object]] = []
    if market_ids:
        markets = (
            (await session.execute(select(MarketRow).where(MarketRow.market_id.in_(market_ids))))
            .scalars()
            .all()
        )
        by_id = {market.market_id: market for market in markets}
        for market_id in market_ids:
            market = by_id.get(market_id)
            if market is None:
                continue
            rules.append(
                {
                    "market_id": market.market_id,
                    "title": market.title,
                    "ticker": market.ticker,
                    "status": market.status,
                    "rules_hash": market.rules_hash,
                    "settlement_rules": _str_field(market.document, "settlement_rules"),
                    "settlement_source": _str_field(market.document, "settlement_source"),
                }
            )
    payload: dict[str, object] = {
        **item,
        "explanation": explain_detection(item),
        "legs": [
            {
                "leg_index": leg.leg_index,
                "market_id": leg.market_id,
                "side": leg.side,
                "ratio": money(leg.ratio),
                "quantity": money(leg.quantity),
                "premium": money(leg.premium),
                "leg_cost": money(leg.leg_cost),
            }
            for leg in legs
        ],
        "scenarios": [
            {
                "scenario_index": scenario.scenario_index,
                "state_json": scenario.state_json,
                "payoff_per_unit": money(scenario.payoff_per_unit),
                "is_worst": scenario.is_worst,
            }
            for scenario in scenarios
        ],
        "markets": rules,
        "certificate": certificate,
        "technical_report": technical_report(item, row.certificate_json),
        "record": json_ready(row.record_json),
    }
    return payload


async def list_markets(session: AsyncSession) -> dict[str, object]:
    rows = (
        await session.execute(
            select(MarketRow, SeriesRow.category, DataSourceRow.kind)
            .join(SeriesRow, SeriesRow.series_id == MarketRow.series_id)
            .join(DataSourceRow, DataSourceRow.data_source_id == SeriesRow.data_source_id)
            .order_by(MarketRow.market_id)
        )
    ).all()
    markets = [
        {
            "market_id": market.market_id,
            "ticker": market.ticker,
            "title": market.title,
            "status": market.status,
            "category": category,
            "data_source": kind,
            "event_id": market.event_id,
        }
        for market, category, kind in rows
    ]
    return {"markets": markets}


async def get_market_row(session: AsyncSession, market_id: str) -> dict[str, object] | None:
    row = (
        await session.execute(
            select(MarketRow, SeriesRow.category, DataSourceRow.kind)
            .join(SeriesRow, SeriesRow.series_id == MarketRow.series_id)
            .join(DataSourceRow, DataSourceRow.data_source_id == SeriesRow.data_source_id)
            .where(MarketRow.market_id == market_id)
        )
    ).one_or_none()
    if row is None:
        return None
    market, category, kind = row
    return {
        "market_id": market.market_id,
        "ticker": market.ticker,
        "title": market.title,
        "status": market.status,
        "category": category,
        "data_source": kind,
        "event_id": market.event_id,
        "rules_hash": market.rules_hash,
        "settlement_rules": _str_field(market.document, "settlement_rules"),
        "settlement_source": _str_field(market.document, "settlement_source"),
        "provenance": _str_field(market.document, "provenance", "source_kind")
        or _str_field(market.document, "provenance"),
    }

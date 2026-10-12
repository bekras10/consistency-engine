"""Map documented Kalshi JSON onto the existing catalog models.

Relationships are not inferred here. Settlement facts stay unknown unless the
payload carries an explicit structured field. Title text is display-only.
A ``consistency_settlement`` object is accepted only when the caller placed it
on the payload (fixtures or an operator merge). Live responses are not given one.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from consistency_connectors.kalshi.families import select_complete_families
from consistency_connectors.kalshi.rest import KalshiRestClient
from consistency_connectors.settings import Settings
from consistency_core.models.common import DataSourceKind, MarketStatus, Provenance
from consistency_core.models.market import Catalog, Event, Market, Series
from consistency_core.models.settlement import (
    Comparator,
    RoundingConvention,
    RoundingMode,
    SettlementSpec,
    ThresholdTerms,
)
from consistency_core.money import ONE, Dec, dec
from consistency_core.ticks import parse_price_grid

_STATUS = {
    "initialized": MarketStatus.INITIALIZED,
    "inactive": MarketStatus.INITIALIZED,
    "unopened": MarketStatus.INITIALIZED,
    "active": MarketStatus.OPEN,
    "open": MarketStatus.OPEN,
    "paused": MarketStatus.PAUSED,
    "closed": MarketStatus.CLOSED,
    "determined": MarketStatus.CLOSED,
    "disputed": MarketStatus.CLOSED,
    "amended": MarketStatus.CLOSED,
    "finalized": MarketStatus.SETTLED,
    "settled": MarketStatus.SETTLED,
}
_COMPARATOR = {
    "greater": Comparator.GT,
    "greater_or_equal": Comparator.GE,
    "less": Comparator.LT,
    "less_or_equal": Comparator.LE,
}


class CatalogLoadError(RuntimeError):
    pass


def _str_field(raw: Mapping[str, Any], key: str) -> str:
    value = raw.get(key)
    return value if isinstance(value, str) else ""


def _string_values(rows: Sequence[Mapping[str, object]], key: str) -> set[str]:
    found: set[str] = set()
    for item in rows:
        value = item.get(key)
        if isinstance(value, str):
            found.add(value)
    return found


def _text(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _decimal_token(value: object) -> Dec | None:
    """Exact decimal from a string or int. Floats are refused."""
    if isinstance(value, bool | float) or value is None:
        return None
    try:
        return dec(value)
    except (ValueError, TypeError, ArithmeticError):
        return None


def _one_dollar(value: object) -> bool:
    token = _decimal_token(value)
    return token is not None and token == ONE


def _source_label(raw: object) -> str | None:
    if not isinstance(raw, list) or not raw:
        return None
    names: list[str] = []
    for item in raw:
        if not isinstance(item, Mapping):
            return None
        name = _text(item.get("name"))
        if name is None:
            return None
        names.append(name)
    return " | ".join(sorted(names))


def _overlay(raw: object) -> dict[str, object]:
    if isinstance(raw, Mapping):
        return dict(raw)
    return {}


def _rounding(raw: object) -> RoundingConvention | None:
    if not isinstance(raw, Mapping):
        return None
    mode = raw.get("mode")
    if not isinstance(mode, str):
        return None
    try:
        parsed = RoundingMode(mode)
    except ValueError:
        return None
    increment = None if raw.get("increment") is None else _decimal_token(raw.get("increment"))
    try:
        return RoundingConvention(mode=parsed, increment=increment)
    except ValueError:
        return None


def _terms(market: Mapping[str, Any]) -> ThresholdTerms | None:
    kind = market.get("strike_type")
    comparator = _COMPARATOR.get(kind) if isinstance(kind, str) else None
    if comparator is None:
        return None
    value = _decimal_token(market.get("floor_strike"))
    if comparator in (Comparator.LT, Comparator.LE):
        value = _decimal_token(market.get("cap_strike"))
        if value is None:
            value = _decimal_token(market.get("floor_strike"))
    if value is None:
        return None
    return ThresholdTerms(comparator=comparator, value=value)


def _early_close(market: Mapping[str, Any]) -> str | None:
    flag = market.get("can_close_early")
    condition = _text(market.get("early_close_condition"))
    if flag is False:
        return "no_early_close"
    if flag is True and condition is not None:
        return condition
    return None


def _spec(market: Mapping[str, Any], event: Mapping[str, Any]) -> SettlementSpec:
    overlay = _overlay(market.get("consistency_settlement"))
    exceptional: tuple[str, ...] | None = None
    raw_exceptional = overlay.get("exceptional_resolution")
    if isinstance(raw_exceptional, list) and all(isinstance(item, str) for item in raw_exceptional):
        exceptional = tuple(raw_exceptional)
    overlay_source = _text(overlay.get("settlement_source"))
    event_source = _source_label(event.get("settlement_sources"))
    source = overlay_source or event_source
    return SettlementSpec(
        underlying_id=_text(overlay.get("underlying_id")),
        measurement=_text(overlay.get("measurement")),
        location=_text(overlay.get("location")),
        observation_window=_text(overlay.get("observation_window")),
        settlement_source=source,
        methodology=_text(overlay.get("methodology")),
        unit=_text(overlay.get("unit")),
        rounding=_rounding(overlay.get("rounding")),
        early_close_policy=_text(overlay.get("early_close_policy")) or _early_close(market),
        exceptional_resolution=exceptional,
        terms=_terms(market),
    )


def _metadata(market: Mapping[str, Any], *, clamped: bool) -> dict[str, Any]:
    override = market.get("fee_type_override")
    multiplier = market.get("fee_multiplier_override")
    fee_override = (override is not None and override != "") or (
        multiplier is not None and multiplier != ""
    )
    out: dict[str, Any] = {
        "exchange_status": _str_field(market, "status"),
        "strike_type": _str_field(market, "strike_type"),
        "price_level_structure": _str_field(market, "price_level_structure"),
        "updated_time": _str_field(market, "updated_time"),
        "can_close_early": market.get("can_close_early") is True,
        "market_type": _str_field(market, "market_type"),
        "notional_value_dollars": _str_field(market, "notional_value_dollars"),
        "expiration_clamped": clamped,
        "fee_override_present": fee_override,
        "mve": bool(_text(market.get("mve_collection_ticker"))),
    }
    return out


def map_market(
    raw: Mapping[str, Any],
    event: Mapping[str, Any],
    series: Mapping[str, Any],
    *,
    retrieved_at: datetime,
    environment: str,
) -> Market | None:
    if raw.get("market_type") not in (None, "binary"):
        return None
    if raw.get("settlement_bounds_type") not in (None, "default"):
        return None
    if not _one_dollar(raw.get("notional_value_dollars")):
        return None
    ticker = _text(raw.get("ticker"))
    if ticker is None:
        return None
    raw_status = raw.get("status")
    status = _STATUS.get(raw_status) if isinstance(raw_status, str) else None
    if status is None:
        return None
    open_time = _time(raw.get("open_time"))
    close_time = _time(raw.get("close_time"))
    expiration = _time(raw.get("latest_expiration_time")) or _time(raw.get("expiration_time"))
    if open_time is None or close_time is None or expiration is None:
        return None
    if open_time > close_time:
        return None
    clamped = False
    if expiration < close_time:
        expiration = close_time
        clamped = True
    try:
        grid = parse_price_grid({"price_ranges": raw.get("price_ranges")})
    except (ValueError, TypeError, KeyError):
        return None
    event_id = _text(event.get("event_ticker")) or _text(raw.get("event_ticker"))
    series_id = _text(series.get("ticker")) or _text(event.get("series_ticker"))
    if event_id is None or series_id is None:
        return None
    rules_primary = _str_field(raw, "rules_primary")
    rules_secondary = _str_field(raw, "rules_secondary")
    rules = "\n".join(part for part in (rules_primary, rules_secondary) if part)
    spec = _spec(raw, event)
    source_name = spec.settlement_source or "unspecified"
    title = _text(raw.get("yes_sub_title")) or _text(raw.get("title")) or ticker
    updated = _text(raw.get("updated_time")) or "unversioned"
    return Market(
        market_id=ticker,
        ticker=ticker,
        event_id=event_id,
        series_id=series_id,
        title=title,
        description=_text(raw.get("no_sub_title")) or "",
        status=status,
        open_time=open_time,
        close_time=close_time,
        expiration_time=expiration,
        settlement_source=source_name,
        settlement_rules=rules,
        rules_version=updated,
        price_grid=grid,
        quantity_increment=dec("0.01"),
        settlement=spec,
        exchange_metadata=_metadata(raw, clamped=clamped),
        provenance=Provenance(
            source_kind=DataSourceKind.KALSHI_AUTHORIZED,
            source_id=f"kalshi:{environment}",
            retrieved_at=retrieved_at,
        ),
    )


def map_event(raw: Mapping[str, Any], *, retrieved_at: datetime, environment: str) -> Event | None:
    event_id = _text(raw.get("event_ticker"))
    series_id = _text(raw.get("series_ticker"))
    title = _text(raw.get("title"))
    if event_id is None or series_id is None or title is None:
        return None
    exclusive = raw.get("mutually_exclusive")
    return Event(
        event_id=event_id,
        series_id=series_id,
        title=title,
        mutually_exclusive=exclusive if isinstance(exclusive, bool) else None,
        outcome_set_complete=None,
        outcome_universe=None,
        provenance=Provenance(
            source_kind=DataSourceKind.KALSHI_AUTHORIZED,
            source_id=f"kalshi:{environment}",
            retrieved_at=retrieved_at,
        ),
    )


def map_series(
    raw: Mapping[str, Any], *, retrieved_at: datetime, environment: str
) -> Series | None:
    series_id = _text(raw.get("ticker"))
    title = _text(raw.get("title")) or series_id
    if series_id is None or title is None:
        return None
    category = _text(raw.get("category")) or "unspecified"
    return Series(
        series_id=series_id,
        title=title,
        category=category,
        provenance=Provenance(
            source_kind=DataSourceKind.KALSHI_AUTHORIZED,
            source_id=f"kalshi:{environment}",
            retrieved_at=retrieved_at,
        ),
    )


def catalog_from_payloads(
    markets: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
    series: Sequence[Mapping[str, Any]],
    *,
    retrieved_at: datetime,
    environment: str,
    limit: int,
) -> tuple[Catalog, list[str]]:
    by_event = {item.get("event_ticker"): item for item in events}
    by_series = {item.get("ticker"): item for item in series}
    skipped: list[str] = []
    mapped_markets: list[Market] = []
    for raw in markets:
        ticker = raw.get("ticker")
        event_raw = by_event.get(raw.get("event_ticker"))
        if not isinstance(event_raw, Mapping):
            skipped.append(f"{ticker}: missing event")
            continue
        series_raw = by_series.get(event_raw.get("series_ticker"))
        if not isinstance(series_raw, Mapping):
            skipped.append(f"{ticker}: missing series")
            continue
        if raw.get("fee_type_override") is None and event_raw.get("fee_type_override") is not None:
            raw = dict(raw)
            raw["fee_type_override"] = event_raw.get("fee_type_override")
            raw["fee_multiplier_override"] = event_raw.get("fee_multiplier_override")
        market = map_market(
            raw, event_raw, series_raw, retrieved_at=retrieved_at, environment=environment
        )
        if market is None:
            skipped.append(f"{ticker}: unsupported or malformed market")
            continue
        mapped_markets.append(market)
    selected, family_skips = select_complete_families(mapped_markets, limit=limit)
    skipped.extend(family_skips)
    mapped_markets = list(selected)
    mapped_events = []
    for event_id in sorted({m.event_id for m in mapped_markets}):
        raw_event = by_event.get(event_id)
        if isinstance(raw_event, Mapping):
            event = map_event(raw_event, retrieved_at=retrieved_at, environment=environment)
            if event is not None:
                mapped_events.append(event)
    mapped_series = []
    for series_id in sorted({m.series_id for m in mapped_markets}):
        raw_series = by_series.get(series_id)
        if isinstance(raw_series, Mapping):
            item = map_series(raw_series, retrieved_at=retrieved_at, environment=environment)
            if item is not None:
                mapped_series.append(item)
    catalog = Catalog(
        series=tuple(sorted(mapped_series, key=lambda s: s.series_id)),
        events=tuple(sorted(mapped_events, key=lambda e: e.event_id)),
        markets=tuple(sorted(mapped_markets, key=lambda m: m.market_id)),
    )
    return catalog, skipped


async def load_catalog(
    client: KalshiRestClient,
    settings: Settings,
    *,
    now: datetime | None = None,
) -> tuple[Catalog, list[str], int]:
    client.require_credentials()
    retrieved = now or datetime.now(UTC)
    series_filter = [
        part.strip() for part in settings.kalshi_series_tickers.split(",") if part.strip()
    ]
    params: dict[str, str] = {"status": "open"}
    if not settings.kalshi_include_mve:
        params["mve_filter"] = "exclude"
    pages = 0
    raw_markets: list[dict[str, object]] = []
    scan_cap = settings.kalshi_page_limit * settings.kalshi_max_pages
    if series_filter:
        for ticker in series_filter:
            remaining = scan_cap - len(raw_markets)
            if remaining <= 0:
                break
            batch, used = await client.paginate(
                "/markets",
                {**params, "series_ticker": ticker},
                "markets",
                max_items=remaining,
            )
            pages += used
            raw_markets.extend(batch)
    else:
        raw_markets, pages = await client.paginate(
            "/markets", params, "markets", max_items=scan_cap
        )
    if not raw_markets:
        raise CatalogLoadError("Bounded discovery returned no open markets.")
    event_ids = sorted(_string_values(raw_markets, "event_ticker"))
    sem = asyncio.Semaphore(settings.kalshi_read_concurrency)

    async def _event(event_id: str) -> dict[str, object] | None:
        async with sem:
            body = await client.get_json(f"/events/{event_id}")
        event = body.get("event")
        return event if isinstance(event, dict) else None

    events = [
        item
        for item in await asyncio.gather(*[_event(eid) for eid in event_ids])
        if isinstance(item, dict)
    ]
    series_ids = sorted(_string_values(events, "series_ticker"))

    async def _series(series_id: str) -> dict[str, object] | None:
        async with sem:
            body = await client.get_json(f"/series/{series_id}")
        series = body.get("series")
        if isinstance(series, dict):
            series = dict(series)
            series.setdefault("ticker", series_id)
            return series
        return None

    series = [
        item
        for item in await asyncio.gather(*[_series(sid) for sid in series_ids])
        if isinstance(item, dict)
    ]
    catalog, skipped = catalog_from_payloads(
        raw_markets,
        events,
        series,
        retrieved_at=retrieved,
        environment=settings.kalshi_environment,
        limit=settings.kalshi_market_limit,
    )
    if not catalog.markets:
        raise CatalogLoadError("No markets survived the read-only mapping checks.")
    return catalog, skipped, pages

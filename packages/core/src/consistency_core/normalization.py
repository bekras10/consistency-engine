"""Normalization of raw exchange payloads into exact domain values.

Field names follow our best understanding of Kalshi's fixed-point API (``*_dollars`` string
prices, ``*_fp`` string quantities). They are **unverified** in this milestone because the
documentation pages could not be fetched; see docs/data-contracts.md. All names are therefore
centralised in :class:`FieldMap` so they can be corrected in one place.

Parsing is strict: no exponent notation, no signs where none are expected, no more than 4 price
decimals or 2 quantity decimals, nothing outside (0, 1). Nothing is rounded.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pydantic import ValidationError

from consistency_core.events import LevelPair, OrderBookDeltaEvent, OrderBookSnapshotEvent
from consistency_core.models.common import Side
from consistency_core.models.orderbook import (
    BookErrorCode,
    BookValidationError,
    PriceLevel,
    build_side,
)
from consistency_core.money import CENT, ONE, PRICE_DP, QUANTITY_DP, ZERO
from consistency_core.ticks import PriceGrid

_UNSIGNED = re.compile(r"^\d+(\.\d+)?$")
_SIGNED = re.compile(r"^[+-]?\d+(\.\d+)?$")


def _frac_digits(text: str) -> int:
    return len(text.split(".", 1)[1]) if "." in text else 0


def parse_price(raw: object) -> Decimal:
    """Parse a dollar price string such as ``"0.3800"``; must lie strictly in (0, 1)."""
    if not isinstance(raw, str) or not _UNSIGNED.match(raw):
        raise BookValidationError(BookErrorCode.MALFORMED, f"bad price literal {raw!r}")
    if _frac_digits(raw) > PRICE_DP:
        raise BookValidationError(BookErrorCode.PRICE_PRECISION, f"{raw} exceeds {PRICE_DP} dp")
    value = Decimal(raw)
    if not (ZERO < value < ONE):
        raise BookValidationError(BookErrorCode.PRICE_OUT_OF_RANGE, f"{raw} not in (0, 1)")
    return value


def parse_quantity(raw: object) -> Decimal:
    """Parse a non-negative fixed-point contract quantity such as ``"12.50"``."""
    if not isinstance(raw, str) or not _UNSIGNED.match(raw):
        raise BookValidationError(BookErrorCode.MALFORMED, f"bad quantity literal {raw!r}")
    if _frac_digits(raw) > QUANTITY_DP:
        raise BookValidationError(
            BookErrorCode.QUANTITY_PRECISION, f"{raw} exceeds {QUANTITY_DP} dp"
        )
    return Decimal(raw)


def parse_signed_quantity(raw: object) -> Decimal:
    """Parse a signed delta such as ``"-54.00"``."""
    if not isinstance(raw, str) or not _SIGNED.match(raw):
        raise BookValidationError(BookErrorCode.MALFORMED, f"bad delta literal {raw!r}")
    if _frac_digits(raw) > QUANTITY_DP:
        raise BookValidationError(
            BookErrorCode.QUANTITY_PRECISION, f"{raw} exceeds {QUANTITY_DP} dp"
        )
    value = Decimal(raw)
    if value == ZERO:
        raise BookValidationError(BookErrorCode.MALFORMED, "zero delta carries no change")
    return value


def parse_legacy_cents(raw: object) -> Decimal:
    """Legacy integer-cent price (1..99) -> dollars."""
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise BookValidationError(BookErrorCode.MALFORMED, f"bad cent price {raw!r}")
    if not (0 < raw < 100):
        raise BookValidationError(BookErrorCode.PRICE_OUT_OF_RANGE, f"{raw} cents not in (0,100)")
    return Decimal(raw) * CENT


def parse_legacy_count(raw: object) -> Decimal:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        raise BookValidationError(BookErrorCode.MALFORMED, f"bad legacy count {raw!r}")
    return Decimal(raw)


def _pairs(raw: object, *, legacy: bool) -> list[LevelPair]:
    if raw is None:
        return []  # empty side
    if not isinstance(raw, Sequence) or isinstance(raw, str | bytes):
        raise BookValidationError(BookErrorCode.MALFORMED, "levels must be a list")
    out: list[LevelPair] = []
    for item in raw:
        if not isinstance(item, Sequence) or isinstance(item, str | bytes) or len(item) != 2:
            raise BookValidationError(BookErrorCode.MALFORMED, f"level {item!r} is not a pair")
        if legacy:
            out.append((parse_legacy_cents(item[0]), parse_legacy_count(item[1])))
        else:
            out.append((parse_price(item[0]), parse_quantity(item[1])))
    return out


def validate_side(
    side: Side, pairs: Sequence[LevelPair], grid: PriceGrid
) -> tuple[PriceLevel, ...]:
    """Grid-check prices, reject negatives/duplicates, drop zero quantities, sort descending."""
    for price, _qty in pairs:
        if not grid.is_valid_price(price):
            raise BookValidationError(BookErrorCode.PRICE_OFF_GRID, f"{side} price {price}")
    try:
        return build_side(side, pairs)
    except ValidationError as exc:
        raise _unwrap(exc) from exc


def _unwrap(exc: ValidationError) -> BookValidationError:
    for err in exc.errors():
        inner = (err.get("ctx") or {}).get("error")
        if isinstance(inner, BookValidationError):
            return inner
    return BookValidationError(BookErrorCode.MALFORMED, str(exc))


@dataclass(frozen=True, slots=True)
class FieldMap:
    """Payload field names. Defaults = best-knowledge Kalshi fixed-point names (UNVERIFIED)."""

    rest_container: str = "orderbook_fp"
    rest_yes: str = "yes_dollars"
    rest_no: str = "no_dollars"
    rest_legacy_container: str = "orderbook"
    rest_legacy_yes: str = "yes"
    rest_legacy_no: str = "no"
    ws_snapshot_type: str = "orderbook_snapshot"
    ws_delta_type: str = "orderbook_delta"
    ws_sid: str = "sid"
    ws_seq: str = "seq"
    ws_body: str = "msg"
    ws_market: str = "market_ticker"
    ws_snapshot_yes: str = "yes_dollars_fp"
    ws_snapshot_no: str = "no_dollars_fp"
    ws_delta_price: str = "price_dollars"
    ws_delta_qty: str = "delta_fp"
    ws_delta_side: str = "side"


DEFAULT_FIELDS = FieldMap()


@dataclass(frozen=True, slots=True)
class RestBook:
    yes_bids: tuple[PriceLevel, ...]
    no_bids: tuple[PriceLevel, ...]


def normalize_rest_orderbook(
    payload: Mapping[str, Any], grid: PriceGrid, fields: FieldMap = DEFAULT_FIELDS
) -> RestBook:
    """Normalize a REST order-book response (fixed-point form preferred, legacy cents accepted)."""
    if fields.rest_container in payload:
        body = payload[fields.rest_container]
        legacy = False
        yes_key, no_key = fields.rest_yes, fields.rest_no
    elif fields.rest_legacy_container in payload:
        body = payload[fields.rest_legacy_container]
        legacy = True
        yes_key, no_key = fields.rest_legacy_yes, fields.rest_legacy_no
    else:
        raise BookValidationError(BookErrorCode.MALFORMED, "no order-book container in payload")
    if not isinstance(body, Mapping):
        raise BookValidationError(BookErrorCode.MALFORMED, "order-book container is not an object")
    yes = validate_side(Side.YES, _pairs(body.get(yes_key), legacy=legacy), grid)
    no = validate_side(Side.NO, _pairs(body.get(no_key), legacy=legacy), grid)
    if yes and no and yes[0].price + no[0].price >= ONE:
        raise BookValidationError(BookErrorCode.CROSSED_BOOK, "best bids sum to >= 1")
    return RestBook(yes_bids=yes, no_bids=no)


def _int_field(body: Mapping[str, Any], key: str) -> int:
    v = body.get(key)
    if isinstance(v, bool) or not isinstance(v, int) or v < 0:
        raise BookValidationError(BookErrorCode.MALFORMED, f"{key} must be a non-negative int")
    return v


def normalize_ws_message(
    raw: Mapping[str, Any],
    *,
    connection_id: str,
    received_ts_ms: int,
    fields: FieldMap = DEFAULT_FIELDS,
) -> OrderBookSnapshotEvent | OrderBookDeltaEvent:
    """Normalize one WebSocket order-book message (snapshot or delta) to a core event.

    Only syntactic validation happens here; grid/market validation is the book manager's job.
    ``received_ts_ms`` is accepted for API symmetry with the ingestion path.
    """
    del received_ts_ms
    kind = raw.get("type")
    sid = _int_field(raw, fields.ws_sid)
    seq = _int_field(raw, fields.ws_seq)
    body = raw.get(fields.ws_body)
    if not isinstance(body, Mapping):
        raise BookValidationError(BookErrorCode.MALFORMED, "message body missing")
    market = body.get(fields.ws_market)
    if not isinstance(market, str) or not market:
        raise BookValidationError(BookErrorCode.MALFORMED, "market identifier missing")
    if kind == fields.ws_snapshot_type:
        return OrderBookSnapshotEvent(
            market_id=market,
            sid=str(sid),
            seq=seq,
            connection_id=connection_id,
            yes_bids=tuple(_pairs(body.get(fields.ws_snapshot_yes), legacy=False)),
            no_bids=tuple(_pairs(body.get(fields.ws_snapshot_no), legacy=False)),
        )
    if kind == fields.ws_delta_type:
        side_raw = body.get(fields.ws_delta_side)
        if side_raw not in ("yes", "no"):
            raise BookValidationError(BookErrorCode.MALFORMED, f"bad side {side_raw!r}")
        return OrderBookDeltaEvent(
            market_id=market,
            sid=str(sid),
            seq=seq,
            connection_id=connection_id,
            side=Side(side_raw),
            price=parse_price(body.get(fields.ws_delta_price)),
            delta=parse_signed_quantity(body.get(fields.ws_delta_qty)),
        )
    raise BookValidationError(BookErrorCode.MALFORMED, f"unsupported message type {kind!r}")

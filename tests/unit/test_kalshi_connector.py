"""Read-only Kalshi connector. HTTP and WebSocket are mocked. No live exchange."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from consistency_connectors.ingestion import BookManager
from consistency_connectors.kalshi.auth import PrivateKeySigner, signed_headers
from consistency_connectors.kalshi.endpoints import (
    PRODUCTION_REST,
    PRODUCTION_WS,
    ReadOnlyViolationError,
    assert_read_only,
    resolve_rest_base,
    resolve_ws_url,
)
from consistency_connectors.kalshi.mapping import catalog_from_payloads
from consistency_connectors.kalshi.rest import HttpResponse, KalshiHttpError, KalshiRestClient
from consistency_connectors.kalshi.stream import OrderBookStream
from consistency_connectors.kalshi.study import (
    build_report,
    distinct_episodes,
    report_json,
    report_markdown,
)
from consistency_connectors.settings import ConfigurationError, DataSourceMode, Settings
from consistency_connectors.sources.kalshi import KalshiDataSource
from consistency_core.events import OrderBookDeltaEvent, OrderBookSnapshotEvent, RawWireEvent
from consistency_core.fees import FeeCalculator, FeeScheduleRegistry
from consistency_core.fees.calculator import FeeReason
from consistency_core.models.common import DataSourceKind, Side, SyncStatus
from consistency_core.models.orderbook import BookValidationError, OrderBook
from consistency_core.money import dec
from consistency_core.normalization import normalize_rest_orderbook
from consistency_core.redact import redact
from consistency_core.relationships.discovery import discover
from consistency_core.ticks import PriceGrid
from consistency_worker.health import assess_feed
from tests.factories import market

NOW = datetime(2026, 10, 11, 18, 0, tzinfo=UTC)
GRID = PriceGrid.uniform(dec("0.01"))
SUBCENT = PriceGrid.uniform(dec("0.001"))


class _Signer:
    def __init__(self) -> None:
        self.last = ""

    def sign_text(self, text: str) -> str:
        self.last = text
        return "c2lnbmF0dXJl"


class _Http:
    def __init__(self, handler) -> None:
        self.handler = handler
        self.calls: list[tuple[str, str, dict[str, str]]] = []

    async def request(self, method: str, url: str, headers: dict[str, str], timeout: float):
        self.calls.append((method, url, dict(headers)))
        return self.handler(method, url, headers)

    async def aclose(self) -> None:
        return None


class _Socket:
    def __init__(self, frames: list[str]) -> None:
        self.frames = list(frames)
        self.sent: list[dict[str, object]] = []

    async def send(self, text: str) -> None:
        self.sent.append(json.loads(text))

    async def recv(self) -> str:
        if not self.frames:
            raise ConnectionError("closed")
        return self.frames.pop(0)

    async def close(self) -> None:
        return None

    async def ping(self) -> None:
        return None


class _Ws:
    def __init__(self, frames: list[str]) -> None:
        self.socket = _Socket(frames)
        self.headers: dict[str, str] = {}
        self.url = ""

    async def connect(self, url: str, headers: dict[str, str]):
        self.url = url
        self.headers = dict(headers)
        return self.socket


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "ENABLE_KALSHI_API": True,
        "KALSHI_AUTHORIZATION_CONFIRMED": True,
        "KALSHI_API_KEY_ID": "key-id",
        "KALSHI_PING_INTERVAL_S": 0,
        "KALSHI_MIN_REQUEST_INTERVAL_S": 0,
        "KALSHI_MAX_RETRIES": 2,
        "KALSHI_MARKET_LIMIT": 2,
        "KALSHI_PAGE_LIMIT": 1,
        "KALSHI_MAX_PAGES": 3,
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def _client(handler, **overrides: object) -> tuple[KalshiRestClient, _Http]:
    transport = _Http(handler)
    client = KalshiRestClient(
        _settings(**overrides),
        transport=transport,
        clock_ms=lambda: 1_700_000_000_000,
        sleep=_no_sleep,
        signer=_Signer(),
    )
    return client, transport


async def _no_sleep(_seconds: float) -> None:
    return None


def _market(ticker: str = "KXTEST-26-T50", **extra: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "ticker": ticker,
        "event_ticker": "KXTEST-26",
        "market_type": "binary",
        "status": "active",
        "notional_value_dollars": "1.0000",
        "settlement_bounds_type": "default",
        "open_time": "2026-10-01T00:00:00Z",
        "close_time": "2026-10-31T00:00:00Z",
        "expiration_time": "2026-10-31T00:00:00Z",
        "rules_primary": "Resolves YES when the named measurement is greater than 50.",
        "rules_secondary": "Void rules are not supplied here.",
        "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}],
        "strike_type": "greater",
        "floor_strike": "50",
        "title": "Reading above 50",
        "yes_sub_title": "Above 50",
    }
    payload.update(extra)
    return payload


def _event() -> dict[str, object]:
    return {
        "event_ticker": "KXTEST-26",
        "series_ticker": "KXTEST",
        "title": "Reading",
        "mutually_exclusive": True,
        "settlement_sources": [{"name": "Agency bulletin", "url": "https://example.invalid/a"}],
    }


def _series() -> dict[str, object]:
    return {"ticker": "KXTEST", "title": "Reading", "category": "climate"}


def _catalog(markets: list[dict[str, object]]):
    return catalog_from_payloads(
        markets, [_event()], [_series()], retrieved_at=NOW, environment="production", limit=20
    )


def test_read_only_boundary_refuses_orders_and_balances() -> None:
    assert assert_read_only("GET", "/markets") == "/trade-api/v2/markets"
    assert assert_read_only("GET", "/account/limits") == "/trade-api/v2/account/limits"
    with pytest.raises(ReadOnlyViolationError):
        assert_read_only("POST", "/portfolio/orders")
    with pytest.raises(ReadOnlyViolationError):
        assert_read_only("DELETE", "/orders/1")
    with pytest.raises(ReadOnlyViolationError):
        assert_read_only("GET", "/portfolio/balance")
    source = KalshiDataSource.__dict__
    assert "create_order" not in source
    assert "cancel_order" not in source


def test_hosts_stay_on_the_documented_environment() -> None:
    settings = _settings()
    assert resolve_rest_base(settings) == PRODUCTION_REST
    assert resolve_ws_url(settings) == PRODUCTION_WS
    demo = _settings(KALSHI_ENVIRONMENT="demo")
    assert "demo" in resolve_rest_base(demo)
    demo_url = "https://external-api.demo.kalshi.co/trade-api/v2"
    with pytest.raises(ConfigurationError):
        resolve_rest_base(_settings(KALSHI_REST_BASE_URL=demo_url))


def test_ed25519_signature_covers_timestamp_method_and_path_without_query() -> None:
    signer = PrivateKeySigner(Ed25519PrivateKey.generate())
    headers = signed_headers(
        key_id="key-id",
        signer=signer,
        timestamp_ms=1700000000000,
        method="GET",
        sign_path="/trade-api/v2/markets?cursor=abc",
    )
    assert headers["KALSHI-ACCESS-KEY"] == "key-id"
    assert headers["KALSHI-ACCESS-TIMESTAMP"] == "1700000000000"
    assert headers["KALSHI-ACCESS-SIGNATURE"]
    again = signed_headers(
        key_id="key-id",
        signer=signer,
        timestamp_ms=1700000000000,
        method="get",
        sign_path="/trade-api/v2/markets",
    )
    assert headers["KALSHI-ACCESS-SIGNATURE"] == again["KALSHI-ACCESS-SIGNATURE"]


def test_secrets_are_redacted() -> None:
    pem = "-----BEGIN PRIVATE KEY-----\nQUJD\n-----END PRIVATE KEY-----"
    text = redact(f"key {pem} KALSHI-ACCESS-SIGNATURE: abcdef")
    assert "QUJD" not in text
    assert "abcdef" not in text
    assert redact({"private_key": pem, "signature": "abcdef"}) == {
        "private_key": "***",
        "signature": "***",
    }


async def test_pagination_stops_at_the_market_cap() -> None:
    pages = {
        "": {"markets": [{"ticker": "A"}], "cursor": "next"},
        "next": {"markets": [{"ticker": "B"}], "cursor": "later"},
        "later": {"markets": [{"ticker": "C"}], "cursor": ""},
    }

    def handler(_method: str, url: str, _headers: dict[str, str]) -> HttpResponse:
        cursor = ""
        if "cursor=" in url:
            cursor = url.split("cursor=", 1)[1].split("&", 1)[0]
        body = json.dumps(pages[cursor]).encode()
        return HttpResponse(200, body)

    client, transport = _client(handler)
    items, page_count = await client.paginate(
        "/markets", {"status": "open"}, "markets", max_items=2
    )
    assert [item["ticker"] for item in items] == ["A", "B"]
    assert page_count == 2
    assert all(call[0] == "GET" for call in transport.calls)
    assert transport.calls[0][2]["KALSHI-ACCESS-SIGNATURE"]


async def test_http_429_retries_then_succeeds() -> None:
    state = {"n": 0}

    def handler(_method: str, _url: str, _headers: dict[str, str]) -> HttpResponse:
        state["n"] += 1
        if state["n"] == 1:
            return HttpResponse(429, b'{"error":"too many requests"}')
        return HttpResponse(200, b'{"ok":true}')

    client, _transport = _client(handler)
    body = await client.get_json("/exchange/status")
    assert body == {"ok": True}
    assert client.retries == 1


async def test_network_failure_is_bounded() -> None:
    def handler(_method: str, _url: str, _headers: dict[str, str]) -> HttpResponse:
        raise TimeoutError("timed out")

    client, _transport = _client(handler, KALSHI_MAX_RETRIES=1)
    with pytest.raises(TimeoutError):
        await client.get_json("/exchange/status")
    assert client.retries == 1


async def test_batch_orderbooks_repeat_the_ticker_parameter() -> None:
    def handler(_method: str, url: str, _headers: dict[str, str]) -> HttpResponse:
        assert url.count("tickers=") == 2
        body = {
            "orderbooks": [
                {"ticker": "A", "orderbook_fp": {"yes_dollars": [], "no_dollars": []}},
                {"ticker": "B", "orderbook_fp": {"yes_dollars": [], "no_dollars": []}},
            ]
        }
        return HttpResponse(200, json.dumps(body).encode())

    client, _transport = _client(handler, KALSHI_ORDERBOOK_BATCH_SIZE=2)
    rows = await client.get_orderbooks(["A", "B"])
    assert [row["ticker"] for row in rows] == ["A", "B"]
    with pytest.raises(KalshiHttpError):
        await client.get_orderbooks(["A", "B", "C"])


def test_metadata_keeps_rules_and_refuses_non_dollar_markets() -> None:
    catalog, skipped = _catalog([_market()])
    assert skipped == []
    market_row = catalog.markets[0]
    assert market_row.market_id == "KXTEST-26-T50"
    assert market_row.provenance.source_kind is DataSourceKind.KALSHI_AUTHORIZED
    assert market_row.settlement.terms is not None
    assert market_row.settlement.terms.value == dec("50")
    assert market_row.settlement.settlement_source == "Agency bulletin"
    assert market_row.settlement.underlying_id is None
    changed = _market(rules_primary="A different rule.")
    other, _ = _catalog([changed])
    assert other.markets[0].rules_hash != market_row.rules_hash
    skipped_catalog, skipped_rows = _catalog([_market(notional_value_dollars="2.0000")])
    assert skipped_catalog.markets == ()
    assert skipped_rows
    between, _ = _catalog([_market(strike_type="between", floor_strike="1", cap_strike="2")])
    assert between.markets[0].settlement.terms is None


def test_similar_titles_are_not_verified() -> None:
    first = _market("KXTEST-26-T50", title="Reading above 50")
    second = _market(
        "KXTEST-26-T60",
        title="Reading above 60",
        floor_strike="60",
        rules_primary="Resolves YES when the named measurement is greater than 60.",
    )
    catalog, _skipped = _catalog([first, second])
    relationships = discover(catalog, as_of=NOW)
    assert all(item.verification_status != "VERIFIED" for item in relationships)


def test_complementary_prices_fractional_size_and_subcent_grid() -> None:
    parsed = normalize_rest_orderbook(
        {"orderbook_fp": {"yes_dollars": [["0.4000", "1.25"]], "no_dollars": [["0.5500", "2.50"]]}},
        GRID,
    )
    book = OrderBook(
        market_id="KX",
        source="kalshi:production",
        received_ts_ms=1,
        yes_bids=parsed.yes_bids,
        no_bids=parsed.no_bids,
    )
    assert book.best_bid(Side.YES) == dec("0.4000")
    assert book.best_ask(Side.YES) == dec("0.4500")
    assert book.yes_asks[0].quantity == dec("2.50")
    assert book.best_ask(Side.NO) == dec("0.6000")
    assert book.displayed_quantity(Side.YES) == dec("2.50")
    empty = normalize_rest_orderbook({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}, GRID)
    assert empty.yes_bids == () and empty.no_bids == ()
    with pytest.raises(BookValidationError):
        normalize_rest_orderbook(
            {
                "orderbook_fp": {
                    "yes_dollars": [["0.6000", "1.00"]],
                    "no_dollars": [["0.5000", "1.00"]],
                }
            },
            GRID,
        )
    with pytest.raises(BookValidationError):
        normalize_rest_orderbook(
            {"orderbook_fp": {"yes_dollars": [["0.4000", "-1.00"]], "no_dollars": []}},
            GRID,
        )
    fine = normalize_rest_orderbook(
        {"orderbook_fp": {"yes_dollars": [["0.1230", "0.50"]], "no_dollars": []}},
        SUBCENT,
    )
    assert fine.yes_bids[0].price == dec("0.1230")


def test_rest_books_are_not_one_atomic_snapshot() -> None:
    manager = BookManager([market("A"), market("B")], source="kalshi:production")
    feed_a = OrderBookSnapshotEvent(
        market_id="A",
        sid="rest",
        seq=1,
        connection_id="rest-a",
        exchange_ts_ms=100,
        yes_bids=((dec("0.40"), dec("1")),),
        no_bids=((dec("0.50"), dec("1")),),
    )
    from consistency_core.events import StreamMessage

    manager.process(StreamMessage(position=0, emitted_ts_ms=100, received_ts_ms=110, event=feed_a))
    assert manager.sync_status("A") is SyncStatus.SYNCHRONIZED
    assert manager.sync_status("B") is SyncStatus.AWAITING_SNAPSHOT


def test_sequence_gap_invalidates_the_subscription() -> None:
    manager = BookManager([market("A", grid=GRID)], source="kalshi:production")
    from consistency_core.events import StreamMessage

    def message(event, position: int) -> StreamMessage:
        return StreamMessage(position=position, emitted_ts_ms=100, received_ts_ms=110, event=event)

    first = OrderBookSnapshotEvent(
        market_id="A",
        sid="7",
        seq=4,
        connection_id="c1",
        yes_bids=((dec("0.40"), dec("1.00")),),
        no_bids=((dec("0.50"), dec("1.00")),),
    )
    gap = OrderBookDeltaEvent(
        market_id="A",
        sid="7",
        seq=6,
        connection_id="c1",
        side=Side.YES,
        price=dec("0.40"),
        delta=dec("-1.00"),
    )
    manager.process(message(first, 0))
    assert manager.sync_status("A") is SyncStatus.SYNCHRONIZED
    manager.process(message(gap, 1))
    assert manager.sync_status("A") is SyncStatus.UNSYNCHRONIZED


async def test_websocket_handshake_snapshot_delta_and_unsubscribe() -> None:
    frames = [
        json.dumps({"type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 7}}),
        json.dumps(
            {
                "type": "orderbook_snapshot",
                "sid": 7,
                "seq": 1,
                "msg": {
                    "market_ticker": "KX",
                    "yes_dollars_fp": [["0.4000", "10.00"]],
                    "no_dollars_fp": [["0.5500", "5.00"]],
                    "ts_ms": 100,
                },
            }
        ),
        json.dumps(
            {
                "type": "orderbook_delta",
                "sid": 7,
                "seq": 2,
                "msg": {
                    "market_ticker": "KX",
                    "price_dollars": "0.4000",
                    "delta_fp": "-10.00",
                    "side": "yes",
                    "ts_ms": 110,
                },
            }
        ),
        json.dumps({"type": "error", "msg": {"code": 1}}),
    ]
    transport = _Ws(frames)
    stream = OrderBookStream(
        _settings(),
        signer=_Signer(),
        transport=transport,
        clock_ms=lambda: 50,
        sleep=_no_sleep,
    )
    seen = []

    async def collect() -> None:
        async for message in stream.messages(["KX"]):
            seen.append(message.event)
            if isinstance(message.event, OrderBookSnapshotEvent):
                await stream.unsubscribe(["KX"])

    with pytest.raises(ConnectionError):
        await collect()
    assert transport.url == PRODUCTION_WS
    assert transport.headers["KALSHI-ACCESS-KEY"] == "key-id"
    assert "KALSHI-ACCESS-SIGNATURE" in transport.headers
    assert transport.socket.sent[0]["cmd"] == "subscribe"
    assert any(item.get("cmd") == "update_subscription" for item in transport.socket.sent)
    snapshots = [event for event in seen if isinstance(event, OrderBookSnapshotEvent)]
    deltas = [event for event in seen if isinstance(event, OrderBookDeltaEvent)]
    assert snapshots[0].sid == "7" and snapshots[0].seq == 1
    assert snapshots[0].yes_bids[0] == (dec("0.4000"), dec("10.00"))
    assert deltas[0].seq == 2
    assert any(isinstance(event, RawWireEvent) for event in seen)


async def test_auth_error_is_not_retried_inside_the_stream() -> None:
    transport = _Ws([json.dumps({"type": "error", "msg": {"code": 9, "msg": "auth"}})])
    stream = OrderBookStream(
        _settings(), signer=_Signer(), transport=transport, clock_ms=lambda: 1, sleep=_no_sleep
    )
    from consistency_connectors.base import SourceAuthenticationError

    with pytest.raises(SourceAuthenticationError):
        async for _message in stream.messages(["KX"]):
            pass
    assert stream.auth_failures == 1
    assert stream.state == "authentication_failed"


def test_fee_override_and_default_assumptions_stay_unverified() -> None:
    overridden, _skipped = _catalog([_market(fee_type_override="quadratic")])
    market_row = overridden.markets[0]
    assert market_row.exchange_metadata["fee_override_present"] is True
    calculator = FeeCalculator(FeeScheduleRegistry.from_directory(Path("fixtures/fees")))
    resolution = calculator.resolve(market_row, NOW)
    assert resolution.verified is False
    assert FeeReason.EVENT_FEE_OVERRIDE in resolution.reasons
    plain, _ = _catalog([_market()])
    plain_resolution = calculator.resolve(plain.markets[0], NOW)
    assert plain_resolution.verified is False
    assert FeeReason.EVENT_FEE_OVERRIDE not in plain_resolution.reasons
    synthetic = market("SYN")
    assert synthetic.provenance.source_kind is DataSourceKind.SYNTHETIC
    assert plain.markets[0].provenance.source_kind is not synthetic.provenance.source_kind


def test_raw_retention_stays_off_unless_the_operator_sets_the_flag() -> None:
    blocked = _settings(DATA_SOURCE=DataSourceMode.KALSHI_AUTHORIZED)
    assert blocked.raw_market_data_persisted is False
    allowed = _settings(
        DATA_SOURCE=DataSourceMode.KALSHI_AUTHORIZED,
        THIRD_PARTY_RAW_PERSISTENCE_AUTHORIZED=True,
    )
    assert allowed.raw_market_data_persisted is True


def test_episodes_collapse_and_a_zero_result_stays_in_progress() -> None:
    rows = [
        {
            "detection_id": "d1",
            "relationship_id": "r1",
            "strategy_id": "s1",
            "classification": "FEE_UNVERIFIED",
            "status": "open",
            "first_observed_ms": 10,
            "last_observed_ms": 20,
            "gross_edge": "0.01",
            "fees": "0.002",
            "net_edge": "0.008",
            "depth_supported_quantity": "1.00",
        },
        {
            "detection_id": "d1",
            "relationship_id": "r1",
            "strategy_id": "s1",
            "classification": "FEE_UNVERIFIED",
            "status": "closed",
            "first_observed_ms": 10,
            "last_observed_ms": 40,
            "close_reason": "quote_left",
        },
    ]
    episodes = distinct_episodes(rows)
    assert len(episodes) == 1
    assert episodes[0].last_ms == 40
    report = build_report(
        study_id="study-1",
        study_start="2026-10-11T00:00:00+00:00",
        study_end="",
        now=NOW,
        commit="abc",
        config_fingerprint="cfg",
        environment="production",
        market_ids=["KX"],
        relationship_ids=["r1"],
        verified_relationship_ids=[],
        fee_schedule_ids=["kalshi-2026-07-07"],
        raw_retention_authorized=False,
        replay_available=False,
        rows=[],
        analyzed_updates=0,
        synchronized_evaluations=0,
        observed_ms=0,
        synchronized_ms=0,
    )
    assert report["window_status"] == "IN_PROGRESS"
    assert report["fee_verified_positive_candidates"] == 0
    assert report["distinct_episodes"] == 0
    text = report_markdown(report)
    assert "IN_PROGRESS" in text
    assert "executed arbitrage" not in text.lower() or "not describe an executed" in text.lower()
    blob = report_json(report)
    assert "orderbook_fp" not in blob
    again = build_report(
        study_id="study-1",
        study_start="",
        study_end="",
        now=NOW,
        commit="abc",
        config_fingerprint="cfg",
        environment="production",
        market_ids=[],
        relationship_ids=[],
        verified_relationship_ids=[],
        fee_schedule_ids=[],
        raw_retention_authorized=False,
        replay_available=False,
        rows=[],
        analyzed_updates=None,
        synchronized_evaluations=None,
    )
    assert again["window_status"] == "NOT_STARTED"


def test_feed_health_distinguishes_startup_from_a_dead_socket() -> None:
    assert assess_feed(state=None, checked_at_epoch=None, now=100, max_age_s=30)[1] == 0
    assert (
        assess_feed(state="connected", checked_at_epoch=90, now=100, max_age_s=30)[0] == "feed_ok"
    )
    assert assess_feed(state="disconnected", checked_at_epoch=99, now=100, max_age_s=30)[1] == 1
    assert (
        assess_feed(state="connected", checked_at_epoch=10, now=100, max_age_s=30)[0]
        == "feed_stale"
    )


def test_live_smoke_stays_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KALSHI_LIVE_SMOKE", raising=False)
    monkeypatch.delenv("KALSHI_API_KEY_ID", raising=False)
    monkeypatch.delenv("KALSHI_AUTHORIZATION_CONFIRMED", raising=False)
    assert os.environ.get("KALSHI_LIVE_SMOKE") != "1"
    source = KalshiDataSource(_settings())
    assert source.stream_is_finite is False
    assert source.feed_status()["replay_available"] is False

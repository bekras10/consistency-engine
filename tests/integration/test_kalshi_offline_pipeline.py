"""Mocked Kalshi discovery, books, reconnect, and a PostgreSQL study report.

No live exchange. The expected report numbers are derived in the test body.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from urllib.parse import urlsplit

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat

from consistency_connectors.ingestion import BookManager
from consistency_connectors.kalshi.families import apply_settlement_overlay
from consistency_connectors.kalshi.rest import HttpResponse
from consistency_connectors.kalshi.study import build_report
from consistency_connectors.kalshi.universe import MembershipChange
from consistency_connectors.settings import Settings
from consistency_connectors.sources.kalshi import KalshiDataSource
from consistency_core.events import OrderBookDeltaEvent, OrderBookSnapshotEvent
from consistency_core.models.common import SyncStatus
from consistency_core.relationships.discovery import discover
from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.recording import DetectionWriter
from consistency_persistence.replay_host import _load_playback
from consistency_persistence.schema import DataSourceRow, DetectionRow, IngestionSessionRow
from consistency_persistence.study_ledger import StudyLedger
from consistency_persistence.timeutil import now_utc
from tests.unit.test_detection_pipeline import Harness

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 11, tzinfo=UTC)
STUDY = "study-offline"
SESSION = "ses-offline"


def _market(ticker: str, strike: str) -> dict[str, object]:
    return {
        "ticker": ticker,
        "event_ticker": "EVT-A",
        "market_type": "binary",
        "status": "active",
        "notional_value_dollars": "1.0000",
        "settlement_bounds_type": "default",
        "open_time": "2026-10-01T00:00:00Z",
        "close_time": "2026-10-31T00:00:00Z",
        "expiration_time": "2026-10-31T00:00:00Z",
        "rules_primary": f"Resolves using the reviewed measurement above {strike}.",
        "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}],
        "strike_type": "greater",
        "floor_strike": strike,
        "yes_sub_title": f"Above {strike}",
        "title": f"Reading above {strike}",
    }


def _event() -> dict[str, object]:
    return {
        "event_ticker": "EVT-A",
        "series_ticker": "KXTEST",
        "title": "Reading",
        "mutually_exclusive": False,
        "settlement_sources": [{"name": "Agency bulletin"}],
    }


class _Http:
    def __init__(self) -> None:
        self.drop_narrowest = False

    async def request(self, method: str, url: str, headers: dict[str, str], timeout: float):
        del method, headers, timeout
        path = urlsplit(url).path
        tickers = ("KX-40", "KX-50") if self.drop_narrowest else ("KX-30", "KX-40", "KX-50")
        if path.endswith("/markets"):
            body = {"markets": [_market(ticker, ticker.split("-")[1]) for ticker in tickers]}
        elif path.endswith("/events/EVT-A"):
            body = {"event": _event()}
        elif path.endswith("/series/KXTEST"):
            body = {"series": {"ticker": "KXTEST", "title": "Reading", "category": "climate"}}
        else:
            body = {}
        return HttpResponse(status=200, body=json.dumps(body).encode())

    async def aclose(self) -> None:
        return None


class _Socket:
    def __init__(self, frames: list[str]) -> None:
        self.frames = list(frames)

    async def send(self, text: str) -> None:
        del text

    async def recv(self) -> str:
        if not self.frames:
            raise ConnectionError("closed")
        return self.frames.pop(0)

    async def close(self) -> None:
        return None


class _Ws:
    def __init__(self, sessions: list[list[str]]) -> None:
        self.sessions = sessions

    async def connect(self, url: str, headers: dict[str, str]):
        del url, headers
        return _Socket(self.sessions.pop(0))


def _frame(kind: str, seq: int, ts_ms: int) -> str:
    if kind == "subscribed":
        return json.dumps({"type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 7}})
    if kind == "snapshot":
        return json.dumps(
            {
                "type": "orderbook_snapshot",
                "sid": 7,
                "seq": seq,
                "msg": {
                    "market_ticker": "KX-50",
                    "yes_dollars_fp": [["0.4000", "10.00"]],
                    "no_dollars_fp": [["0.5500", "5.00"]],
                    "ts_ms": ts_ms,
                },
            }
        )
    return json.dumps(
        {
            "type": "orderbook_delta",
            "sid": 7,
            "seq": seq,
            "msg": {
                "market_ticker": "KX-50",
                "price_dollars": "0.4000",
                "delta_fp": "-1.00",
                "side": "yes",
                "ts_ms": ts_ms,
            },
        }
    )


def _membership_event(change: MembershipChange, *, ordinal: int) -> dict[str, object]:
    return {
        "idempotency_key": (
            f"{SESSION}|membership|{change.subject_kind}|{change.subject_id}|"
            f"{change.action}|{change.at_ms}|{ordinal}"
        ),
        "study_id": STUDY,
        "session_id": SESSION,
        "kind": "membership",
        "occurred_ms": change.at_ms,
        "subject_id": change.subject_id,
        "classification": change.verification_status,
        "synchronized": True,
        "payload": {
            "action": change.action,
            "subject_kind": change.subject_kind,
            "fingerprint": change.fingerprint,
        },
    }


async def _consume(source: KalshiDataSource, manager: BookManager, seen: list[object]) -> None:
    async for message in source.subscribe_orderbooks(["KX-50", "KX-40", "KX-30"]):
        seen.append(message.event)
        manager.process(message)


async def _persist_membership(ledger: StudyLedger, changes: list[MembershipChange]) -> None:
    events = [_membership_event(change, ordinal=index) for index, change in enumerate(changes)]
    await ledger.record_events(events)
    for event in events:
        payload = event["payload"]
        assert isinstance(payload, dict)
        occurred = event["occurred_ms"]
        assert isinstance(occurred, int)
        if payload["action"] == "enter":
            await ledger.enter_membership(
                study_id=STUDY,
                session_id=SESSION,
                subject_kind=str(payload["subject_kind"]),
                subject_id=str(event["subject_id"]),
                entered_ms=occurred,
                fingerprint=str(payload["fingerprint"]),
                verification_status=str(event["classification"]),
            )
        elif payload["action"] == "exit":
            await ledger.exit_membership(
                study_id=STUDY,
                session_id=SESSION,
                subject_kind=str(payload["subject_kind"]),
                subject_id=str(event["subject_id"]),
                exited_ms=occurred,
            )


async def test_mocked_discovery_books_reconnect_and_study_report(db: str, tmp_path) -> None:
    """Hand-derived report for this seeded session:

    synchronized coverage [1000, 2000) = 1000 ms
    no downtime, so observed duration is also 1000 ms
    evaluations = 2: one synchronized NO_OPPORTUNITY and one UNSYNCHRONIZED_DATA
    funnel 1 + 1 = 2; synchronized evaluations = 1
    book updates = 2 and episodes = 0 (updates are not opportunities)
    reconnects = 1
    markets KX-30, KX-40, KX-50 enter at 1000; KX-30 exits at 5000
    verified relationships = 1
    planned end 2026-10-22 is after the report clock, so status is IN_PROGRESS
    raw retention is off, so replay_available is false
    """
    key_path = tmp_path / "kalshi.pem"
    key_path.write_bytes(
        Ed25519PrivateKey.generate().private_bytes(
            Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
        )
    )
    clock = {"ms": 1000}

    def now_ms() -> int:
        return clock["ms"]

    async def sleep(_seconds: float) -> None:
        return None

    http = _Http()
    ws = _Ws(
        [
            [_frame("subscribed", 0, 100), _frame("snapshot", 1, 100), _frame("delta", 2, 110)],
            [_frame("subscribed", 0, 200), _frame("snapshot", 1, 200)],
        ]
    )
    settings = Settings(
        ENABLE_KALSHI_API=True,
        KALSHI_AUTHORIZATION_CONFIRMED=True,
        KALSHI_API_KEY_ID="key-id",
        KALSHI_PRIVATE_KEY_PATH=str(key_path),
        KALSHI_SERIES_TICKERS="KXTEST",
        KALSHI_MARKET_LIMIT=20,
        KALSHI_PAGE_LIMIT=20,
        KALSHI_MAX_PAGES=2,
        KALSHI_MIN_REQUEST_INTERVAL_S=0,
        KALSHI_PING_INTERVAL_S=0,
        THIRD_PARTY_RAW_PERSISTENCE_AUTHORIZED=False,
        ENABLE_LIVE_TRADING=False,
    )
    source = KalshiDataSource(
        settings, transport=http, ws_transport=ws, clock_ms=now_ms, sleep=sleep
    )
    markets = await source.get_markets()
    assert [market.market_id for market in markets] == ["KX-30", "KX-40", "KX-50"]
    catalog = await source._ensure()
    assert not any(item.is_verified for item in discover(catalog, as_of=NOW))
    overlay = {
        market_id: {
            "underlying_id": "station-1",
            "measurement": "daily high",
            "location": "station-1",
            "observation_window": "2026-10-31 local day",
            "settlement_source": "Agency bulletin",
            "methodology": "agency daily high",
            "unit": "celsius",
            "early_close_policy": "no_early_close",
            "exceptional_resolution": [],
            "rounding": {"mode": "none"},
        }
        for market_id in ("KX-30", "KX-40", "KX-50")
    }
    reviewed = apply_settlement_overlay(catalog, overlay)
    verified = [item for item in discover(reviewed, as_of=NOW) if item.is_verified]
    assert len(verified) == 1
    source.note_relationships(verified)
    opened = source.drain_membership_changes()
    manager = BookManager(reviewed.markets, source="kalshi:production")
    seen: list[object] = []
    with pytest.raises(ConnectionError):
        await _consume(source, manager, seen)
    assert any(isinstance(event, OrderBookSnapshotEvent) for event in seen)
    assert any(isinstance(event, OrderBookDeltaEvent) for event in seen)
    assert manager.sync_status("KX-50") is SyncStatus.SYNCHRONIZED
    manager.connection_lost("kalshi-1", "CONNECTION_LOST")
    assert manager.sync_status("KX-50") is SyncStatus.UNSYNCHRONIZED
    with pytest.raises(ConnectionError):
        await _consume(source, manager, seen)
    assert manager.sync_status("KX-50") is SyncStatus.SYNCHRONIZED
    assert source.get_connection_health().reconnects == 1

    clock["ms"] = 5000
    http.drop_narrowest = True
    await source.refresh_universe()
    rotated = source.drain_membership_changes()
    exited = [item for item in rotated if item.action == "exit"]
    assert {item.subject_id for item in exited} == {"KX-30"}

    engine = make_engine(db)
    ledger = StudyLedger(make_sessionmaker(engine))
    await ledger.ensure_study(
        study_id=STUDY,
        source_kind="kalshi_authorized",
        environment="production",
        commit_sha="offline",
        config_fingerprint="cfg-offline",
        planned_start="2026-10-01T00:00:00+00:00",
        planned_end="2026-10-22T00:00:00+00:00",
        raw_retention_authorized=False,
    )
    await ledger.open_session(
        study_id=STUDY,
        session_id=SESSION,
        source_kind="kalshi_authorized",
        eligible=True,
        started_ms=1000,
    )
    await _persist_membership(ledger, [*opened, *rotated])
    await ledger.record_events(
        [
            {
                "idempotency_key": f"{SESSION}|coverage|1",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "coverage",
                "occurred_ms": 2000,
                "synchronized": True,
                "payload": {"start_ms": 1000, "end_ms": 2000},
            },
            {
                "idempotency_key": f"{SESSION}|eval|neg",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "evaluation",
                "occurred_ms": 1500,
                "classification": "NO_OPPORTUNITY",
                "synchronized": True,
                "payload": {},
            },
            {
                "idempotency_key": f"{SESSION}|eval|unsync",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "evaluation",
                "occurred_ms": 1600,
                "classification": "UNSYNCHRONIZED_DATA",
                "synchronized": False,
                "payload": {},
            },
            {
                "idempotency_key": f"{SESSION}|book|1",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "book_update",
                "occurred_ms": 1100,
                "synchronized": True,
                "payload": {},
            },
            {
                "idempotency_key": f"{SESSION}|book|2",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "book_update",
                "occurred_ms": 1200,
                "synchronized": True,
                "payload": {},
            },
            {
                "idempotency_key": f"{SESSION}|reconnect|1",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "reconnect",
                "occurred_ms": 1800,
                "synchronized": False,
                "payload": {},
            },
        ]
    )
    sessions = make_sessionmaker(engine)
    async with sessions() as session, session.begin():
        session.add(
            DataSourceRow(
                data_source_id="src-offline",
                name="kalshi",
                kind="kalshi_authorized",
                created_at=now_utc(),
            )
        )
        session.add(
            IngestionSessionRow(
                session_id="unit",
                data_source_id="src-offline",
                status="running",
                fingerprint="fp",
                source_label="kalshi:production",
                deterministic=False,
                pinned=False,
                raw_persisted=False,
                started_at=now_utc(),
            )
        )
    harness = Harness()
    events = harness.open_books()
    assert events[0].record.certificate_json is not None
    assert "asks" in events[0].record.certificate_json
    await DetectionWriter(sessions, retain_quote_levels=False).write(events)
    async with sessions() as session:
        row = await session.get(DetectionRow, events[0].detection_id)
        assert row is not None and row.certificate_json is not None
        body = json.loads(row.certificate_json)
        assert body["books"][0]["asks"] == []
        assert body["top_of_book"]["best_asks"]
        with pytest.raises(ValueError, match="no stored journal"):
            await _load_playback(session, "unit")
    facts = await ledger.load(STUDY)
    report = build_report(
        study_id=facts.study_id,
        study_start=facts.planned_start,
        study_end=facts.planned_end,
        now=NOW,
        commit=facts.commit_sha,
        config_fingerprint=facts.config_fingerprint,
        environment=facts.environment,
        market_ids=facts.market_ids,
        relationship_ids=facts.relationship_ids,
        verified_relationship_ids=facts.verified_relationship_ids,
        fee_schedule_ids=["kalshi-2026-07-07"],
        raw_retention_authorized=facts.raw_retention_authorized,
        replay_available=facts.replay_available,
        rows=facts.episode_rows,
        analyzed_updates=facts.analyzed_updates,
        synchronized_evaluations=None,
        observed_ms=facts.observed_ms,
        synchronized_ms=facts.synchronized_ms,
        coverage_established=facts.coverage_established,
        eligible_sessions=facts.eligible_sessions,
        evaluation_rows=facts.evaluation_rows,
        membership=facts.membership,
        exclusions=facts.exclusions,
        ledger={"reconnects": facts.reconnects, "downtime_ms": facts.downtime_ms},
    )
    assert report["window_status"] == "IN_PROGRESS"
    assert report["observed_duration_ms"] == 1000
    assert report["synchronized_monitoring_ms"] == 1000
    assert facts.eligible_sessions == 1
    assert facts.replay_available is False
    assert facts.reconnects == 1
    assert facts.analyzed_updates == 2
    assert report["distinct_episodes"] == 0
    assert len(facts.verified_relationship_ids) == 1
    funnel = report["evaluation_funnel"]
    assert isinstance(funnel, dict)
    assert funnel["population_count"] == 2
    assert funnel["synchronized_evaluations"] == 1
    assert funnel["non_overlapping"] == {
        "market_data_quality_exclusion": 1,
        "no_theoretical_deviation": 1,
    }
    by_market = {
        str(row["subject_id"]): row["exited_ms"]
        for row in facts.membership
        if row["subject_kind"] == "market"
    }
    assert by_market["KX-30"] == 5000
    assert by_market["KX-40"] is None
    assert by_market["KX-50"] is None
    await engine.dispose()

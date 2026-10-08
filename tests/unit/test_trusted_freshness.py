"""Freshness is only derived from trusted messages; future-dated timestamps are quarantined and
negative ages never reach a verified classification (hardening pass, P2)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from consistency_connectors.ingestion import BookManager, DesyncReason
from consistency_core.events import (
    HeartbeatEvent,
    OrderBookDeltaEvent,
    RawWireEvent,
    StreamEvent,
    StreamMessage,
)
from consistency_core.models import Side, SyncStatus
from consistency_core.models.detection import Classification
from tests.factories import T0_MS, market
from tests.golden.support import NOW_MS
from tests.unit.test_evaluator import run_a
from tests.unit.test_ingestion import delta, levels, snap

D = Decimal
LATER = T0_MS + 60_000


def _msg(event: StreamEvent, pos: int, ts: int, *, received: int | None = None) -> StreamMessage:
    return StreamMessage(
        position=pos,
        emitted_ts_ms=ts,
        received_ts_ms=ts + 5 if received is None else received,
        event=event,
    )


@pytest.fixture
def mgr() -> BookManager:
    """A on c1/sid=1 and B on c1/sid=9, both synchronized; confirmed through T0+20."""
    m = BookManager([market("A"), market("B")], source="unit")
    m.process(_msg(snap("A", 1, sid="1", yes=[("0.40", "10")]), 0, T0_MS + 10))
    m.process(_msg(snap("B", 1, sid="9", yes=[("0.20", "10")]), 1, T0_MS + 20))
    assert m.book("B").confirmed_through_ms == T0_MS + 20
    return m


def _confirmed(mgr: BookManager) -> int | None:
    return mgr.book("B").confirmed_through_ms


# ---------------------------------------------------------------- untrusted messages
def test_malformed_message_does_not_advance_confirmed_through(mgr: BookManager) -> None:
    bad = RawWireEvent(
        connection_id="c1",
        payload={"type": "orderbook_delta", "sid": 1, "seq": 2, "msg": {"market_ticker": "A"}},
    )
    mgr.process(_msg(bad, 2, LATER))
    assert mgr.sync_status("B") is SyncStatus.SYNCHRONIZED
    assert _confirmed(mgr) == T0_MS + 20


def test_duplicate_message_does_not_advance_confirmed_through(mgr: BookManager) -> None:
    mgr.process(_msg(delta("B", 1, Side.YES, "0.20", "1", sid="9"), 2, LATER))
    assert mgr.stats.duplicates_dropped == 1
    assert _confirmed(mgr) == T0_MS + 20


def test_rejected_message_does_not_advance_confirmed_through(mgr: BookManager) -> None:
    mgr.process(_msg(delta("A", 2, Side.YES, "0.40", "-100"), 2, LATER))  # negative qty
    assert mgr.stats.rejected == 1
    assert mgr.sync_status("A") is SyncStatus.UNSYNCHRONIZED
    assert _confirmed(mgr) == T0_MS + 20


def test_gapped_message_does_not_advance_confirmed_through(mgr: BookManager) -> None:
    mgr.process(_msg(delta("A", 5, Side.YES, "0.40", "1"), 2, LATER))
    assert mgr.stats.gaps_detected == 1
    assert _confirmed(mgr) == T0_MS + 20


def test_stale_subscription_message_does_not_advance_confirmed_through(
    mgr: BookManager,
) -> None:
    mgr.process(_msg(delta("A", 2, Side.YES, "0.40", "1", sid="77"), 2, LATER))
    assert mgr.stats.ignored_stale_subscription == 1
    assert _confirmed(mgr) == T0_MS + 20


def test_accepted_delta_and_heartbeat_advance_confirmed_through(mgr: BookManager) -> None:
    mgr.process(_msg(delta("A", 2, Side.YES, "0.40", "1"), 2, T0_MS + 30))
    assert _confirmed(mgr) == T0_MS + 30
    mgr.process(_msg(HeartbeatEvent(connection_id="c1"), 3, T0_MS + 40))
    assert _confirmed(mgr) == T0_MS + 40


# ---------------------------------------------------------------- future-dated timestamps
def _future_delta(seq: int, exchange_ts: int) -> OrderBookDeltaEvent:
    return OrderBookDeltaEvent(
        market_id="A",
        sid="1",
        seq=seq,
        connection_id="c1",
        exchange_ts_ms=exchange_ts,
        side=Side.YES,
        price=D("0.40"),
        delta=D("1"),
    )


def test_future_dated_exchange_timestamp_is_quarantined(mgr: BookManager) -> None:
    t = T0_MS + 30
    ups = mgr.process(_msg(_future_delta(2, t + 5 + 60_000), 2, t))
    assert [(u.market_id, u.kind, u.reason) for u in ups] == [
        ("A", "desync", DesyncReason.FUTURE_TIMESTAMP)
    ]
    assert levels(mgr, "A", Side.YES) == [("0.40", "10")]  # not applied
    assert mgr.stats.future_timestamps == 1
    assert _confirmed(mgr) == T0_MS + 20
    # the sequence number was consumed: the next in-order message is not a gap
    mgr.process(_msg(delta("A", 3, Side.YES, "0.40", "1"), 3, t + 10))
    assert mgr.stats.gaps_detected == 0


def test_future_dated_emission_time_does_not_advance_confirmed_through(
    mgr: BookManager,
) -> None:
    hb = HeartbeatEvent(connection_id="c1")
    mgr.process(_msg(hb, 2, T0_MS + 120_000, received=T0_MS + 30))
    assert mgr.stats.future_timestamps == 1
    assert _confirmed(mgr) == T0_MS + 20
    assert mgr.sync_status("B") is SyncStatus.SYNCHRONIZED


def test_future_tolerance_is_configurable() -> None:
    strict = BookManager([market("A")], source="unit", max_future_ms=0)
    ok = BookManager([market("A")], source="unit", max_future_ms=10_000)
    for m in (strict, ok):
        m.process(_msg(snap("A", 1, yes=[("0.40", "10")]), 0, T0_MS))
        m.process(_msg(_future_delta(2, T0_MS + 1_000), 1, T0_MS, received=T0_MS))
    assert strict.sync_status("A") is SyncStatus.UNSYNCHRONIZED
    assert ok.sync_status("A") is SyncStatus.SYNCHRONIZED
    assert levels(ok, "A", Side.YES) == [("0.40", "11")]


def test_within_default_tolerance_is_accepted(mgr: BookManager) -> None:
    t = T0_MS + 30
    mgr.process(_msg(_future_delta(2, t + 5 + 200), 2, t))
    assert mgr.sync_status("A") is SyncStatus.SYNCHRONIZED
    assert mgr.stats.future_timestamps == 0


# ---------------------------------------------------------------- evaluator: negative ages
def test_negative_book_age_is_never_verified_and_never_reported() -> None:
    future = NOW_MS + 5_000
    ev = run_a(book_update={"GOLD-A-GE3": {"confirmed_through_ms": future}})
    assert ev.classification is Classification.STALE_DATA
    assert "NEGATIVE_BOOK_AGE" in ev.reason_codes
    assert ev.certificate.timing.max_book_age_ms is None


def test_one_ms_in_future_still_fails() -> None:
    ev = run_a(
        book_update={
            "GOLD-A-GE3": {"confirmed_through_ms": NOW_MS + 1},
            "GOLD-A-GE2": {"confirmed_through_ms": NOW_MS},
        }
    )
    assert ev.classification is Classification.STALE_DATA
    assert ev.reason_codes == ("NEGATIVE_BOOK_AGE",)


def test_zero_age_is_fresh() -> None:
    ev = run_a(
        book_update={
            "GOLD-A-GE3": {"confirmed_through_ms": NOW_MS},
            "GOLD-A-GE2": {"confirmed_through_ms": NOW_MS},
        }
    )
    assert ev.classification is Classification.FEE_ADJUSTED_CANDIDATE
    assert ev.certificate.timing.max_book_age_ms == 0

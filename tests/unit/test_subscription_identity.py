"""Subscription state is keyed by (connection_id, subscription_id): two connections that both
use sid=1 never share sequence, gap, duplicate or malformed-message state (hardening pass, P2)."""

from __future__ import annotations

import pytest

from consistency_connectors.ingestion import BookManager, DesyncReason
from consistency_core.events import RawWireEvent
from consistency_core.models import Side, SyncStatus
from tests.factories import market
from tests.unit.test_ingestion import Feed, delta, levels, snap


@pytest.fixture
def mgr() -> BookManager:
    """A on c1/sid=1 at seq 1; B on c2/sid=1 at seq 1. Both synchronized."""
    m = BookManager([market("A"), market("B")], source="unit")
    feed = Feed()
    m.process(feed.msg(snap("A", 1, sid="1", conn="c1", yes=[("0.40", "10")])))
    m.process(feed.msg(snap("B", 1, sid="1", conn="c2", yes=[("0.20", "10")])))
    return m


@pytest.fixture
def feed() -> Feed:
    f = Feed()
    f.pos = 10
    return f


def test_same_sid_on_two_connections_both_synchronize(mgr: BookManager) -> None:
    assert mgr.sync_status("A") is SyncStatus.SYNCHRONIZED
    assert mgr.sync_status("B") is SyncStatus.SYNCHRONIZED
    assert mgr.stats.duplicates_dropped == 0
    assert mgr.book("B").connection_id == "c2"


def test_sequences_are_independent_per_connection(mgr: BookManager, feed: Feed) -> None:
    for seq in range(2, 6):
        mgr.process(feed.msg(delta("A", seq, Side.YES, "0.40", "1", conn="c1")))
    ups = mgr.process(feed.msg(delta("B", 2, Side.YES, "0.20", "1", conn="c2")))
    assert [(u.market_id, u.kind) for u in ups] == [("B", "delta")]
    assert levels(mgr, "A", Side.YES) == [("0.40", "14")]
    assert levels(mgr, "B", Side.YES) == [("0.20", "11")]
    assert mgr.stats.duplicates_dropped == 0
    assert mgr.stats.gaps_detected == 0


def test_gap_on_one_connection_does_not_desync_the_other(mgr: BookManager, feed: Feed) -> None:
    ups = mgr.process(feed.msg(delta("A", 7, Side.YES, "0.40", "1", conn="c1")))
    assert [(u.market_id, u.reason) for u in ups] == [("A", DesyncReason.SEQUENCE_GAP)]
    assert mgr.sync_status("B") is SyncStatus.SYNCHRONIZED
    mgr.process(feed.msg(delta("B", 2, Side.YES, "0.20", "1", conn="c2")))
    assert levels(mgr, "B", Side.YES) == [("0.20", "11")]
    assert mgr.sync_status("B") is SyncStatus.SYNCHRONIZED


def test_duplicate_detection_is_per_connection(mgr: BookManager, feed: Feed) -> None:
    mgr.process(feed.msg(delta("A", 2, Side.YES, "0.40", "1", conn="c1")))
    mgr.process(feed.msg(delta("A", 3, Side.YES, "0.40", "1", conn="c1")))
    # seq 2 is a duplicate on c1, but brand new on c2
    mgr.process(feed.msg(delta("A", 2, Side.YES, "0.40", "1", conn="c1")))
    assert mgr.stats.duplicates_dropped == 1
    ups = mgr.process(feed.msg(delta("B", 2, Side.YES, "0.20", "1", conn="c2")))
    assert [u.kind for u in ups] == ["delta"]
    assert mgr.stats.duplicates_dropped == 1


def test_malformed_message_on_one_connection_only_desyncs_that_connection(
    mgr: BookManager, feed: Feed
) -> None:
    bad = RawWireEvent(
        connection_id="c1",
        payload={"type": "orderbook_delta", "sid": 1, "seq": 9, "msg": {"market_ticker": "A"}},
    )
    ups = mgr.process(feed.msg(bad))
    assert [(u.market_id, u.kind) for u in ups] == [("A", "desync")]
    assert mgr.sync_status("B") is SyncStatus.SYNCHRONIZED
    # c2's sequence was not advanced by c1's malformed seq=9
    ups = mgr.process(feed.msg(delta("B", 2, Side.YES, "0.20", "1", conn="c2")))
    assert [u.kind for u in ups] == ["delta"]
    assert mgr.stats.gaps_detected == 0


def test_delta_on_other_connection_with_same_sid_never_touches_book(
    mgr: BookManager, feed: Feed
) -> None:
    # A lives on c1/sid=1. A delta for A arriving on c2/sid=1 is not A's subscription.
    mgr.process(feed.msg(delta("A", 2, Side.YES, "0.40", "5", conn="c2")))
    assert levels(mgr, "A", Side.YES) == [("0.40", "10")]
    assert mgr.stats.ignored_stale_subscription == 1
    # and it did not consume c1's next sequence number
    ups = mgr.process(feed.msg(delta("A", 2, Side.YES, "0.40", "1", conn="c1")))
    assert [u.kind for u in ups] == ["delta"]


def test_connection_loss_only_affects_its_own_subscriptions(mgr: BookManager) -> None:
    mgr.connection_lost("c1", DesyncReason.CONNECTION_LOST)
    assert mgr.sync_status("A") is SyncStatus.UNSYNCHRONIZED
    assert mgr.sync_status("B") is SyncStatus.SYNCHRONIZED
    assert mgr.book("B").confirmed_through_ms is not None


def test_state_view_lists_subscriptions_by_connection(mgr: BookManager) -> None:
    subs = mgr.state_view()["subscriptions"]
    assert isinstance(subs, dict)
    assert sorted(subs) == ["c1/1", "c2/1"]

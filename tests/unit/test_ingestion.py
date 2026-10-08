"""Phase 4: book manager, queues, runner, data sources (spec section 7, Test H)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal

import pytest

from consistency_connectors.base import (
    ConnectionHealth,
    ConnectionState,
    MarketDataSource,
    RecoveryRequest,
    SourceAuthenticationError,
)
from consistency_connectors.ingestion import (
    Backoff,
    BookManager,
    BookUpdate,
    CoalescingQueue,
    DesyncReason,
    IngestionRunner,
)
from consistency_connectors.settings import DataSourceMode, Settings
from consistency_connectors.sources import (
    KalshiAccessRefusedError,
    KalshiDataSource,
    ReplayDataSource,
    SyntheticDataSource,
)
from consistency_core.events import (
    ConnectionEvent,
    HeartbeatEvent,
    MarketLifecycleEvent,
    MarketStatusEvent,
    OrderBookDeltaEvent,
    OrderBookSnapshotEvent,
    RawWireEvent,
    StreamEvent,
    StreamMessage,
)
from consistency_core.models import DataSourceKind, MarketStatus, Side, SyncStatus
from consistency_core.models.market import Event, Market, MarketRules, Series
from consistency_core.money import dec, dec_str
from consistency_simulation.datasets import PRESETS
from consistency_simulation.exchange import SyntheticExchange
from tests.conftest import FIXTURES
from tests.factories import T0_MS, market

D = dec


class Feed:
    """Builds StreamMessages with monotone positions/timestamps."""

    def __init__(self) -> None:
        self.pos = 0
        self.t = T0_MS

    def msg(self, event: StreamEvent, *, dt: int = 10) -> StreamMessage:
        self.t += dt
        m = StreamMessage(
            position=self.pos, emitted_ts_ms=self.t, received_ts_ms=self.t + 5, event=event
        )
        self.pos += 1
        return m


def snap(mid: str, seq: int, *, sid: str = "1", conn: str = "c1", yes=(), no=()):
    return OrderBookSnapshotEvent(
        market_id=mid,
        sid=sid,
        seq=seq,
        connection_id=conn,
        exchange_ts_ms=T0_MS,
        yes_bids=tuple((D(p), D(q)) for p, q in yes),
        no_bids=tuple((D(p), D(q)) for p, q in no),
    )


def delta(mid: str, seq: int, side: Side, price: str, d: str, *, sid: str = "1", conn: str = "c1"):
    return OrderBookDeltaEvent(
        market_id=mid, sid=sid, seq=seq, connection_id=conn, side=side, price=D(price), delta=D(d)
    )


def levels(mgr: BookManager, mid: str, side: Side) -> list[tuple[str, str]]:
    b = mgr.book(mid)
    lv = b.yes_bids if side is Side.YES else b.no_bids
    return [(dec_str(x.price), dec_str(x.quantity)) for x in lv]


@pytest.fixture
def mgr() -> BookManager:
    return BookManager([market("A"), market("B")], source="unit")


@pytest.fixture
def feed() -> Feed:
    return Feed()


def test_awaiting_snapshot_until_first_snapshot(mgr: BookManager, feed: Feed) -> None:
    assert mgr.sync_status("A") is SyncStatus.AWAITING_SNAPSHOT
    mgr.process(feed.msg(delta("A", 1, Side.YES, "0.40", "5")))  # unknown sid: ignored
    assert mgr.sync_status("A") is SyncStatus.AWAITING_SNAPSHOT
    assert mgr.stats.ignored_stale_subscription == 1
    ups = mgr.process(feed.msg(snap("A", 1, yes=[("0.40", "10")], no=[("0.55", "7")])))
    assert [u.kind for u in ups] == ["snapshot"]
    assert mgr.sync_status("A") is SyncStatus.SYNCHRONIZED
    assert mgr.book("A").best_ask(Side.YES) == D("0.45")  # 1 - 0.55


def test_snapshot_then_deltas_apply_and_remove_levels(mgr: BookManager, feed: Feed) -> None:
    mgr.process(feed.msg(snap("A", 1, yes=[("0.40", "10")], no=[("0.55", "7")])))
    mgr.process(feed.msg(delta("A", 2, Side.YES, "0.41", "3")))
    mgr.process(feed.msg(delta("A", 3, Side.YES, "0.40", "-10")))
    mgr.process(feed.msg(delta("A", 4, Side.NO, "0.55", "2.5")))
    assert levels(mgr, "A", Side.YES) == [("0.41", "3")]
    assert levels(mgr, "A", Side.NO) == [("0.55", "9.5")]
    assert mgr.book("A").source_sequence == 4
    assert mgr.stats.deltas_applied == 3


def test_sequence_is_per_subscription_not_per_market(mgr: BookManager, feed: Feed) -> None:
    """One sid carries A and B; seq increments across both markets."""
    mgr.process(feed.msg(snap("A", 1, yes=[("0.40", "10")])))
    mgr.process(feed.msg(snap("B", 2, yes=[("0.20", "10")])))
    mgr.process(feed.msg(delta("A", 3, Side.YES, "0.40", "1")))
    mgr.process(feed.msg(delta("B", 4, Side.YES, "0.20", "1")))
    assert mgr.stats.gaps_detected == 0
    assert levels(mgr, "A", Side.YES) == [("0.40", "11")]
    assert levels(mgr, "B", Side.YES) == [("0.20", "11")]


def test_duplicate_is_dropped_without_effect(mgr: BookManager, feed: Feed) -> None:
    mgr.process(feed.msg(snap("A", 1, yes=[("0.40", "10")])))
    d = delta("A", 2, Side.YES, "0.40", "5")
    mgr.process(feed.msg(d))
    before = mgr.state_view()["markets"]
    assert mgr.process(feed.msg(d)) == []
    assert mgr.state_view()["markets"] == before
    assert mgr.stats.duplicates_dropped == 1
    assert levels(mgr, "A", Side.YES) == [("0.40", "15")]
    assert mgr.sync_status("A") is SyncStatus.SYNCHRONIZED


def test_H_gap_desyncs_every_market_on_sid_and_snapshot_recovers(
    mgr: BookManager, feed: Feed
) -> None:
    """Test H: a sequence gap must (1) mark books UNSYNCHRONIZED, (2) ignore subsequent deltas,
    (3) request recovery, and (4) only a fresh validated snapshot restores trust."""
    mgr.process(feed.msg(snap("A", 1, yes=[("0.40", "10")], no=[("0.50", "4")])))
    mgr.process(feed.msg(snap("B", 2, yes=[("0.20", "10")])))
    ups = mgr.process(feed.msg(delta("A", 4, Side.YES, "0.40", "1")))  # seq 3 lost
    assert {(u.market_id, u.kind, u.reason) for u in ups} == {
        ("A", "desync", DesyncReason.SEQUENCE_GAP),
        ("B", "desync", DesyncReason.SEQUENCE_GAP),
    }
    assert mgr.sync_status("A") is SyncStatus.UNSYNCHRONIZED
    assert mgr.book("A").sync_status is SyncStatus.UNSYNCHRONIZED
    assert mgr.book("A").confirmed_through_ms is None  # never fresh while untrusted
    # deltas after the gap are not applied
    mgr.process(feed.msg(delta("A", 5, Side.YES, "0.40", "100")))
    assert levels(mgr, "A", Side.YES) == [("0.40", "10")]
    assert mgr.stats.ignored_unsynchronized == 1
    reqs = mgr.drain_recovery_requests()
    assert reqs == [
        RecoveryRequest(connection_id="c1", market_ids=("A", "B"), reason="SEQUENCE_GAP")
    ]
    assert mgr.drain_recovery_requests() == []
    # recovery: resubscribe on a new sid, fresh snapshots
    mgr.process(feed.msg(snap("A", 1, sid="2", yes=[("0.42", "8")], no=[("0.50", "4")])))
    assert mgr.sync_status("A") is SyncStatus.SYNCHRONIZED
    assert mgr.sync_status("B") is SyncStatus.UNSYNCHRONIZED
    assert levels(mgr, "A", Side.YES) == [("0.42", "8")]
    mgr.process(feed.msg(delta("A", 2, Side.YES, "0.42", "1", sid="2")))
    assert levels(mgr, "A", Side.YES) == [("0.42", "9")]
    # late deltas on the old sid for the recovered market are ignored
    mgr.process(feed.msg(delta("A", 6, Side.YES, "0.42", "50")))
    assert levels(mgr, "A", Side.YES) == [("0.42", "9")]
    mgr.process(feed.msg(snap("B", 3, sid="2", yes=[("0.21", "1")])))
    assert mgr.sync_status("B") is SyncStatus.SYNCHRONIZED


def test_negative_quantity_desyncs(mgr: BookManager, feed: Feed) -> None:
    mgr.process(feed.msg(snap("A", 1, yes=[("0.40", "10")])))
    ups = mgr.process(feed.msg(delta("A", 2, Side.YES, "0.40", "-11")))
    assert ups[0].reason == DesyncReason.NEGATIVE_QUANTITY
    assert mgr.sync_status("A") is SyncStatus.UNSYNCHRONIZED
    assert levels(mgr, "A", Side.YES) == [("0.40", "10")]


def test_delta_that_crosses_book_desyncs(mgr: BookManager, feed: Feed) -> None:
    mgr.process(feed.msg(snap("A", 1, yes=[("0.40", "10")], no=[("0.55", "5")])))
    ups = mgr.process(feed.msg(delta("A", 2, Side.YES, "0.45", "1")))  # 0.45 + 0.55 = 1
    assert ups[0].reason == DesyncReason.CROSSED_BOOK
    assert mgr.sync_status("A") is SyncStatus.UNSYNCHRONIZED


def test_crossed_or_offgrid_snapshot_rejected(feed: Feed) -> None:
    from consistency_core.ticks import PriceGrid

    m = BookManager([market("A", grid=PriceGrid.uniform(D("0.01")))], source="unit")
    ups = m.process(feed.msg(snap("A", 1, yes=[("0.60", "1")], no=[("0.40", "1")])))
    assert ups[0].reason == "INVALID_SNAPSHOT:CROSSED_BOOK"
    ups = m.process(feed.msg(snap("A", 2, yes=[("0.405", "1")])))
    assert m.sync_status("A") is SyncStatus.UNSYNCHRONIZED
    assert m.stats.rejected == 2
    assert ups == []  # already unsynchronized: no duplicate desync notification
    m.process(feed.msg(snap("A", 3, yes=[("0.40", "1")])))
    assert m.sync_status("A") is SyncStatus.SYNCHRONIZED


def test_offgrid_delta_desyncs(feed: Feed) -> None:
    from consistency_core.ticks import PriceGrid

    m = BookManager([market("A", grid=PriceGrid.uniform(D("0.01")))], source="unit")
    m.process(feed.msg(snap("A", 1, yes=[("0.40", "1")])))
    ups = m.process(feed.msg(delta("A", 2, Side.YES, "0.405", "1")))
    assert ups[0].reason == "INVALID_DELTA:PRICE_OFF_GRID"


def test_malformed_raw_message_desyncs_its_subscription(mgr: BookManager, feed: Feed) -> None:
    mgr.process(feed.msg(snap("A", 1, yes=[("0.40", "10")])))
    mgr.process(feed.msg(snap("B", 1, sid="9", yes=[("0.20", "10")])))
    bad = RawWireEvent(
        connection_id="c1",
        payload={"type": "orderbook_delta", "sid": 1, "seq": 2, "msg": {"market_ticker": "A"}},
    )
    ups = mgr.process(feed.msg(bad))
    assert [(u.market_id, u.kind) for u in ups] == [("A", "desync")]
    assert ups[0].reason is not None
    assert ups[0].reason.startswith(DesyncReason.MALFORMED_MESSAGE)
    assert mgr.sync_status("B") is SyncStatus.SYNCHRONIZED  # other sid unaffected
    assert mgr.stats.malformed == 1


def test_malformed_without_sid_kills_whole_connection(mgr: BookManager, feed: Feed) -> None:
    mgr.process(feed.msg(snap("A", 1, yes=[("0.40", "10")])))
    mgr.process(feed.msg(snap("B", 1, sid="9", yes=[("0.20", "10")])))
    mgr.process(feed.msg(RawWireEvent(connection_id="c1", payload={"garbage": True})))
    assert mgr.sync_status("A") is SyncStatus.UNSYNCHRONIZED
    assert mgr.sync_status("B") is SyncStatus.UNSYNCHRONIZED


def test_well_formed_raw_message_is_normalized_and_applied(mgr: BookManager, feed: Feed) -> None:
    mgr.process(feed.msg(snap("A", 1, yes=[("0.40", "10")])))
    raw = RawWireEvent(
        connection_id="c1",
        payload={
            "type": "orderbook_delta",
            "sid": 1,
            "seq": 2,
            "msg": {
                "market_ticker": "A",
                "side": "yes",
                "price_dollars": "0.4000",
                "delta_fp": "2.00",
            },
        },
    )
    mgr.process(feed.msg(raw))
    (lv,) = mgr.book("A").yes_bids
    assert (lv.price, lv.quantity) == (D("0.40"), D("12"))


def test_disconnect_desyncs_connection_and_resubscribe_recovers(
    mgr: BookManager, feed: Feed
) -> None:
    mgr.process(feed.msg(snap("A", 1, yes=[("0.40", "10")])))
    mgr.process(feed.msg(snap("B", 1, sid="5", conn="c2", yes=[("0.20", "10")])))
    ups = mgr.process(feed.msg(ConnectionEvent(state="disconnected", connection_id="c1")))
    assert [(u.market_id, u.reason) for u in ups] == [("A", DesyncReason.CONNECTION_LOST)]
    assert mgr.sync_status("B") is SyncStatus.SYNCHRONIZED
    mgr.process(feed.msg(ConnectionEvent(state="connected", connection_id="c3")))
    mgr.process(feed.msg(snap("A", 1, sid="7", conn="c3", yes=[("0.41", "1")])))
    assert mgr.sync_status("A") is SyncStatus.SYNCHRONIZED
    assert mgr.book("A").connection_id == "c3"


def test_freshness_confirmation_advances_with_heartbeats(mgr: BookManager, feed: Feed) -> None:
    mgr.process(feed.msg(snap("A", 1, yes=[("0.40", "10")])))
    first = mgr.book("A").confirmed_through_ms
    hb = feed.msg(HeartbeatEvent(connection_id="c1"), dt=1000)
    mgr.process(hb)
    assert first is not None
    assert mgr.book("A").confirmed_through_ms == hb.emitted_ts_ms == first + 1000
    assert mgr.book("A").observed_ts_ms == hb.emitted_ts_ms
    # a heartbeat on another connection does not confirm this book
    mgr.process(feed.msg(HeartbeatEvent(connection_id="other"), dt=1000))
    assert mgr.book("A").confirmed_through_ms == hb.emitted_ts_ms


def test_status_and_lifecycle(mgr: BookManager, feed: Feed) -> None:
    mgr.process(feed.msg(MarketStatusEvent(market_id="A", status=MarketStatus.PAUSED)))
    assert mgr.market_status("A") is MarketStatus.PAUSED
    mgr.process(feed.msg(MarketLifecycleEvent(action="created", market_id="N", market=market("N"))))
    assert mgr.has_market("N")
    mgr.process(feed.msg(snap("N", 1, yes=[("0.10", "1")])))
    assert mgr.sync_status("N") is SyncStatus.SYNCHRONIZED
    mgr.process(feed.msg(MarketLifecycleEvent(action="removed", market_id="N")))
    assert not mgr.has_market("N")
    mgr.process(feed.msg(delta("N", 2, Side.YES, "0.10", "1")))
    assert mgr.stats.ignored_unknown_market == 1


def test_timing_metadata_and_injected_clock(feed: Feed) -> None:
    ticks = iter(range(0, 10_000, 100))
    m = BookManager([market("A")], source="unit", clock_ns=lambda: next(ticks))
    msg = feed.msg(snap("A", 1, yes=[("0.40", "1")]))
    (u,) = m.process(msg)
    assert u.timing.received_ts_ms == msg.received_ts_ms
    assert u.timing.exchange_ts_ms == T0_MS
    assert u.timing.internal_latency_ns == 100


# ---------------------------------------------------------------- replay of recorded datasets


def _replay(messages: Sequence[StreamMessage], catalog_markets: Sequence[Market]) -> BookManager:
    mgr = BookManager(catalog_markets, source="replay")
    for msg in messages:
        mgr.process(msg)
    return mgr


@pytest.mark.parametrize("preset", ["smoke", "corruption"])
def test_replay_reconstructs_exchange_truth(preset: str) -> None:
    """After every fault (gap, duplicate, reorder, malformed, disconnect, lag, pause, lifecycle)
    plus the in-stream recovery, every live market is SYNCHRONIZED and the local book equals
    the exchange's final book exactly."""
    result = SyntheticExchange(PRESETS[preset]).run()
    mgr = _replay(result.messages, result.catalog.markets)
    assert set(mgr.market_ids) == set(result.final_books)
    for mid, truth in result.final_books.items():
        assert mgr.sync_status(mid) is SyncStatus.SYNCHRONIZED, mid
        assert [list(x) for x in levels(mgr, mid, Side.YES)] == truth["yes"], mid
        assert [list(x) for x in levels(mgr, mid, Side.NO)] == truth["no"], mid
    if preset == "corruption":
        s = mgr.stats
        assert s.gaps_detected > 0
        assert s.duplicates_dropped > 0
        assert s.malformed > 0
        assert s.connection_losses > 0


def test_replay_is_deterministic_state_digest() -> None:
    result = SyntheticExchange(PRESETS["smoke"]).run()
    a = _replay(result.messages, result.catalog.markets).state_digest()
    b = _replay(result.messages, result.catalog.markets).state_digest()
    assert a == b


def test_bundled_smoke_dataset_replays_to_metadata_final_books() -> None:
    src = ReplayDataSource(FIXTURES / "datasets" / "smoke")
    mgr = _replay(src.messages, src.catalog.markets)
    final = src.metadata["final_books"]
    assert isinstance(final, dict)
    for mid, truth in final.items():
        assert mgr.sync_status(mid) is SyncStatus.SYNCHRONIZED
        assert [list(x) for x in levels(mgr, mid, Side.YES)] == truth["yes"]


# ---------------------------------------------------------------- queues / backoff / runner


async def test_coalescing_queue_keeps_latest_and_is_bounded() -> None:
    q: CoalescingQueue[int] = CoalescingQueue(maxsize=2)
    await q.put("A", 1)
    await q.put("B", 1)
    await q.put("A", 2)  # coalesces, does not block
    assert len(q) == 2
    assert q.stats.coalesced == 1
    blocked = asyncio.create_task(q.put("C", 1))
    await asyncio.sleep(0)
    assert not blocked.done()  # bounded: waits for space
    assert await q.get() == ("A", 2)
    await asyncio.wait_for(blocked, 1)
    assert q.get_nowait() == ("B", 1)
    assert q.get_nowait() == ("C", 1)
    assert q.get_nowait() is None
    assert q.stats.high_water == 2
    with pytest.raises(ValueError, match="maxsize"):
        CoalescingQueue[int](0)


def test_backoff_is_bounded_and_deterministic() -> None:
    b = Backoff(base_s=0.5, factor=2, max_s=4, max_attempts=6, jitter=0.2, seed=7)
    ds = b.delays()
    assert ds == Backoff(base_s=0.5, factor=2, max_s=4, max_attempts=6, jitter=0.2, seed=7).delays()
    assert len(ds) == 6
    nominal = [0.5, 1, 2, 4, 4, 4]
    for d, n in zip(ds, nominal, strict=True):
        assert n * 0.8 <= d <= n * 1.2


async def test_runner_end_to_end_with_synthetic_source() -> None:
    src = SyntheticDataSource("smoke")
    mgr = BookManager(src.catalog.markets, source="synthetic")
    updates: CoalescingQueue[BookUpdate] = CoalescingQueue(maxsize=10_000)
    runner = IngestionRunner(src, mgr, updates, inbound_maxsize=64, heartbeat_timeout_s=None)
    await runner.run()
    assert runner.failed is None
    assert runner.stats.messages == len(src.messages)
    assert runner.stats.inbound_high_water <= 64
    assert runner.stats.recovery_requests >= 1  # the smoke gap
    assert src.recovery_requests
    for mid, truth in src.result.final_books.items():
        assert mgr.sync_status(mid) is SyncStatus.SYNCHRONIZED
        assert [list(x) for x in levels(mgr, mid, Side.NO)] == truth["no"]
    health = src.get_connection_health()
    assert health.messages_delivered == len(src.messages)
    assert health.source_kind is DataSourceKind.SYNTHETIC


async def test_synthetic_rest_orderbook_reflects_truth_at_position() -> None:
    src = SyntheticDataSource("smoke")
    mid = src.catalog.markets[0].market_id
    src.position = len(src.messages) - 1
    rest = await src.get_market_orderbook(mid)
    assert rest.sid == "rest"
    truth = src.result.final_books[mid]
    assert [[dec_str(p), dec_str(q)] for p, q in rest.yes_bids] == truth["yes"]
    assert (await src.get_market_rules(mid)).market_id == mid
    assert len(await src.get_markets()) == len(src.catalog.markets)


class _FlakySource(MarketDataSource):
    """Fails with a connection error once, then serves one snapshot."""

    kind = DataSourceKind.SYNTHETIC
    stream_is_finite = True  # a scripted run: the final stream end is expected

    def __init__(self, fail_with: Exception) -> None:
        self.calls = 0
        self.fail_with = fail_with

    async def get_series(self) -> Sequence[Series]:
        return ()

    async def get_events(self) -> Sequence[Event]:
        return ()

    async def get_markets(self) -> Sequence[Market]:
        return ()

    async def get_market_rules(self, market_id: str) -> MarketRules:
        raise NotImplementedError

    async def get_market_orderbook(self, market_id: str) -> OrderBookSnapshotEvent:
        raise NotImplementedError

    async def subscribe_orderbooks(
        self, market_ids: Sequence[str] | None = None
    ) -> AsyncIterator[StreamMessage]:
        self.calls += 1
        feed = Feed()
        feed.pos = self.calls * 10
        yield feed.msg(snap("A", 1, sid=str(self.calls), conn=f"c{self.calls}", yes=[("0.4", "1")]))
        if self.calls == 1:
            raise self.fail_with

    async def unsubscribe_orderbooks(self, market_ids: Sequence[str]) -> None:
        return None

    async def request_recovery(self, request: RecoveryRequest) -> None:
        return None

    def get_connection_health(self) -> ConnectionHealth:
        return ConnectionHealth(source_kind=self.kind, state=ConnectionState.CONNECTED)


async def test_runner_reconnects_after_connection_error_and_desyncs_meanwhile() -> None:
    slept: list[float] = []

    async def fake_sleep(s: float) -> None:
        slept.append(s)

    src = _FlakySource(ConnectionError("reset"))
    mgr = BookManager([market("A")], source="flaky")
    updates: CoalescingQueue[BookUpdate] = CoalescingQueue(maxsize=100)
    runner = IngestionRunner(
        src, mgr, updates, heartbeat_timeout_s=None, backoff=Backoff(seed=1), sleep=fake_sleep
    )
    await runner.run()
    assert src.calls == 2
    assert runner.stats.reconnect_attempts == 1
    assert len(slept) == 1
    assert mgr.stats.connection_losses == 1
    assert mgr.sync_status("A") is SyncStatus.SYNCHRONIZED  # recovered on new connection
    assert mgr.book("A").connection_id == "c2"


async def test_runner_does_not_retry_authentication_errors() -> None:
    src = _FlakySource(SourceAuthenticationError("bad key"))
    mgr = BookManager([market("A")], source="flaky")
    runner = IngestionRunner(
        src, mgr, CoalescingQueue(maxsize=10), heartbeat_timeout_s=None, sleep=asyncio.sleep
    )
    await runner.run()
    assert src.calls == 1
    assert runner.failed is not None
    assert runner.failed.startswith("authentication")


class _SilentSource(_FlakySource):
    async def subscribe_orderbooks(
        self, market_ids: Sequence[str] | None = None
    ) -> AsyncIterator[StreamMessage]:
        self.calls += 1
        yield Feed().msg(snap("A", 1, sid=str(self.calls), conn="c1", yes=[("0.4", "1")]))
        await asyncio.sleep(3600)


async def test_heartbeat_timeout_marks_books_unsynchronized() -> None:
    async def no_sleep(_: float) -> None:
        return None

    src = _SilentSource(ConnectionError())
    mgr = BookManager([market("A")], source="silent")
    runner = IngestionRunner(
        src,
        mgr,
        CoalescingQueue(maxsize=10),
        heartbeat_timeout_s=0.01,
        backoff=Backoff(max_attempts=1, base_s=0),
        sleep=no_sleep,
    )
    await asyncio.wait_for(runner.run(), 5)
    assert runner.stats.heartbeat_timeouts == 2
    assert runner.failed == "reconnect attempts exhausted"
    assert mgr.sync_status("A") is SyncStatus.UNSYNCHRONIZED
    assert mgr.stats.connection_losses == 2


# ---------------------------------------------------------------- Kalshi guard


@pytest.mark.parametrize(("enable", "confirmed"), [(False, False), (True, False), (False, True)])
def test_kalshi_source_refuses_without_both_flags(enable: bool, confirmed: bool) -> None:
    s = Settings(ENABLE_KALSHI_API=enable, KALSHI_AUTHORIZATION_CONFIRMED=confirmed)
    with pytest.raises(KalshiAccessRefusedError):
        KalshiDataSource(s)


async def test_kalshi_skeleton_with_flags_still_performs_no_access() -> None:
    s = Settings(
        DATA_SOURCE=DataSourceMode.KALSHI_AUTHORIZED,
        ENABLE_KALSHI_API=True,
        KALSHI_AUTHORIZATION_CONFIRMED=True,
    )
    src = KalshiDataSource(s)
    assert src.get_connection_health().state is ConnectionState.DISABLED
    with pytest.raises(NotImplementedError):
        await src.get_markets()
    with pytest.raises(NotImplementedError):
        src.subscribe_orderbooks(["X"])


def test_kalshi_module_imports_no_network_client() -> None:
    from pathlib import Path

    import consistency_connectors.sources.kalshi as k

    text = Path(k.__file__).read_text(encoding="utf-8")
    for banned in ("httpx", "requests", "aiohttp", "websockets", "urllib", "socket"):
        assert f"import {banned}" not in text
        assert f"from {banned}" not in text


def test_decimal_only_in_books(mgr: BookManager, feed: Feed) -> None:
    mgr.process(feed.msg(snap("A", 1, yes=[("0.40", "10")])))
    lv = mgr.book("A").yes_bids[0]
    assert isinstance(lv.price, Decimal)
    assert isinstance(lv.quantity, Decimal)

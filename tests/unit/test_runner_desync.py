"""Connection failures are published downstream as explicit UNSYNCHRONIZED updates, without any
further market-data message (hardening pass, P1)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence

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
from consistency_core.events import OrderBookSnapshotEvent, StreamMessage
from consistency_core.models import DataSourceKind, Market, MarketRules, SyncStatus
from consistency_core.models.market import Event, Series
from tests.factories import market
from tests.unit.test_ingestion import Feed, snap

MARKETS = ("A", "B")


class _ScriptedSource(MarketDataSource):
    """Live-style source (stream end is unexpected). Each subscription serves synchronized
    snapshots for every market on one connection, then ends / raises / goes silent."""

    kind = DataSourceKind.SYNTHETIC

    def __init__(self, ending: str | BaseException, *, repeat_failure: bool = True) -> None:
        self.ending = ending
        self.repeat_failure = repeat_failure
        self.calls = 0

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
        feed.pos = self.calls * 100
        conn = f"c{self.calls}"
        for i, mid in enumerate(MARKETS):
            yield feed.msg(snap(mid, i + 1, sid=f"s{self.calls}", conn=conn, yes=[("0.4", "1")]))
        if self.calls > 1 and not self.repeat_failure:
            return
        if self.ending == "silent":
            await asyncio.sleep(3600)
        elif isinstance(self.ending, BaseException):
            raise self.ending

    async def unsubscribe_orderbooks(self, market_ids: Sequence[str]) -> None:
        return None

    async def request_recovery(self, request: RecoveryRequest) -> None:
        return None

    def get_connection_health(self) -> ConnectionHealth:
        return ConnectionHealth(source_kind=self.kind, state=ConnectionState.CONNECTED)


async def _no_sleep(_: float) -> None:
    return None


def _drain(q: CoalescingQueue[BookUpdate]) -> dict[str, BookUpdate]:
    out: dict[str, BookUpdate] = {}
    while (item := q.get_nowait()) is not None:
        out[item[0]] = item[1]
    return out


async def _run(
    src: MarketDataSource, **kw: object
) -> tuple[IngestionRunner, dict[str, BookUpdate]]:
    mgr = BookManager([market(m) for m in MARKETS], source="scripted")
    q: CoalescingQueue[BookUpdate] = CoalescingQueue(maxsize=100)
    runner = IngestionRunner(src, mgr, q, sleep=_no_sleep, **kw)  # type: ignore[arg-type]
    await asyncio.wait_for(runner.run(), 5)
    return runner, _drain(q)


def _assert_desynced(updates: dict[str, BookUpdate], reason: str) -> None:
    assert set(updates) == set(MARKETS)
    for mid in MARKETS:
        u = updates[mid]
        assert u.sync_status is SyncStatus.UNSYNCHRONIZED, (mid, u)
        assert u.kind == "desync"
        assert u.reason == reason


async def test_unexpected_end_of_stream_publishes_desync() -> None:
    runner, updates = await _run(_ScriptedSource("eos"), heartbeat_timeout_s=None)
    assert runner.failed == "unexpected end of stream"
    _assert_desynced(updates, DesyncReason.END_OF_STREAM)


async def test_authentication_failure_publishes_desync() -> None:
    runner, updates = await _run(
        _ScriptedSource(SourceAuthenticationError("revoked")), heartbeat_timeout_s=None
    )
    assert runner.failed is not None and runner.failed.startswith("authentication")
    _assert_desynced(updates, DesyncReason.AUTHENTICATION_FAILED)


async def test_exhausted_reconnects_publish_desync() -> None:
    runner, updates = await _run(
        _ScriptedSource(ConnectionError("reset")),
        heartbeat_timeout_s=None,
        backoff=Backoff(max_attempts=2, base_s=0),
    )
    assert runner.failed == "reconnect attempts exhausted"
    _assert_desynced(updates, DesyncReason.RECONNECT_EXHAUSTED)


async def test_heartbeat_timeout_without_retries_left_publishes_desync() -> None:
    runner, updates = await _run(
        _ScriptedSource("silent"), heartbeat_timeout_s=0.01, backoff=Backoff(max_attempts=0)
    )
    assert runner.stats.heartbeat_timeouts == 1
    _assert_desynced(updates, DesyncReason.RECONNECT_EXHAUSTED)


async def test_heartbeat_timeout_desync_is_published_before_reconnect() -> None:
    """A consumer draining concurrently sees HEARTBEAT_TIMEOUT before any new snapshot."""
    mgr = BookManager([market(m) for m in MARKETS], source="scripted")
    q: CoalescingQueue[BookUpdate] = CoalescingQueue(maxsize=100)
    gate = asyncio.Event()

    async def held_sleep(_: float) -> None:
        await gate.wait()

    runner = IngestionRunner(
        _ScriptedSource("silent", repeat_failure=False),
        mgr,
        q,
        heartbeat_timeout_s=0.01,
        backoff=Backoff(max_attempts=3, base_s=0),
        sleep=held_sleep,
    )
    task = asyncio.create_task(runner.run())
    seen: dict[str, BookUpdate] = {}
    for _ in range(200):
        await asyncio.sleep(0.005)
        seen |= _drain(q)
        if all(seen.get(m) and seen[m].kind == "desync" for m in MARKETS):
            break
    _assert_desynced(seen, DesyncReason.HEARTBEAT_TIMEOUT)
    gate.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_connection_error_publishes_desync_then_resync_is_marked_interrupted() -> None:
    runner, updates = await _run(
        _ScriptedSource(ConnectionError("reset"), repeat_failure=False),
        heartbeat_timeout_s=None,
        backoff=Backoff(max_attempts=3, base_s=0),
    )
    assert runner.stats.reconnect_attempts == 1
    for mid in MARKETS:
        u = updates[mid]
        # coalesced: the newest state (resynced on c2) wins, but the outage stays visible
        assert u.sync_status is SyncStatus.UNSYNCHRONIZED or u.interrupted
        assert u.interrupted


async def test_unexpected_source_exception_publishes_desync_and_stops() -> None:
    runner, updates = await _run(_ScriptedSource(RuntimeError("bug")), heartbeat_timeout_s=None)
    assert runner.failed is not None and "RuntimeError" in runner.failed
    _assert_desynced(updates, DesyncReason.SOURCE_ERROR)


async def test_runner_cancellation_publishes_desync() -> None:
    mgr = BookManager([market(m) for m in MARKETS], source="scripted")
    q: CoalescingQueue[BookUpdate] = CoalescingQueue(maxsize=100)
    runner = IngestionRunner(_ScriptedSource("silent"), mgr, q, heartbeat_timeout_s=None)
    task = asyncio.create_task(runner.run())
    for _ in range(100):
        await asyncio.sleep(0.005)
        if all(mgr.sync_status(m) is SyncStatus.SYNCHRONIZED for m in MARKETS):
            break
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    _assert_desynced(_drain(q), DesyncReason.RUNNER_STOPPED)


# ---------------------------------------------------------------- queue semantics
def _update(mid: str, sync: SyncStatus, kind: str) -> BookUpdate:
    from consistency_connectors.ingestion.book_manager import Timing

    return BookUpdate(
        market_id=mid,
        position=0,
        kind=kind,
        sync_status=sync,
        reason=None,
        connection_id="c",
        subscription_id="s",
        source_sequence=1,
        timing=Timing(None, 0, 0, 0),
    )


async def test_desync_cannot_be_coalesced_away() -> None:
    from consistency_connectors.ingestion import merge_book_updates

    q: CoalescingQueue[BookUpdate] = CoalescingQueue(maxsize=10, merge=merge_book_updates)
    await q.put("A", _update("A", SyncStatus.SYNCHRONIZED, "delta"))
    await q.put("A", _update("A", SyncStatus.UNSYNCHRONIZED, "desync"))
    await q.put("A", _update("A", SyncStatus.SYNCHRONIZED, "snapshot"))
    got = q.get_nowait()
    assert got is not None
    assert got[1].kind == "snapshot"
    assert got[1].interrupted


async def test_urgent_put_never_blocks_on_a_full_queue() -> None:
    q: CoalescingQueue[BookUpdate] = CoalescingQueue(maxsize=1)
    await q.put("A", _update("A", SyncStatus.SYNCHRONIZED, "delta"))
    q.put_urgent("B", _update("B", SyncStatus.UNSYNCHRONIZED, "desync"))
    assert len(q) == 2
    assert q.stats.urgent_overflow == 1


async def test_expected_end_of_finite_recording_does_not_desync() -> None:
    class _Finite(_ScriptedSource):
        stream_is_finite = True

    runner, updates = await _run(_Finite("eos"), heartbeat_timeout_s=None)
    assert runner.failed is None
    assert all(u.sync_status is SyncStatus.SYNCHRONIZED for u in updates.values())

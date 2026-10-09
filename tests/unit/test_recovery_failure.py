"""``request_recovery`` failures fail closed (hardening P2, before Phase 7).

When the source cannot honour a recovery request (it raises, or never answers), the books it was
meant to restore can never become trustworthy again on this connection. The runner must:

* leave/make every affected book UNSYNCHRONIZED,
* publish explicit desync notifications for every affected market through the non-droppable
  urgent path (``CoalescingQueue.put_urgent``),
* stop in FAILED state without hanging and without processing later messages as if recovered.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence

import pytest

from consistency_connectors.base import (
    ConnectionHealth,
    ConnectionState,
    MarketDataSource,
    RecoveryRequest,
)
from consistency_connectors.ingestion import (
    BookManager,
    BookUpdate,
    CoalescingQueue,
    DesyncReason,
    IngestionRunner,
)
from consistency_core.events import OrderBookSnapshotEvent, StreamMessage
from consistency_core.models import DataSourceKind, Market, MarketRules, Side, SyncStatus
from consistency_core.models.market import Event, Series
from tests.factories import market
from tests.unit.test_ingestion import Feed, delta, snap

MARKETS = ("A", "B", "C")


class _FailingRecoverySource(MarketDataSource):
    """A and B share subscription s1, C is on s2, all on connection c1. A sequence gap on s1
    triggers a recovery request; ``request_recovery`` then raises or hangs. A fresh snapshot
    for A follows the gap and must never be applied."""

    kind = DataSourceKind.SYNTHETIC

    def __init__(self, failure: BaseException | str) -> None:
        self.failure = failure
        self.recovery_calls: list[RecoveryRequest] = []

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
        feed = Feed()
        yield feed.msg(snap("A", 1, sid="s1", yes=[("0.40", "5")]))
        yield feed.msg(snap("B", 2, sid="s1", yes=[("0.40", "5")]))
        yield feed.msg(snap("C", 1, sid="s2", yes=[("0.40", "5")]))
        yield feed.msg(delta("A", 4, Side.YES, "0.41", "1", sid="s1"))  # seq 3 lost -> gap
        yield feed.msg(snap("A", 1, sid="s3", yes=[("0.30", "9")]))  # must not be applied
        await asyncio.sleep(3600)

    async def unsubscribe_orderbooks(self, market_ids: Sequence[str]) -> None:
        return None

    async def request_recovery(self, request: RecoveryRequest) -> None:
        self.recovery_calls.append(request)
        if self.failure == "hang":
            await asyncio.sleep(3600)
        elif isinstance(self.failure, BaseException):
            raise self.failure

    def get_connection_health(self) -> ConnectionHealth:
        return ConnectionHealth(source_kind=self.kind, state=ConnectionState.CONNECTED)


def _drain(q: CoalescingQueue[BookUpdate]) -> dict[str, BookUpdate]:
    out: dict[str, BookUpdate] = {}
    while (item := q.get_nowait()) is not None:
        out[item[0]] = item[1]
    return out


async def _run(
    failure: BaseException | str, **kw: object
) -> tuple[IngestionRunner, BookManager, CoalescingQueue[BookUpdate], _FailingRecoverySource]:
    src = _FailingRecoverySource(failure)
    mgr = BookManager([market(m) for m in MARKETS], source="scripted")
    q: CoalescingQueue[BookUpdate] = CoalescingQueue(maxsize=100)
    runner = IngestionRunner(src, mgr, q, heartbeat_timeout_s=None, **kw)  # type: ignore[arg-type]
    await asyncio.wait_for(runner.run(), 5)
    return runner, mgr, q, src


def _assert_failed_closed(
    runner: IngestionRunner, mgr: BookManager, q: CoalescingQueue[BookUpdate]
) -> None:
    assert runner.failed is not None and runner.failed.startswith("recovery failed"), runner.failed
    for mid in MARKETS:
        assert mgr.sync_status(mid) is SyncStatus.UNSYNCHRONIZED, mid
    # the post-gap snapshot was never applied
    assert mgr.book("A").subscription_id == "s1"
    urgent_before = q.stats.urgent
    updates = _drain(q)
    assert set(updates) == set(MARKETS)
    for mid in MARKETS:
        u = updates[mid]
        assert u.sync_status is SyncStatus.UNSYNCHRONIZED, (mid, u)
        assert u.kind == "desync"
        assert u.reason == DesyncReason.RECOVERY_FAILED, (mid, u.reason)
    assert urgent_before >= len(MARKETS)


@pytest.mark.parametrize(
    "failure", [RuntimeError("recovery endpoint bug"), ValueError("bad request")]
)
async def test_recovery_exception_fails_closed(failure: BaseException) -> None:
    runner, mgr, q, src = await _run(failure)
    assert len(src.recovery_calls) == 1
    assert src.recovery_calls[0].market_ids == ("A", "B")
    _assert_failed_closed(runner, mgr, q)


async def test_recovery_that_never_returns_times_out_and_fails_closed() -> None:
    runner, mgr, q, _ = await _run("hang", recovery_timeout_s=0.05)
    _assert_failed_closed(runner, mgr, q)


async def test_recovery_failure_is_counted() -> None:
    runner, _, _, _ = await _run(RuntimeError("x"))
    assert runner.stats.recovery_failures == 1
    assert "RuntimeError" in runner.stats.errors

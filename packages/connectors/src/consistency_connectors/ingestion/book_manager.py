"""Local order-book reconstruction with explicit trust tracking (spec 4.2 - 4.5).

Rules implemented here:

* A market is SYNCHRONIZED only after a validated snapshot.
* Sequence numbers are tracked **per subscription**, identified by ``(connection_id, sid)``
  (sids are only unique within a connection). A gap desynchronizes every market carried by that
  subscription, because we cannot know which market's update was lost.
* ``seq <= last`` is a duplicate/late message and is dropped without effect.
* Any malformed message, negative resulting quantity, off-grid price, crossed book, or lost
  connection marks the affected books UNSYNCHRONIZED and queues a recovery request. While
  UNSYNCHRONIZED, deltas are ignored; only a fresh snapshot restores trust.
* Freshness: each connection's ``confirmed_through`` is the newest emission time of an
  *accepted* message on it: an applied snapshot or delta, or a heartbeat. Proof: messages on one
  connection arrive in emission order, and every earlier message on it was either applied or
  desynchronized its book, so every book still SYNCHRONIZED on that connection is complete as of
  that instant (quiet markets stay fresh via heartbeats). Malformed, duplicate, gapped, rejected,
  stale-subscription and ignored messages never advance it: their timestamps are untrusted.
* Future-dated timestamps: a message whose emission time or exchange timestamp exceeds its local
  receipt time by more than ``max_future_ms`` is quarantined: it never advances
  ``confirmed_through``; a snapshot/delta is not applied (its sequence number is still consumed)
  and its market is desynchronized with ``FUTURE_TIMESTAMP``.

Timing telemetry (``perf_counter_ns``) is recorded on each update but excluded from
:meth:`state_digest`, so replays compare equal while wall-clock latency may differ.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from pydantic import ValidationError

from consistency_connectors.base import RecoveryRequest
from consistency_core.events import (
    ConnectionEvent,
    HeartbeatEvent,
    MarketLifecycleEvent,
    MarketStatusEvent,
    OrderBookDeltaEvent,
    OrderBookSnapshotEvent,
    RawWireEvent,
    StreamMessage,
)
from consistency_core.models.common import HealthStatus, MarketStatus, Side, SyncStatus
from consistency_core.models.market import Market
from consistency_core.models.orderbook import (
    BookErrorCode,
    BookValidationError,
    OrderBook,
    PriceLevel,
)
from consistency_core.money import ONE, ZERO, dec_str
from consistency_core.normalization import normalize_ws_message, validate_side
from consistency_core.serialization import canonical_json, sha256_of


class DesyncReason:
    SEQUENCE_GAP = "SEQUENCE_GAP"
    MALFORMED_MESSAGE = "MALFORMED_MESSAGE"
    INVALID_SNAPSHOT = "INVALID_SNAPSHOT"
    INVALID_DELTA = "INVALID_DELTA"
    NEGATIVE_QUANTITY = "NEGATIVE_QUANTITY"
    CROSSED_BOOK = "CROSSED_BOOK"
    CONNECTION_LOST = "CONNECTION_LOST"
    HEARTBEAT_TIMEOUT = "HEARTBEAT_TIMEOUT"
    END_OF_STREAM = "END_OF_STREAM"
    AUTHENTICATION_FAILED = "AUTHENTICATION_FAILED"
    RECONNECT_EXHAUSTED = "RECONNECT_EXHAUSTED"
    SOURCE_ERROR = "SOURCE_ERROR"
    RUNNER_STOPPED = "RUNNER_STOPPED"
    FUTURE_TIMESTAMP = "FUTURE_TIMESTAMP"
    RECOVERY_FAILED = "RECOVERY_FAILED"


DEFAULT_MAX_FUTURE_MS = 1_000


@dataclass(frozen=True)
class Timing:
    exchange_ts_ms: int | None
    received_ts_ms: int
    processing_started_ns: int
    processing_completed_ns: int

    @property
    def internal_latency_ns(self) -> int:
        """Latency attributable to this application (excludes network/exchange latency)."""
        return self.processing_completed_ns - self.processing_started_ns


@dataclass(frozen=True)
class BookUpdate:
    """Published to the detection engine after every state-relevant change."""

    market_id: str
    position: int
    kind: str  # "snapshot" | "delta" | "desync" | "status" | "removed"
    sync_status: SyncStatus
    reason: str | None
    connection_id: str | None
    subscription_id: str | None
    source_sequence: int | None
    timing: Timing
    interrupted: bool = False
    """True when this update superseded a pending, unconsumed desync for the market (see
    :func:`consistency_connectors.ingestion.queues.merge_book_updates`)."""


@dataclass
class _MarketState:
    market: Market
    status: MarketStatus
    sync: SyncStatus = SyncStatus.AWAITING_SNAPSHOT
    reason: str | None = None
    sid: str | None = None
    connection_id: str | None = None
    yes: dict[Decimal, Decimal] = field(default_factory=dict)
    no: dict[Decimal, Decimal] = field(default_factory=dict)
    last_seq: int | None = None
    exchange_ts_ms: int | None = None
    received_ts_ms: int | None = None
    last_sync_ts_ms: int | None = None

    def levels(self, side: Side) -> dict[Decimal, Decimal]:
        return self.yes if side is Side.YES else self.no


type _SubKey = tuple[str, str]  # (connection_id, sid)


def _key_str(key: _SubKey) -> str:
    return f"{key[0]}/{key[1]}"


def _market_key(st: _MarketState) -> _SubKey | None:
    if st.connection_id is None or st.sid is None:
        return None
    return (st.connection_id, st.sid)


@dataclass
class _Subscription:
    sid: str
    connection_id: str
    last_seq: int
    markets: set[str] = field(default_factory=set)


@dataclass
class ManagerStats:
    messages: int = 0
    snapshots_applied: int = 0
    deltas_applied: int = 0
    duplicates_dropped: int = 0
    gaps_detected: int = 0
    malformed: int = 0
    rejected: int = 0
    ignored_unsynchronized: int = 0
    ignored_unknown_market: int = 0
    ignored_stale_subscription: int = 0
    desync_events: int = 0
    connection_losses: int = 0
    future_timestamps: int = 0

    def as_dict(self) -> dict[str, int]:
        return dict(sorted(self.__dict__.items()))


class BookManager:
    def __init__(
        self,
        markets: Iterable[Market],
        *,
        source: str,
        clock_ns: Callable[[], int] = time.perf_counter_ns,
        max_future_ms: int = DEFAULT_MAX_FUTURE_MS,
    ) -> None:
        if max_future_ms < 0:
            raise ValueError("max_future_ms must be >= 0")
        self.source = source
        self._clock_ns = clock_ns
        self.max_future_ms = max_future_ms
        self._accepted = False
        self._markets: dict[str, _MarketState] = {
            m.market_id: _MarketState(market=m, status=m.status) for m in markets
        }
        self._subs: dict[_SubKey, _Subscription] = {}
        """Keyed by (connection_id, sid): sids are only unique within one connection, so
        sequence history, gap/duplicate detection and malformed-message recovery are per key."""
        self._confirmed: dict[str, int] = {}
        self._recovery: dict[str | None, dict[str, str]] = {}
        self.stats = ManagerStats()
        self.last_position: int = -1
        self.last_received_ts_ms: int | None = None

    # ------------------------------------------------------------------ public read API
    @property
    def market_ids(self) -> list[str]:
        return sorted(self._markets)

    def sync_status(self, market_id: str) -> SyncStatus:
        return self._markets[market_id].sync

    def market_status(self, market_id: str) -> MarketStatus:
        return self._markets[market_id].status

    def has_market(self, market_id: str) -> bool:
        return market_id in self._markets

    def market(self, market_id: str) -> Market:
        return self._markets[market_id].market

    def book(self, market_id: str) -> OrderBook:
        st = self._markets[market_id]
        confirmed = None
        if st.sync is SyncStatus.SYNCHRONIZED and st.connection_id is not None:
            confirmed = self._confirmed.get(st.connection_id)
        health = (
            HealthStatus.HEALTHY if st.sync is SyncStatus.SYNCHRONIZED else (HealthStatus.UNHEALTHY)
        )
        return OrderBook.model_construct(
            market_id=market_id,
            source=self.source,
            source_sequence=st.last_seq,
            connection_id=st.connection_id,
            subscription_id=st.sid,
            exchange_ts_ms=st.exchange_ts_ms,
            received_ts_ms=st.received_ts_ms if st.received_ts_ms is not None else 0,
            last_sync_ts_ms=st.last_sync_ts_ms,
            confirmed_through_ms=confirmed,
            yes_bids=_levels(Side.YES, st.yes),
            no_bids=_levels(Side.NO, st.no),
            sync_status=st.sync,
            health_status=health,
        )

    def drain_recovery_requests(self) -> list[RecoveryRequest]:
        out = [
            RecoveryRequest(
                connection_id=conn,
                market_ids=tuple(sorted(markets)),
                reason=",".join(sorted(set(markets.values()))),
            )
            for conn, markets in sorted(self._recovery.items(), key=lambda kv: str(kv[0]))
        ]
        self._recovery.clear()
        return out

    def state_digest(self) -> str:
        """Deterministic hash of all book state + sync metadata + counters (no telemetry)."""
        return sha256_of(self.state_view())

    def state_view(self) -> dict[str, object]:
        return {
            "markets": {
                mid: {
                    "sync": st.sync.value,
                    "reason": st.reason,
                    "status": st.status.value,
                    "sid": st.sid,
                    "seq": st.last_seq,
                    "yes": [[dec_str(p), dec_str(q)] for p, q in sorted(st.yes.items())],
                    "no": [[dec_str(p), dec_str(q)] for p, q in sorted(st.no.items())],
                    "exchange_ts_ms": st.exchange_ts_ms,
                    "last_sync_ts_ms": st.last_sync_ts_ms,
                }
                for mid, st in sorted(self._markets.items())
            },
            "subscriptions": {
                _key_str(key): {"last_seq": s.last_seq, "markets": sorted(s.markets)}
                for key, s in sorted(self._subs.items())
            },
            "confirmed": dict(sorted(self._confirmed.items())),
            "stats": self.stats.as_dict(),
        }

    # ------------------------------------------------------------------ checkpoints
    def export_state(self) -> dict[str, Any]:
        """Complete, JSON-serialisable state (checkpoints). :meth:`from_state` restores a
        manager whose future behaviour is identical; timing telemetry is not part of it."""
        return {
            "source": self.source,
            "max_future_ms": self.max_future_ms,
            "markets": [
                {
                    "market": json.loads(canonical_json(st.market)),
                    "status": st.status.value,
                    "sync": st.sync.value,
                    "reason": st.reason,
                    "sid": st.sid,
                    "connection_id": st.connection_id,
                    "yes": [[dec_str(p), dec_str(q)] for p, q in sorted(st.yes.items())],
                    "no": [[dec_str(p), dec_str(q)] for p, q in sorted(st.no.items())],
                    "last_seq": st.last_seq,
                    "exchange_ts_ms": st.exchange_ts_ms,
                    "received_ts_ms": st.received_ts_ms,
                    "last_sync_ts_ms": st.last_sync_ts_ms,
                }
                for _, st in sorted(self._markets.items())
            ],
            "subscriptions": [
                [key[0], key[1], s.last_seq, sorted(s.markets)]
                for key, s in sorted(self._subs.items())
            ],
            "confirmed": dict(sorted(self._confirmed.items())),
            "recovery": [
                [conn, dict(sorted(markets.items()))]
                for conn, markets in sorted(self._recovery.items(), key=lambda kv: str(kv[0]))
            ],
            "stats": self.stats.as_dict(),
            "last_position": self.last_position,
            "last_received_ts_ms": self.last_received_ts_ms,
        }

    @classmethod
    def from_state(
        cls, state: dict[str, Any], *, clock_ns: Callable[[], int] = time.perf_counter_ns
    ) -> BookManager:
        markets = [Market.model_validate(m["market"]) for m in state["markets"]]
        mgr = cls(
            markets, source=state["source"], clock_ns=clock_ns, max_future_ms=state["max_future_ms"]
        )
        for m in state["markets"]:
            st = mgr._markets[m["market"]["market_id"]]
            st.status = MarketStatus(m["status"])
            st.sync = SyncStatus(m["sync"])
            st.reason = m["reason"]
            st.sid = m["sid"]
            st.connection_id = m["connection_id"]
            st.yes = {Decimal(p): Decimal(q) for p, q in m["yes"]}
            st.no = {Decimal(p): Decimal(q) for p, q in m["no"]}
            st.last_seq = m["last_seq"]
            st.exchange_ts_ms = m["exchange_ts_ms"]
            st.received_ts_ms = m["received_ts_ms"]
            st.last_sync_ts_ms = m["last_sync_ts_ms"]
        for conn, sid, last_seq, members in state["subscriptions"]:
            mgr._subs[(conn, sid)] = _Subscription(
                sid=sid, connection_id=conn, last_seq=last_seq, markets=set(members)
            )
        mgr._confirmed = dict(state["confirmed"])
        mgr._recovery = {conn: dict(markets) for conn, markets in state["recovery"]}
        mgr.stats = ManagerStats(**state["stats"])
        mgr.last_position = state["last_position"]
        mgr.last_received_ts_ms = state["last_received_ts_ms"]
        return mgr

    # ------------------------------------------------------------------ processing
    def process(self, msg: StreamMessage) -> list[BookUpdate]:
        started = self._clock_ns()
        self.stats.messages += 1
        self.last_position = msg.position
        self.last_received_ts_ms = msg.received_ts_ms
        ev = msg.event
        out: list[tuple[str, str, str | None]] = []  # (market, kind, reason)
        conn = getattr(ev, "connection_id", None)
        if isinstance(ev, RawWireEvent):
            try:
                ev = normalize_ws_message(
                    ev.payload, connection_id=ev.connection_id, received_ts_ms=msg.received_ts_ms
                )
            except BookValidationError as exc:
                self.stats.malformed += 1
                out.extend(self._on_malformed(msg, conn, exc))
                return self._publish(msg, out, started)
        self._accepted = False
        limit = msg.received_ts_ms + self.max_future_ms
        ex_ts = getattr(ev, "exchange_ts_ms", None)
        future = msg.emitted_ts_ms > limit or (isinstance(ex_ts, int) and ex_ts > limit)
        if future:
            self.stats.future_timestamps += 1
        if future and isinstance(ev, OrderBookSnapshotEvent | OrderBookDeltaEvent):
            out.extend(self._quarantine(ev))
        elif isinstance(ev, OrderBookSnapshotEvent):
            out.extend(self._on_snapshot(msg, ev))
        elif isinstance(ev, OrderBookDeltaEvent):
            out.extend(self._on_delta(msg, ev))
        elif isinstance(ev, MarketStatusEvent):
            st = self._markets.get(ev.market_id)
            if st is not None:
                st.status = ev.status
                out.append((ev.market_id, "status", None))
        elif isinstance(ev, MarketLifecycleEvent):
            out.extend(self._on_lifecycle(ev))
        elif isinstance(ev, ConnectionEvent):
            if ev.state == "disconnected":
                out.extend(
                    self.mark_connection_lost(ev.connection_id, DesyncReason.CONNECTION_LOST)
                )
            elif not future:
                self._confirmed.setdefault(ev.connection_id, msg.emitted_ts_ms)
        elif isinstance(ev, HeartbeatEvent):
            self._accepted = True
        if self._accepted and not future and conn is not None:
            prev = self._confirmed.get(conn)
            if prev is None or msg.emitted_ts_ms > prev:
                self._confirmed[conn] = msg.emitted_ts_ms
        return self._publish(msg, out, started)

    def _quarantine(
        self, ev: OrderBookSnapshotEvent | OrderBookDeltaEvent
    ) -> list[tuple[str, str, str | None]]:
        """Future-dated book message: consume its sequence number, never apply it."""
        key = (ev.connection_id, ev.sid)
        if isinstance(ev, OrderBookDeltaEvent) and key not in self._subs:
            self.stats.ignored_stale_subscription += 1
            return []
        verdict, out = self._check_seq(key, ev.seq)
        if verdict == "dup":
            self.stats.duplicates_dropped += 1
            return out
        if verdict == "gap":
            return out
        st = self._markets.get(ev.market_id)
        if st is None or (isinstance(ev, OrderBookDeltaEvent) and _market_key(st) != key):
            return out
        self.stats.rejected += 1
        if self._desync(ev.market_id, DesyncReason.FUTURE_TIMESTAMP, ev.connection_id):
            out.append((ev.market_id, "desync", DesyncReason.FUTURE_TIMESTAMP))
        return out

    def mark_connection_lost(self, connection_id: str, reason: str) -> list[tuple[str, str, str]]:
        """All books on the connection become untrusted (also used by heartbeat timeouts)."""
        self.stats.connection_losses += 1
        out: list[tuple[str, str, str]] = []
        for key in sorted(k for k in self._subs if k[0] == connection_id):
            for mid in sorted(self._subs[key].markets):
                if self._desync(mid, reason, connection_id):
                    out.append((mid, "desync", reason))
            del self._subs[key]
        self._confirmed.pop(connection_id, None)
        return out

    def connection_lost(self, connection_id: str, reason: str) -> list[BookUpdate]:
        """Out-of-band loss (no market-data message): desync every market on the connection and
        return the notifications to publish. Markets already UNSYNCHRONIZED are not repeated."""
        started = self._clock_ns()
        items: list[tuple[str, str, str | None]] = list(
            self.mark_connection_lost(connection_id, reason)
        )
        received = self.last_received_ts_ms if self.last_received_ts_ms is not None else 0
        return self._build_updates(items, self.last_position, None, received, started)

    def recovery_failed(self, request: RecoveryRequest, reason: str) -> list[BookUpdate]:
        """The source could not honour ``request``: the requested books can never be restored
        on that connection. Every market on the connection is desynchronized, and every
        requested market is (re)published as UNSYNCHRONIZED with ``reason`` even if it already
        was, so consumers learn that no recovery is coming."""
        started = self._clock_ns()
        items: list[tuple[str, str, str | None]] = []
        if request.connection_id is not None:
            items.extend(self.mark_connection_lost(request.connection_id, reason))
        listed = {mid for mid, _, _ in items}
        for mid in sorted(set(request.market_ids) - listed):
            st = self._markets.get(mid)
            if st is None:
                continue
            self._desync(mid, reason, request.connection_id)
            st.reason = reason
            items.append((mid, "desync", reason))
        self._recovery.pop(request.connection_id, None)
        received = self.last_received_ts_ms if self.last_received_ts_ms is not None else 0
        return self._build_updates(items, self.last_position, None, received, started)

    def sync_reason(self, market_id: str) -> str | None:
        """Why the market is not SYNCHRONIZED (None when it is, or before any snapshot)."""
        return self._markets[market_id].reason

    # ------------------------------------------------------------------ handlers
    def _desync(self, market_id: str, reason: str, connection_id: str | None) -> bool:
        st = self._markets.get(market_id)
        if st is None:
            return False
        self._recovery.setdefault(connection_id, {})[market_id] = reason
        if st.sync is SyncStatus.UNSYNCHRONIZED:
            return False
        st.sync = SyncStatus.UNSYNCHRONIZED
        st.reason = reason
        self.stats.desync_events += 1
        return True

    def _desync_sub(self, key: _SubKey, reason: str) -> list[tuple[str, str, str | None]]:
        sub = self._subs[key]
        return [
            (mid, "desync", reason)
            for mid in sorted(sub.markets)
            if self._desync(mid, reason, sub.connection_id)
        ]

    def _check_seq(self, key: _SubKey, seq: int) -> tuple[str, list[tuple[str, str, str | None]]]:
        """Returns ("new" | "ok" | "dup" | "gap", desync updates)."""
        sub = self._subs.get(key)
        if sub is None:
            self._subs[key] = _Subscription(sid=key[1], connection_id=key[0], last_seq=seq)
            return "new", []
        if seq <= sub.last_seq:
            return "dup", []
        if seq == sub.last_seq + 1:
            sub.last_seq = seq
            return "ok", []
        self.stats.gaps_detected += 1
        sub.last_seq = seq
        return "gap", self._desync_sub(key, DesyncReason.SEQUENCE_GAP)

    def _on_snapshot(
        self, msg: StreamMessage, ev: OrderBookSnapshotEvent
    ) -> list[tuple[str, str, str | None]]:
        key = (ev.connection_id, ev.sid)
        verdict, out = self._check_seq(key, ev.seq)
        if verdict == "dup":
            self.stats.duplicates_dropped += 1
            return out
        st = self._markets.get(ev.market_id)
        if st is None:
            self.stats.ignored_unknown_market += 1
            return out
        grid = st.market.price_grid
        try:
            yes = validate_side(Side.YES, ev.yes_bids, grid)
            no = validate_side(Side.NO, ev.no_bids, grid)
            if yes and no and yes[0].price + no[0].price >= ONE:
                raise BookValidationError(BookErrorCode.CROSSED_BOOK, "snapshot crossed")
        except (BookValidationError, ValidationError) as exc:
            self.stats.rejected += 1
            reason = DesyncReason.INVALID_SNAPSHOT
            if self._desync(ev.market_id, reason, ev.connection_id):
                out.append((ev.market_id, "desync", f"{reason}:{_code(exc)}"))
            return out
        old = _market_key(st)
        if old is not None and old in self._subs and old != key:
            self._subs[old].markets.discard(ev.market_id)
        self._subs[key].markets.add(ev.market_id)
        st.sid = ev.sid
        st.connection_id = ev.connection_id
        st.yes = {lv.price: lv.quantity for lv in yes}
        st.no = {lv.price: lv.quantity for lv in no}
        st.sync = SyncStatus.SYNCHRONIZED
        st.reason = None
        st.last_seq = ev.seq
        st.exchange_ts_ms = ev.exchange_ts_ms
        st.received_ts_ms = msg.received_ts_ms
        st.last_sync_ts_ms = msg.received_ts_ms
        self._recovery.get(ev.connection_id, {}).pop(ev.market_id, None)
        self.stats.snapshots_applied += 1
        self._accepted = True
        out.append((ev.market_id, "snapshot", None))
        return out

    def _on_delta(
        self, msg: StreamMessage, ev: OrderBookDeltaEvent
    ) -> list[tuple[str, str, str | None]]:
        key = (ev.connection_id, ev.sid)
        if key not in self._subs:
            self.stats.ignored_stale_subscription += 1
            return []
        verdict, out = self._check_seq(key, ev.seq)
        if verdict == "dup":
            self.stats.duplicates_dropped += 1
            return out
        if verdict == "gap":
            return out
        st = self._markets.get(ev.market_id)
        if st is None:
            self.stats.ignored_unknown_market += 1
            return out
        if _market_key(st) != key:
            self.stats.ignored_stale_subscription += 1
            return out
        if st.sync is not SyncStatus.SYNCHRONIZED:
            self.stats.ignored_unsynchronized += 1
            return out
        side_levels = st.levels(ev.side)
        try:
            PriceLevel(side=ev.side, price=ev.price, quantity=abs(ev.delta))
            if not st.market.price_grid.is_valid_price(ev.price):
                raise BookValidationError(BookErrorCode.PRICE_OFF_GRID, str(ev.price))
        except (BookValidationError, ValidationError) as exc:
            self.stats.rejected += 1
            if self._desync(ev.market_id, DesyncReason.INVALID_DELTA, ev.connection_id):
                out.append((ev.market_id, "desync", f"{DesyncReason.INVALID_DELTA}:{_code(exc)}"))
            return out
        new_qty = side_levels.get(ev.price, ZERO) + ev.delta
        if new_qty < ZERO:
            self.stats.rejected += 1
            if self._desync(ev.market_id, DesyncReason.NEGATIVE_QUANTITY, ev.connection_id):
                out.append((ev.market_id, "desync", DesyncReason.NEGATIVE_QUANTITY))
            return out
        if new_qty == ZERO:
            side_levels.pop(ev.price, None)
        else:
            side_levels[ev.price] = new_qty
        if st.yes and st.no and max(st.yes) + max(st.no) >= ONE:
            self.stats.rejected += 1
            if self._desync(ev.market_id, DesyncReason.CROSSED_BOOK, ev.connection_id):
                out.append((ev.market_id, "desync", DesyncReason.CROSSED_BOOK))
            return out
        st.last_seq = ev.seq
        st.exchange_ts_ms = ev.exchange_ts_ms
        st.received_ts_ms = msg.received_ts_ms
        self.stats.deltas_applied += 1
        self._accepted = True
        out.append((ev.market_id, "delta", None))
        return out

    def _on_malformed(
        self, msg: StreamMessage, conn: str | None, exc: BookValidationError
    ) -> list[tuple[str, str, str | None]]:
        assert isinstance(msg.event, RawWireEvent)
        payload = msg.event.payload
        sid_raw, seq_raw = payload.get("sid"), payload.get("seq")
        reason = f"{DesyncReason.MALFORMED_MESSAGE}:{exc.code.value}"
        key = (conn, str(sid_raw)) if conn is not None else None
        if (
            key is not None
            and isinstance(sid_raw, int)
            and not isinstance(sid_raw, bool)
            and key in self._subs
        ):
            if isinstance(seq_raw, int) and not isinstance(seq_raw, bool):
                self._subs[key].last_seq = max(self._subs[key].last_seq, seq_raw)
            return self._desync_sub(key, reason)
        if conn is not None:
            return list(self.mark_connection_lost(conn, reason))
        return []

    def _on_lifecycle(self, ev: MarketLifecycleEvent) -> list[tuple[str, str, str | None]]:
        if ev.action == "created":
            if ev.market is not None and ev.market_id not in self._markets:
                self._markets[ev.market_id] = _MarketState(
                    market=ev.market, status=ev.market.status
                )
            return [(ev.market_id, "status", "created")]
        st = self._markets.pop(ev.market_id, None)
        if st is None:
            return []
        old = _market_key(st)
        if old is not None and old in self._subs:
            self._subs[old].markets.discard(ev.market_id)
        for pending in self._recovery.values():
            pending.pop(ev.market_id, None)
        return [(ev.market_id, "removed", "removed")]

    def _publish(
        self, msg: StreamMessage, items: list[tuple[str, str, str | None]], started: int
    ) -> list[BookUpdate]:
        ex_ts = getattr(msg.event, "exchange_ts_ms", None)
        return self._build_updates(
            items,
            msg.position,
            ex_ts if isinstance(ex_ts, int) else None,
            msg.received_ts_ms,
            started,
        )

    def _build_updates(
        self,
        items: list[tuple[str, str, str | None]],
        position: int,
        ex_ts: int | None,
        received_ts_ms: int,
        started: int,
    ) -> list[BookUpdate]:
        if not items:
            return []
        done = self._clock_ns()
        out = []
        for mid, kind, reason in items:
            st = self._markets.get(mid)
            out.append(
                BookUpdate(
                    market_id=mid,
                    position=position,
                    kind=kind,
                    sync_status=st.sync if st is not None else SyncStatus.UNSYNCHRONIZED,
                    reason=reason,
                    connection_id=st.connection_id if st is not None else None,
                    subscription_id=st.sid if st is not None else None,
                    source_sequence=st.last_seq if st is not None else None,
                    timing=Timing(
                        exchange_ts_ms=ex_ts,
                        received_ts_ms=received_ts_ms,
                        processing_started_ns=started,
                        processing_completed_ns=done,
                    ),
                )
            )
        return out


def _levels(side: Side, levels: dict[Decimal, Decimal]) -> tuple[PriceLevel, ...]:
    return tuple(
        PriceLevel.model_construct(side=side, price=p, quantity=q)
        for p, q in sorted(levels.items(), key=lambda kv: kv[0], reverse=True)
    )


def _code(exc: Exception) -> str:
    if isinstance(exc, BookValidationError):
        return exc.code.value
    if isinstance(exc, ValidationError):
        for err in exc.errors():
            inner = (err.get("ctx") or {}).get("error")
            if isinstance(inner, BookValidationError):
                return inner.code.value
    return BookErrorCode.MALFORMED.value

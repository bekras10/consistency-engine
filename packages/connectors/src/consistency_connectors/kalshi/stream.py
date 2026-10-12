"""Authenticated orderbook_delta WebSocket session.

Sequence numbers are forwarded on the subscription id the server sends. This
module does not assume a per-market sequence. A dead socket raises
``ConnectionError`` so the ingestion runner invalidates books before the next
connection. Authentication failure is not retried.
"""

from __future__ import annotations

import asyncio
import json
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from typing import Protocol

from consistency_connectors.base import RecoveryRequest, SourceAuthenticationError
from consistency_connectors.kalshi.auth import Signer, signed_headers
from consistency_connectors.kalshi.endpoints import WS_SIGN_PATH, resolve_ws_url
from consistency_connectors.settings import Settings
from consistency_core.events import (
    ConnectionEvent,
    HeartbeatEvent,
    MarketStatusEvent,
    OrderBookDeltaEvent,
    OrderBookSnapshotEvent,
    RawWireEvent,
    StreamMessage,
)
from consistency_core.models.common import MarketStatus
from consistency_core.models.orderbook import BookValidationError
from consistency_core.normalization import normalize_ws_message


class WsConnection(Protocol):
    async def send(self, text: str) -> None: ...

    async def recv(self) -> str: ...

    async def close(self) -> None: ...

    async def ping(self) -> None: ...


class WsTransport(Protocol):
    async def connect(self, url: str, headers: Mapping[str, str]) -> WsConnection: ...


class WebsocketsTransport:
    async def connect(self, url: str, headers: Mapping[str, str]) -> WsConnection:
        import websockets

        try:
            socket = await websockets.connect(
                url,
                additional_headers=dict(headers),
                open_timeout=10,
                ping_interval=None,
                max_queue=32,
            )
        except Exception as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status in (401, 403):
                raise SourceAuthenticationError(
                    "Kalshi WebSocket handshake was rejected."
                ) from None
            raise ConnectionError("Kalshi WebSocket connection failed.") from None
        return _LibrarySocket(socket)


class _LibrarySocket:
    def __init__(self, socket: object) -> None:
        self._socket = socket

    async def send(self, text: str) -> None:
        await self._socket.send(text)  # type: ignore[attr-defined]

    async def recv(self) -> str:
        message = await self._socket.recv()  # type: ignore[attr-defined]
        if isinstance(message, bytes):
            return message.decode("utf-8")
        if isinstance(message, str):
            return message
        raise ConnectionError("Kalshi WebSocket frame was not text.")

    async def close(self) -> None:
        await self._socket.close()  # type: ignore[attr-defined]

    async def ping(self) -> None:
        waiter = await self._socket.ping()  # type: ignore[attr-defined]
        await waiter


class OrderBookStream:
    def __init__(
        self,
        settings: Settings,
        *,
        signer: Signer,
        transport: WsTransport | None = None,
        clock_ms: Callable[[], int] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self.settings = settings
        self._signer = signer
        self._transport = transport or WebsocketsTransport()
        self._clock = clock_ms or _wall_ms
        self._sleep = sleep or asyncio.sleep
        self._conn: WsConnection | None = None
        self._sid: int | None = None
        self._connection_id = ""
        self._generation = 0
        self._command = 0
        self._position = 0
        self._control: asyncio.Queue[StreamMessage] = asyncio.Queue(settings.kalshi_ws_queue_max)
        self._suspend_reason: dict[str, str] = {}
        self.backpressured = False
        self.reconnects = 0
        self.gaps_recovered = 0
        self.message_count = 0
        self.state = "idle"
        self.last_error = ""
        self.auth_failures = 0
        self.mismatches: deque[dict[str, object]] = deque(maxlen=50)
        self.rules_changed: list[str] = []
        self.subscribed: tuple[str, ...] = ()

    def status(self) -> dict[str, object]:
        return {
            "state": self.state,
            "connection_id": self._connection_id,
            "subscription_id": None if self._sid is None else str(self._sid),
            "subscribed": list(self.subscribed),
            "reconnects": self.reconnects,
            "messages": self.message_count,
            "auth_failures": self.auth_failures,
            "backpressured": self.backpressured,
            "rules_changed": list(self.rules_changed),
            "mismatches": list(self.mismatches),
            "last_error": self.last_error,
            "suspended": sorted(self._suspend_reason),
        }

    def suspend(self, market_id: str, status: MarketStatus, reason: str) -> None:
        self._suspend_reason[market_id] = reason
        if reason == "rules_changed" and market_id not in self.rules_changed:
            self.rules_changed.append(market_id)
        self._enqueue(self._status_message(market_id, status))

    def record_mismatch(self, ticker: str, *, equal: bool) -> None:
        self.mismatches.append({"ticker": ticker, "equal": equal, "checked_at_ms": self._clock()})

    async def request_recovery(self, request: RecoveryRequest) -> None:
        conn = self._conn
        sid = self._sid
        if conn is None or sid is None:
            raise ConnectionError("Kalshi order book is not subscribed.")
        tickers = [ticker for ticker in request.market_ids if ticker]
        if not tickers:
            return
        self.gaps_recovered += 1
        await conn.send(_dumps(self._update(sid, tickers, "get_snapshot")))

    async def unsubscribe(self, market_ids: list[str]) -> None:
        conn = self._conn
        sid = self._sid
        if conn is None or sid is None or not market_ids:
            return
        await conn.send(_dumps(self._update(sid, market_ids, "delete_markets")))

    async def messages(self, tickers: list[str]) -> AsyncIterator[StreamMessage]:
        """One connection. The caller reconnects after ``ConnectionError``."""
        self.subscribed = tuple(tickers)
        self._generation += 1
        self._connection_id = f"kalshi-{self._generation}"
        self._sid = None
        self.state = "connecting"
        headers = signed_headers(
            key_id=self.settings.kalshi_api_key_id.strip(),
            signer=self._signer,
            timestamp_ms=self._clock(),
            method="GET",
            sign_path=WS_SIGN_PATH,
        )
        try:
            conn = await self._transport.connect(resolve_ws_url(self.settings), headers)
        except SourceAuthenticationError:
            self.auth_failures += 1
            self.state = "authentication_failed"
            self.last_error = "authentication_failed"
            raise
        except Exception as exc:
            self.state = "disconnected"
            self.last_error = type(exc).__name__
            raise ConnectionError("Kalshi WebSocket connection failed.") from None
        self._conn = conn
        self.state = "connected"
        self.reconnects = max(0, self._generation - 1)
        yield self._frame(ConnectionEvent(state="connected", connection_id=self._connection_id))
        try:
            await conn.send(
                _dumps(
                    {
                        "id": self._next_command(),
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["orderbook_delta"],
                            "market_tickers": tickers,
                        },
                    }
                )
            )
            async for kind, payload in self._read(conn):
                if kind == "heartbeat":
                    yield self._frame(HeartbeatEvent(connection_id=self._connection_id))
                    continue
                if kind == "control" and isinstance(payload, StreamMessage):
                    yield payload
                    continue
                if kind == "data" and isinstance(payload, str):
                    for message in self._decode(payload):
                        yield message
        finally:
            self._conn = None
            self._sid = None
            if self.state != "authentication_failed":
                self.state = "disconnected"
            await _close_quietly(conn)

    async def _read(self, conn: WsConnection) -> AsyncIterator[tuple[str, object]]:
        recv_task: asyncio.Task[str] | None = asyncio.create_task(conn.recv())
        ctrl_task: asyncio.Task[StreamMessage] | None = asyncio.create_task(self._control.get())
        hb_task: asyncio.Task[None] | None = (
            asyncio.create_task(self._heartbeat(conn))
            if self.settings.kalshi_ping_interval_s > 0
            else None
        )
        pending = {task for task in (recv_task, ctrl_task, hb_task) if task is not None}
        try:
            while pending:
                done, _waiting = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                if recv_task in done:
                    text = recv_task.result()
                    self.message_count += 1
                    yield ("data", text)
                    recv_task = asyncio.create_task(conn.recv())
                    pending.add(recv_task)
                if ctrl_task in done:
                    yield ("control", ctrl_task.result())
                    ctrl_task = asyncio.create_task(self._control.get())
                    pending.add(ctrl_task)
                if hb_task is not None and hb_task in done:
                    hb_task.result()
                    yield ("heartbeat", None)
                    hb_task = asyncio.create_task(self._heartbeat(conn))
                    pending.add(hb_task)
                pending = {task for task in pending if not task.done()}
        except asyncio.CancelledError:
            raise
        except SourceAuthenticationError:
            raise
        except Exception as exc:
            self.last_error = type(exc).__name__
            raise ConnectionError("Kalshi WebSocket connection lost.") from None
        finally:
            for task in (recv_task, ctrl_task, hb_task):
                if task is not None:
                    task.cancel()
            await asyncio.gather(
                *(task for task in (recv_task, ctrl_task, hb_task) if task is not None),
                return_exceptions=True,
            )

    async def _heartbeat(self, conn: WsConnection) -> None:
        await self._sleep(self.settings.kalshi_ping_interval_s)
        await asyncio.wait_for(conn.ping(), self.settings.kalshi_pong_timeout_s)

    def _decode(self, text: str) -> list[StreamMessage]:
        try:
            raw = json.loads(text)
        except json.JSONDecodeError:
            return [self._malformed()]
        if not isinstance(raw, dict):
            return [self._malformed()]
        kind = raw.get("type")
        if kind == "subscribed":
            body = raw.get("msg")
            if isinstance(body, dict) and isinstance(body.get("sid"), int):
                self._sid = body["sid"]
            return []
        if kind == "error":
            body = raw.get("msg")
            code = body.get("code") if isinstance(body, dict) else None
            if code == 9:
                self.auth_failures += 1
                self.state = "authentication_failed"
                raise SourceAuthenticationError("Kalshi WebSocket channel rejected the session.")
            self.last_error = f"ws_error:{code}"
            return [self._malformed()]
        if kind in ("orderbook_snapshot", "orderbook_delta"):
            return self._book(raw)
        if kind in ("ok", "unsubscribed"):
            return []
        return [self._malformed()]

    def _book(self, raw: dict[str, object]) -> list[StreamMessage]:
        received = self._clock()
        try:
            event = normalize_ws_message(
                raw, connection_id=self._connection_id, received_ts_ms=received
            )
        except BookValidationError:
            return [self._malformed()]
        exchange = _exchange_ts(raw)
        event = event.model_copy(update={"exchange_ts_ms": exchange})
        out = [self._frame(event, exchange=exchange, received=received)]
        if isinstance(event, OrderBookSnapshotEvent):
            reason = self._suspend_reason.get(event.market_id)
            if reason == "rest_mismatch":
                self._suspend_reason.pop(event.market_id, None)
                out.append(self._status_message(event.market_id, MarketStatus.OPEN))
        return out

    def _malformed(self) -> StreamMessage:
        return self._frame(
            RawWireEvent(connection_id=self._connection_id, payload={"error": "malformed"})
        )

    def _status_message(self, market_id: str, status: MarketStatus) -> StreamMessage:
        return self._frame(
            MarketStatusEvent(market_id=market_id, status=status, exchange_ts_ms=self._clock())
        )

    def _frame(
        self,
        event: ConnectionEvent
        | HeartbeatEvent
        | MarketStatusEvent
        | OrderBookSnapshotEvent
        | OrderBookDeltaEvent
        | RawWireEvent,
        *,
        exchange: int | None = None,
        received: int | None = None,
    ) -> StreamMessage:
        stamp = received if received is not None else self._clock()
        self._position += 1
        return StreamMessage(
            position=self._position,
            emitted_ts_ms=exchange if exchange is not None else stamp,
            received_ts_ms=stamp,
            event=event,
        )

    def _enqueue(self, message: StreamMessage) -> None:
        try:
            self._control.put_nowait(message)
        except asyncio.QueueFull:
            self.backpressured = True

    def _next_command(self) -> int:
        self._command += 1
        return self._command

    def _update(self, sid: int, tickers: list[str], action: str) -> dict[str, object]:
        return {
            "id": self._next_command(),
            "cmd": "update_subscription",
            "params": {"sids": [sid], "market_tickers": tickers, "action": action},
        }


def _exchange_ts(raw: Mapping[str, object]) -> int | None:
    body = raw.get("msg")
    if isinstance(body, dict):
        stamp = body.get("ts_ms")
        if isinstance(stamp, int) and not isinstance(stamp, bool):
            return stamp
    sending = raw.get("sending_ts_ms")
    if isinstance(sending, int) and not isinstance(sending, bool):
        return sending
    return None


def _dumps(payload: Mapping[str, object]) -> str:
    return json.dumps(payload, separators=(",", ":"))


def _wall_ms() -> int:
    import time

    return time.time_ns() // 1_000_000


async def _close_quietly(conn: WsConnection) -> None:
    try:
        await conn.close()
    except Exception:
        return

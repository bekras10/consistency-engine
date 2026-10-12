"""Read-only Kalshi REST client: pagination, 429 backoff, bounded concurrency.

JSON numbers are parsed with ``parse_float=str`` so a strike that arrives as a
JSON number keeps the decimal token the server sent. Money is never a binary float.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Protocol
from urllib.parse import parse_qsl, urlencode

from consistency_connectors.base import SourceAuthenticationError
from consistency_connectors.kalshi.auth import (
    KalshiCredentialsError,
    Signer,
    load_signer,
    signed_headers,
)
from consistency_connectors.kalshi.endpoints import assert_read_only, resolve_rest_base
from consistency_connectors.settings import Settings


class KalshiHttpError(RuntimeError):
    def __init__(self, status: int, path: str) -> None:
        super().__init__(f"Kalshi GET {path} failed with HTTP {status}")
        self.status = status
        self.path = path


@dataclass
class HttpResponse:
    status: int
    body: bytes
    headers: Mapping[str, str] = field(default_factory=dict)


class HttpTransport(Protocol):
    async def request(
        self, method: str, url: str, headers: Mapping[str, str], timeout_s: float
    ) -> HttpResponse: ...

    async def aclose(self) -> None: ...


class HttpxTransport:
    """Production transport. Imported only when a live client is constructed."""

    def __init__(self) -> None:
        self._client: object | None = None

    async def request(
        self, method: str, url: str, headers: Mapping[str, str], timeout_s: float
    ) -> HttpResponse:
        import httpx

        if self._client is None:
            self._client = httpx.AsyncClient()
        client = self._client
        assert isinstance(client, httpx.AsyncClient)
        response = await client.request(method, url, headers=dict(headers), timeout=timeout_s)
        return HttpResponse(response.status_code, response.content, dict(response.headers))

    async def aclose(self) -> None:
        client = self._client
        if client is not None:
            import httpx

            assert isinstance(client, httpx.AsyncClient)
            await client.aclose()
            self._client = None


def loads_exact(payload: bytes) -> object:
    return json.loads(payload.decode("utf-8"), parse_float=str)


def _delay(attempt: int, *, base: float, cap: float, rng: random.Random) -> float:
    span = base * (2**attempt)
    limited = cap if span > cap else span
    sample = rng.random()
    if isinstance(sample, bool) or not isinstance(sample, float):
        sample = 0.0
    return limited * (1 + 0.2 * (2 * sample - 1))


class KalshiRestClient:
    def __init__(
        self,
        settings: Settings,
        *,
        transport: HttpTransport | None = None,
        clock_ms: Callable[[], int] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        rng: random.Random | None = None,
        signer: Signer | None = None,
    ) -> None:
        self.settings = settings
        self.base_url = resolve_rest_base(settings)
        self._transport = transport
        self._clock = clock_ms or (lambda: _wall_ms())
        self._sleep = sleep or asyncio.sleep
        self._rng = rng or random.Random()
        self._signer = signer
        self._signer_loaded = signer is not None
        self.requests = 0
        self.retries = 0
        self._interval = max(
            settings.kalshi_min_request_interval_s,
            settings.kalshi_request_token_cost / settings.kalshi_read_token_budget,
        )

    @property
    def clock_ms(self) -> Callable[[], int]:
        return self._clock

    def signer(self) -> Signer | None:
        return self._signer_or_none()

    def _signer_or_none(self) -> Signer | None:
        if self._signer_loaded:
            return self._signer
        self._signer_loaded = True
        key_id = self.settings.kalshi_api_key_id.strip()
        path = self.settings.kalshi_private_key_path.strip()
        if not key_id and not path:
            self._signer = None
            return None
        if not key_id or not path:
            raise KalshiCredentialsError(
                "Kalshi API key id and private-key path are both required."
            )
        self._signer = load_signer(path)
        return self._signer

    def require_credentials(self) -> None:
        """Live ingestion signs every call. Missing credentials never open a socket."""
        if self._signer_or_none() is None:
            raise KalshiCredentialsError(
                "Kalshi credentials are not configured. Set KALSHI_API_KEY_ID and "
                "KALSHI_PRIVATE_KEY_PATH. Authorization flags alone do not connect."
            )

    async def aclose(self) -> None:
        if self._transport is not None:
            await self._transport.aclose()

    def _owned_transport(self) -> HttpTransport:
        if self._transport is None:
            self._transport = HttpxTransport()
        return self._transport

    async def get_json(
        self, path: str, params: Mapping[str, str] | None = None
    ) -> dict[str, object]:
        bare, _, embedded = path.partition("?")
        signed = assert_read_only("GET", bare)
        pairs = [(key, value) for key, value in parse_qsl(embedded, keep_blank_values=False)]
        if params:
            pairs.extend((key, value) for key, value in params.items() if value != "")
        query = "?" + urlencode(pairs) if pairs else ""
        url = self.base_url + bare + query
        headers: dict[str, str] = {"Accept": "application/json"}
        signer = self._signer_or_none()
        if signer is not None:
            headers.update(
                signed_headers(
                    key_id=self.settings.kalshi_api_key_id.strip(),
                    signer=signer,
                    timestamp_ms=self._clock(),
                    method="GET",
                    sign_path=signed,
                )
            )
        transport = self._owned_transport()
        last_status = 0
        for attempt in range(self.settings.kalshi_max_retries + 1):
            if attempt == 0 and self._interval > 0 and self.requests:
                await self._sleep(self._interval)
            self.requests += 1
            try:
                response = await transport.request(
                    "GET", url, headers, self.settings.kalshi_http_timeout_s
                )
            except (TimeoutError, ConnectionError, OSError):
                last_status = 0
                if attempt >= self.settings.kalshi_max_retries:
                    raise
                self.retries += 1
                await self._sleep(_delay(attempt, base=0.2, cap=8.0, rng=self._rng))
                continue
            last_status = response.status
            if response.status == 429 or response.status >= 500:
                if attempt >= self.settings.kalshi_max_retries:
                    raise KalshiHttpError(response.status, bare)
                self.retries += 1
                await self._sleep(_delay(attempt, base=0.2, cap=8.0, rng=self._rng))
                continue
            if response.status in (401, 403):
                raise SourceAuthenticationError("Kalshi rejected the signed GET.")
            if response.status >= 400:
                raise KalshiHttpError(response.status, bare)
            parsed = loads_exact(response.body)
            if not isinstance(parsed, dict):
                raise KalshiHttpError(response.status, bare)
            return parsed
        raise KalshiHttpError(last_status, bare)

    async def get_orderbooks(self, tickers: list[str]) -> list[dict[str, object]]:
        if not tickers:
            return []
        if len(tickers) > self.settings.kalshi_orderbook_batch_size:
            raise KalshiHttpError(400, "/markets/orderbooks")
        query = urlencode([("tickers", ticker) for ticker in tickers])
        body = await self.get_json("/markets/orderbooks?" + query)
        books = body.get("orderbooks")
        if not isinstance(books, list):
            raise KalshiHttpError(200, "/markets/orderbooks")
        return [item for item in books if isinstance(item, dict)]

    async def paginate(
        self,
        path: str,
        params: Mapping[str, str],
        item_key: str,
        *,
        max_items: int,
    ) -> tuple[list[dict[str, object]], int]:
        items: list[dict[str, object]] = []
        cursor = ""
        pages = 0
        while pages < self.settings.kalshi_max_pages and len(items) < max_items:
            limit = min(self.settings.kalshi_page_limit, max_items - len(items))
            query = dict(params)
            query["limit"] = str(limit)
            if cursor:
                query["cursor"] = cursor
            body = await self.get_json(path, query)
            pages += 1
            batch = body.get(item_key, [])
            if not isinstance(batch, list):
                raise KalshiHttpError(200, path)
            for item in batch:
                if isinstance(item, dict):
                    items.append(item)
                if len(items) >= max_items:
                    break
            next_cursor = body.get("cursor")
            cursor = next_cursor if isinstance(next_cursor, str) else ""
            if not cursor:
                break
        return items[:max_items], pages


def _wall_ms() -> int:
    import time

    return time.time_ns() // 1_000_000

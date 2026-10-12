"""Documented Kalshi Trade API hosts, paths, and token costs.

Sources fetched 2026-10-11 from docs.kalshi.com (API environments, rate limits,
pagination, Get Markets, Get Multiple Market Orderbooks). Production and demo
are separate credential domains. This module does not open sockets.
"""

from __future__ import annotations

from urllib.parse import urlsplit

from consistency_connectors.settings import ConfigurationError, Settings

API_ROOT = "/trade-api/v2"
WS_SIGN_PATH = "/trade-api/ws/v2"

PRODUCTION_REST = "https://external-api.kalshi.com/trade-api/v2"
DEMO_REST = "https://external-api.demo.kalshi.co/trade-api/v2"
PRODUCTION_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
DEMO_WS = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"

# Also supported, per the environments page. Not the recommended hosts.
PRODUCTION_REST_HOSTS = frozenset({"external-api.kalshi.com", "api.elections.kalshi.com"})
DEMO_REST_HOSTS = frozenset({"external-api.demo.kalshi.co", "demo-api.kalshi.co"})
PRODUCTION_WS_HOSTS = frozenset({"external-api-ws.kalshi.com", "api.elections.kalshi.com"})
DEMO_WS_HOSTS = frozenset({"external-api-ws.demo.kalshi.co", "demo-api.kalshi.co"})

# Documented defaults (Basic tier read budget, default request cost, batch cap).
DEFAULT_READ_TOKEN_BUDGET = 200
DEFAULT_REQUEST_TOKEN_COST = 10
MARKETS_PAGE_MAX = 1000
ORDERBOOK_BATCH_MAX = 100

# Read-only allowlist. Paths are relative to the Trade API root.
ALLOWED_PREFIXES = (
    "/exchange/status",
    "/exchange/schedule",
    "/series",
    "/events",
    "/markets",
    "/account/limits",
)
DENIED_PREFIXES = (
    "/portfolio",
    "/orders",
    "/communications",
    "/order-groups",
    "/order_groups",
    "/fcm",
    "/api_keys",
    "/margin",
    "/historical",
)


class ReadOnlyViolationError(PermissionError):
    """A method or path that could trade, or that reads positions or balances."""


def assert_read_only(method: str, path: str) -> str:
    """Return the signable path or refuse the call.

    ``path`` is the path under the API root, with an optional query string.
    Only GET is allowed, and only the public market-data paths plus the
    account-limits connectivity check (usage tier, not a balance).
    """
    if method.upper() != "GET":
        raise ReadOnlyViolationError(f"{method.upper()} is not a read-only Kalshi call")
    bare = path.split("?", 1)[0]
    if not bare.startswith("/"):
        bare = "/" + bare
    for denied in DENIED_PREFIXES:
        if bare == denied or bare.startswith(denied + "/"):
            raise ReadOnlyViolationError(f"path {bare} is outside the read-only allowlist")
    if not any(bare == prefix or bare.startswith(prefix + "/") for prefix in ALLOWED_PREFIXES):
        raise ReadOnlyViolationError(f"path {bare} is outside the read-only allowlist")
    return API_ROOT + bare


def sign_path(path_under_root: str) -> str:
    """Full path from the API root, without a query string."""
    return assert_read_only("GET", path_under_root)


def _host_ok(url: str, *, scheme: str, allowed: frozenset[str]) -> bool:
    parts = urlsplit(url)
    return parts.scheme == scheme and parts.hostname in allowed and parts.username is None


def resolve_rest_base(settings: Settings) -> str:
    if settings.kalshi_environment == "demo":
        default, hosts = DEMO_REST, DEMO_REST_HOSTS
    else:
        default, hosts = PRODUCTION_REST, PRODUCTION_REST_HOSTS
    chosen = settings.kalshi_rest_base_url.strip() or default
    if not _host_ok(chosen, scheme="https", allowed=hosts):
        raise ConfigurationError(
            "KALSHI_REST_BASE_URL does not match the selected Kalshi environment."
        )
    return chosen.rstrip("/")


def resolve_ws_url(settings: Settings) -> str:
    if settings.kalshi_environment == "demo":
        default, hosts = DEMO_WS, DEMO_WS_HOSTS
    else:
        default, hosts = PRODUCTION_WS, PRODUCTION_WS_HOSTS
    chosen = settings.kalshi_ws_url.strip() or default
    if not _host_ok(chosen, scheme="wss", allowed=hosts):
        raise ConfigurationError("KALSHI_WS_URL does not match the selected Kalshi environment.")
    return chosen

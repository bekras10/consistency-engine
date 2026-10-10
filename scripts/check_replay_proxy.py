"""Hit the Next.js replay proxy: unauthorized posts are 401, and two viewers seek apart.

The shared ``REPLAY_API_TOKEN`` is attached by the Next server when it calls the
gateway. A browser receives an httpOnly capability cookie instead. Run while
``scripts/e2e.sh`` has the gateway and the Next server up.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request


def _call(
    url: str,
    method: str,
    body: dict[str, object] | None,
    *,
    cookie: str | None = None,
    token: str | None = None,
) -> tuple[int, dict[str, object], str, str]:
    data = None if body is None else json.dumps(body).encode()
    headers: dict[str, str] = {}
    if data is not None:
        headers["content-type"] = "application/json"
    if cookie:
        headers["cookie"] = cookie
    if token:
        headers["x-replay-token"] = token
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read().decode()
            payload = json.loads(raw) if raw else {}
            status = response.status
            set_cookie = response.headers.get("Set-Cookie") or ""
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        payload = json.loads(raw) if raw else {}
        status = exc.code
        set_cookie = exc.headers.get("Set-Cookie") or ""
    if not isinstance(payload, dict):
        raise SystemExit(f"{method} {url} did not return an object")
    return status, payload, raw if isinstance(raw, str) else "", set_cookie


def _capability(base: str, secret: str) -> str:
    status, body, raw, set_cookie = _call(f"{base}/app-data/replay-capability", "POST", {})
    if status != 200 or body.get("ok") is not True:
        raise SystemExit(f"capability issue failed: {status} {body}")
    if secret in raw or secret in set_cookie:
        raise SystemExit("capability response contains the shared replay token")
    if "HttpOnly" not in set_cookie:
        raise SystemExit(f"capability cookie is not httpOnly: {set_cookie}")
    pair = set_cookie.split(";", 1)[0].strip()
    if not pair.startswith("ce_replay_capability="):
        raise SystemExit(f"capability cookie missing: {set_cookie}")
    return pair


def _page_text(url: str, headers: dict[str, str] | None = None) -> str:
    request = urllib.request.Request(url, headers=headers or {}, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.read().decode()


def main() -> None:
    base = os.environ["PLAYWRIGHT_BASE_URL"].rstrip("/")
    gateway = os.environ["DASHBOARD_GATEWAY_URL"].rstrip("/")
    token = os.environ["REPLAY_API_TOKEN"]
    status, body, raw, _cookie = _call(f"{base}/app-data/replay/missing/start", "POST", {})
    if status != 401 or body.get("error") != "unauthorized":
        raise SystemExit(f"unauthorized replay post returned {status} {body}")
    if token and token in raw:
        raise SystemExit("401 response contains the shared replay token")

    presented, presented_body, presented_raw, _presented_cookie = _call(
        f"{base}/app-data/replay/missing/start",
        "POST",
        {},
        token=token,
    )
    if presented != 401 or presented_body.get("error") != "unauthorized":
        raise SystemExit(f"browser-supplied token was accepted: {presented} {presented_body}")
    if token in presented_raw:
        raise SystemExit("token-bearing 401 response echoed the shared replay token")

    overview_status, overview, overview_raw, _overview_cookie = _call(
        f"{gateway}/internal/overview", "GET", None
    )
    if overview_status != 200 or overview.get("ok") is not True:
        raise SystemExit(f"overview failed: {overview_status} {overview}")
    if token in overview_raw:
        raise SystemExit("overview contains the shared replay token")
    data = overview.get("data")
    if not isinstance(data, dict):
        raise SystemExit("overview payload was not an object")
    source = data.get("data_source")
    if not isinstance(source, dict) or not isinstance(source.get("session_id"), str):
        raise SystemExit("overview did not name a session")
    session_id = source["session_id"]

    page = _page_text(f"{base}/replay/{session_id}")
    if token in page:
        raise SystemExit("replay page HTML contains the shared replay token")
    flight = _page_text(
        f"{base}/replay/{session_id}",
        {"RSC": "1", "Next-Url": f"/replay/{session_id}"},
    )
    if token in flight:
        raise SystemExit("replay RSC payload contains the shared replay token")

    preview_status, preview, preview_raw, _preview_cookie = _call(
        f"{base}/app-data/replay/{session_id}", "GET", None
    )
    if preview_status != 200 or preview.get("ok") is not True:
        raise SystemExit(f"replay preview failed: {preview_status} {preview}")
    if token in preview_raw:
        raise SystemExit("replay preview response contains the shared replay token")
    preview_data = preview.get("data")
    if not isinstance(preview_data, dict):
        raise SystemExit("replay preview was not an object")
    last_ms = preview_data.get("last_ms")
    if not isinstance(last_ms, int):
        raise SystemExit("replay preview has no last_ms")

    first_cookie = _capability(base, token)
    second_cookie = _capability(base, token)
    first_status, first, first_raw, _first_cookie = _call(
        f"{base}/app-data/replay/{session_id}/start", "POST", {}, cookie=first_cookie
    )
    second_status, second, second_raw, _second_cookie = _call(
        f"{base}/app-data/replay/{session_id}/start", "POST", {}, cookie=second_cookie
    )
    if token in first_raw or token in second_raw:
        raise SystemExit("start response contains the shared replay token")
    if first_status != 200 or second_status != 200:
        raise SystemExit(
            f"authorized start failed: {first_status} {first} / {second_status} {second}"
        )
    first_data = first.get("data")
    second_data = second.get("data")
    if not isinstance(first_data, dict) or not isinstance(second_data, dict):
        raise SystemExit("start responses were not objects")
    first_id = first_data.get("replay_id")
    second_id = second_data.get("replay_id")
    if not isinstance(first_id, str) or not isinstance(second_id, str) or first_id == second_id:
        raise SystemExit(f"viewers were not forked: {first_id} {second_id}")
    if first_id == session_id or second_id == session_id:
        raise SystemExit("a viewer reused the recording id")

    early_status, early, _early_raw, _early_cookie = _call(
        f"{base}/app-data/replay/{first_id}/seek",
        "POST",
        {"timestamp_ms": 0},
        cookie=first_cookie,
    )
    late_status, late, _late_raw, _late_cookie = _call(
        f"{base}/app-data/replay/{second_id}/seek",
        "POST",
        {"timestamp_ms": last_ms},
        cookie=second_cookie,
    )
    if early_status != 200 or late_status != 200:
        raise SystemExit(f"seek failed: {early_status} {early} / {late_status} {late}")
    early_data = early.get("data")
    late_data = late.get("data")
    if not isinstance(early_data, dict) or not isinstance(late_data, dict):
        raise SystemExit("seek responses were not objects")
    if early_data.get("cursor") == late_data.get("cursor"):
        raise SystemExit(f"viewers share a cursor: {early_data.get('cursor')}")

    crossed_status, crossed, crossed_raw, _crossed_cookie = _call(
        f"{base}/app-data/replay/{second_id}/seek",
        "POST",
        {"timestamp_ms": 0},
        cookie=first_cookie,
    )
    if crossed_status != 403 or crossed.get("error") != "replay_forbidden":
        raise SystemExit(f"cross-viewer seek was not rejected: {crossed_status} {crossed}")
    if token in crossed_raw:
        raise SystemExit("forbidden response contains the shared replay token")

    again_status, again, again_raw, _again_cookie = _call(
        f"{base}/app-data/replay/{second_id}", "GET", None
    )
    if again_status != 200:
        raise SystemExit(f"viewer read failed: {again_status} {again}")
    if token in again_raw:
        raise SystemExit("viewer read contains the shared replay token")
    again_data = again.get("data")
    if not isinstance(again_data, dict) or again_data.get("cursor") != late_data.get("cursor"):
        raise SystemExit("the second viewer cursor moved when another capability posted")
    print(
        f"Replay proxy: 401 without a token; viewers {first_id} and {second_id} "
        f"seek to cursors {early_data.get('cursor')} and {late_data.get('cursor')}."
    )


if __name__ == "__main__":
    main()

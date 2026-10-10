"""Hit the Next.js replay proxy: unauthorized posts are 401, and two viewers seek apart.

Run while ``scripts/e2e.sh`` has the gateway and the Next server up.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request


def _call(
    url: str,
    method: str,
    token: str | None,
    body: dict[str, object] | None,
) -> tuple[int, dict[str, object]]:
    data = None if body is None else json.dumps(body).encode()
    headers: dict[str, str] = {}
    if data is not None:
        headers["content-type"] = "application/json"
    if token:
        headers["x-replay-token"] = token
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = json.loads(response.read().decode())
            status = response.status
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        payload = json.loads(raw) if raw else {}
        status = exc.code
    if not isinstance(payload, dict):
        raise SystemExit(f"{method} {url} did not return an object")
    return status, payload


def main() -> None:
    base = os.environ["PLAYWRIGHT_BASE_URL"].rstrip("/")
    gateway = os.environ["DASHBOARD_GATEWAY_URL"].rstrip("/")
    token = os.environ["REPLAY_API_TOKEN"]
    status, body = _call(f"{base}/app-data/replay/missing/start", "POST", None, {})
    if status != 401 or body.get("error") != "unauthorized":
        raise SystemExit(f"unauthorized replay post returned {status} {body}")

    overview_status, overview = _call(f"{gateway}/internal/overview", "GET", None, None)
    if overview_status != 200 or overview.get("ok") is not True:
        raise SystemExit(f"overview failed: {overview_status} {overview}")
    data = overview.get("data")
    if not isinstance(data, dict):
        raise SystemExit("overview payload was not an object")
    source = data.get("data_source")
    if not isinstance(source, dict) or not isinstance(source.get("session_id"), str):
        raise SystemExit("overview did not name a session")
    session_id = source["session_id"]

    preview_status, preview = _call(f"{base}/app-data/replay/{session_id}", "GET", None, None)
    if preview_status != 200 or preview.get("ok") is not True:
        raise SystemExit(f"replay preview failed: {preview_status} {preview}")
    preview_data = preview.get("data")
    if not isinstance(preview_data, dict):
        raise SystemExit("replay preview was not an object")
    last_ms = preview_data.get("last_ms")
    if not isinstance(last_ms, int):
        raise SystemExit("replay preview has no last_ms")

    first_status, first = _call(f"{base}/app-data/replay/{session_id}/start", "POST", token, {})
    second_status, second = _call(f"{base}/app-data/replay/{session_id}/start", "POST", token, {})
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

    early_status, early = _call(
        f"{base}/app-data/replay/{first_id}/seek", "POST", token, {"timestamp_ms": 0}
    )
    late_status, late = _call(
        f"{base}/app-data/replay/{second_id}/seek",
        "POST",
        token,
        {"timestamp_ms": last_ms},
    )
    if early_status != 200 or late_status != 200:
        raise SystemExit(f"seek failed: {early_status} {early} / {late_status} {late}")
    early_data = early.get("data")
    late_data = late.get("data")
    if not isinstance(early_data, dict) or not isinstance(late_data, dict):
        raise SystemExit("seek responses were not objects")
    if early_data.get("cursor") == late_data.get("cursor"):
        raise SystemExit(f"viewers share a cursor: {early_data.get('cursor')}")
    again_status, again = _call(f"{base}/app-data/replay/{second_id}", "GET", None, None)
    if again_status != 200:
        raise SystemExit(f"viewer read failed: {again_status} {again}")
    again_data = again.get("data")
    if not isinstance(again_data, dict) or again_data.get("cursor") != late_data.get("cursor"):
        raise SystemExit("the second viewer cursor moved when the first was read")
    print(
        f"Replay proxy: 401 without a token; viewers {first_id} and {second_id} "
        f"seek to cursors {early_data.get('cursor')} and {late_data.get('cursor')}."
    )


if __name__ == "__main__":
    main()

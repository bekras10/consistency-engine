"""Lightweight benchmark smoke against the configured PostgreSQL.

Asserts the process exits 0 and the JSON has the required fields. It does not
assert a throughput target.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import scripts.benchmark as benchmark
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[2]


def test_benchmark_smoke_exits_zero(database_url: str, tmp_path: Path) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["PYTHONPATH"] = benchmark._pythonpath()
    env["PYTHONUNBUFFERED"] = "1"
    parsed = make_url(database_url)
    port = 5432 if parsed.port is None else parsed.port
    if port == 5432:
        env["BENCHMARK_ALLOW_PORT_5432"] = "1"
    else:
        env.pop("BENCHMARK_ALLOW_PORT_5432", None)
    out = tmp_path / "smoke.json"
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "benchmark.py"), "--smoke", "--output", str(out)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr[-4000:] + "\n" + proc.stdout[-2000:]
    report = json.loads(out.read_text(encoding="utf-8"))
    assert benchmark.smoke_report_errors(report) == []
    assert report["kalshi_contacted"] is False

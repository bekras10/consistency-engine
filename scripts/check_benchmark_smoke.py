"""Exit 0 only when a benchmark smoke JSON has the required fields.

This does not check a performance target.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.benchmark import smoke_report_errors  # noqa: E402


def main() -> None:
    if len(sys.argv) != 2:
        print("usage: check_benchmark_smoke.py RESULT.json", file=sys.stderr)
        raise SystemExit(2)
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    errors = smoke_report_errors(report)
    if errors:
        print("smoke report missing: " + ", ".join(errors), file=sys.stderr)
        raise SystemExit(1)
    workloads = report["workloads"]
    if any(row.get("status") != "ok" for row in workloads):
        print("smoke workload did not finish ok", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()

"""Worker entry point. The long-running detection worker is a milestone-2 deliverable."""

from __future__ import annotations

import sys


def main() -> int:
    sys.stderr.write(
        "consistency-worker: the detection pipeline is not yet implemented (milestone 2).\n"
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

"""Generate synthetic datasets deterministically.

uv run python scripts/generate_datasets.py --bundled          # smoke + inconsistent -> fixtures
uv run python scripts/generate_datasets.py --bundled --check  # verify bundled files are exact
uv run python scripts/generate_datasets.py --all              # all six (others -> generated/)
uv run python scripts/generate_datasets.py --name corruption
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from consistency_simulation.datasets import BUNDLED, PRESETS, build, read_text, render, write

ROOT = Path(__file__).resolve().parents[1]
DATASETS = ROOT / "fixtures" / "datasets"


def target_dir(name: str) -> Path:
    return DATASETS / name if name in BUNDLED else DATASETS / "generated" / name


def main() -> int:
    ap = argparse.ArgumentParser()
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--bundled", action="store_true")
    group.add_argument("--all", action="store_true")
    group.add_argument("--name", choices=sorted(PRESETS))
    ap.add_argument("--check", action="store_true", help="compare instead of writing")
    args = ap.parse_args()
    names = list(BUNDLED) if args.bundled else sorted(PRESETS) if args.all else [args.name]
    failed = False
    for name in names:
        result = build(name)
        out = target_dir(name)
        if args.check:
            for fname, text in render(result).items():
                if read_text(out, fname) != text:
                    print(f"MISMATCH {out / fname}")
                    failed = True
            print(f"checked {name}: {'FAIL' if failed else 'ok'}")
        else:
            write(result, out)
            print(f"wrote {name}: {result.counts.get('messages', 0)} messages -> {out}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

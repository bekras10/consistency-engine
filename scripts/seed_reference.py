"""Load the bundled synthetic catalog, relationships, reviews, and fee schedules.

Usage: DATABASE_URL=postgresql+asyncpg://... python scripts/seed_reference.py
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

from consistency_connectors.settings import Settings
from consistency_core.fees.schedule import FeeScheduleRegistry
from consistency_persistence.seed import load_reference
from consistency_simulation.datasets import load
from consistency_simulation.families import SIM_EPOCH
from consistency_worker.bootstrap import relationships_for

ROOT = Path(__file__).resolve().parents[1]


def main(argv: list[str] | None = None) -> int:
    del argv
    settings = Settings()
    if not settings.database_url:
        sys.stderr.write("make seed: DATABASE_URL is required (PostgreSQL, never SQLite)\n")
        return 2
    env = os.environ.copy()
    env["DATABASE_URL"] = settings.database_url
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env=env,
        check=True,
    )
    dataset = ROOT / "fixtures/datasets/inconsistent"
    catalog = load(dataset).catalog
    relationships = relationships_for(
        catalog, Path(settings.relationship_reviews_path), as_of=SIM_EPOCH
    )
    schedules = list(FeeScheduleRegistry.from_directory(Path(settings.fee_schedule_dir)).schedules)
    counts = asyncio.run(
        load_reference(
            settings.database_url,
            catalog,
            relationships,
            schedules,
            source_id="synthetic",
            source_name="synthetic:inconsistent",
            source_kind="synthetic",
        )
    )
    print(
        "seeded synthetic reference data: "
        f"{counts['markets']} markets, {counts['relationships']} relationships, "
        f"{counts['reviews']} reviews, {counts['fee_schedules']} fee schedules"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

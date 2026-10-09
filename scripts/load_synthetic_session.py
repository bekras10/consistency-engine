"""Load one fast synthetic session when the database has no detections.

The worker runs the real pipeline. The dashboard does not invent these rows.
"""

from __future__ import annotations

import asyncio
import os

from sqlalchemy import func, select

from consistency_connectors.settings import Settings
from consistency_persistence.schema import DetectionRow
from consistency_persistence.store import PersistenceStore
from consistency_worker.bootstrap import build_inputs
from consistency_worker.service import WorkerService
from consistency_worker.store import DatabaseSessionStore


async def _detection_count(url: str) -> int:
    store = PersistenceStore(url)
    try:
        async with store.sessions() as session:
            count = await session.scalar(select(func.count()).select_from(DetectionRow))
        return int(count or 0)
    finally:
        await store.aclose()


async def main() -> None:
    os.environ.setdefault("DATA_SOURCE", "synthetic")
    os.environ.setdefault("SYNTHETIC_PRESET", "inconsistent")
    settings = Settings()
    if not settings.database_url:
        raise SystemExit("DATABASE_URL is required")
    existing = await _detection_count(settings.database_url)
    if existing > 0:
        print(f"detections already stored: {existing}")
        return
    inputs = await build_inputs(settings, fast=True)
    worker_store = DatabaseSessionStore(settings)
    try:
        result = await WorkerService(settings, inputs=inputs, store=worker_store).run()
    finally:
        await worker_store.aclose()
    print(
        f"loaded synthetic session {result.session_id} "
        f"status={result.status} events={result.events}"
    )


if __name__ == "__main__":
    asyncio.run(main())

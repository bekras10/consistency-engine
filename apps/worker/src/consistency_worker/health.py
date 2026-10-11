"""Worker lifecycle health. PostgreSQL connectivity alone is not a healthy worker.

The latest ingestion session for this database is the contract:

* ``running`` — ingestion is in progress. Exit 0.
* ``completed`` — the deterministic session finished. That is an expected terminal
  state. Exit 0.
* ``interrupted`` — a deterministic session stopped before the journal ended and
  the next start will resume it. Exit 0.
* ``failed`` — processing failed. Exit 1.
* no session yet — ``starting``. Exit 1.

The printed word is the status. Compose treats exit 0 as healthy, so a completed
demo and an active run are both healthy and are distinguished by that word.
A failed session is unhealthy even when PostgreSQL answers.
"""

from __future__ import annotations

import logging

from sqlalchemy import select

from consistency_persistence.schema import IngestionSessionRow
from consistency_persistence.store import PersistenceStore

log = logging.getLogger("consistency.worker.health")


def describe_health(status: str | None) -> tuple[str, int]:
    if status == "completed":
        return "completed", 0
    if status == "running":
        return "running", 0
    if status == "interrupted":
        return "interrupted", 0
    if status == "failed":
        return "failed", 1
    if status is None:
        return "starting", 1
    return "unhealthy", 1


async def report_health(url: str) -> tuple[str, int]:
    store = PersistenceStore(url)
    try:
        async with store.sessions() as session:
            status = await session.scalar(
                select(IngestionSessionRow.status)
                .order_by(
                    IngestionSessionRow.started_at.desc(),
                    IngestionSessionRow.session_id.desc(),
                )
                .limit(1)
            )
    except Exception:
        log.warning("worker health could not read ingestion status")
        return "unhealthy", 1
    finally:
        await store.aclose()
    if not isinstance(status, str):
        return describe_health(None)
    return describe_health(status)

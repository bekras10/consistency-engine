"""Worker lifecycle health. PostgreSQL connectivity alone is not a healthy worker.

Ingestion status and maintenance freshness are separate.

The latest ingestion session for this database:

* ``running`` — ingestion is in progress. Exit 0, unless maintenance is required and stale.
* ``completed`` — the deterministic session finished. That is an expected terminal
  state. Exit 0 when maintenance is fresh or not required.
* ``interrupted`` — a deterministic session stopped before the journal ended and
  the next start will resume it. Exit 0, unless maintenance is required and stale.
* ``failed`` — processing failed. Exit 1. A fresh heartbeat does not hide this.
* no session yet — ``starting``. Exit 1.

When ``EXPECT_MAINTENANCE=1`` (the worker entrypoint and the compose worker
service set this), a missing or old heartbeat file is ``unhealthy`` / 1 even if
the last ingestion session is ``completed``. The maintenance process rewrites
that file on a short interval. A host ``health`` command that does not expect
maintenance still treats ``completed`` as healthy.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

from sqlalchemy import select

from consistency_persistence.schema import IngestionSessionRow
from consistency_persistence.store import PersistenceStore

log = logging.getLogger("consistency.worker.health")

_DEFAULT_HEARTBEAT = Path("/tmp/consistency-worker/heartbeat")
_DEFAULT_STALE_S = 30.0
_DEFAULT_INTERVAL_S = 5.0


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


def expect_maintenance() -> bool:
    return os.environ.get("EXPECT_MAINTENANCE", "").strip() == "1"


def heartbeat_path() -> Path:
    raw = os.environ.get("MAINTENANCE_HEARTBEAT_PATH", "").strip()
    if raw:
        return Path(raw)
    return _DEFAULT_HEARTBEAT


def stale_after_s() -> float:
    return _positive_float("MAINTENANCE_STALE_S", _DEFAULT_STALE_S)


def heartbeat_interval_s() -> float:
    return _positive_float("MAINTENANCE_HEARTBEAT_S", _DEFAULT_INTERVAL_S)


def _positive_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    if value <= 0:
        return default
    return value


def read_heartbeat_epoch(path: Path) -> float | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def write_heartbeat(path: Path, *, now: float | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = time.time() if now is None else now
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(f"{stamp:.6f}\n", encoding="utf-8")
    os.replace(temporary, path)


def heartbeat_is_fresh(epoch: float | None, *, now: float, stale_after_s: float) -> bool:
    if epoch is None:
        return False
    return (now - epoch) <= stale_after_s


def assess_health(
    status: str | None,
    *,
    expect_maintenance: bool,
    heartbeat_epoch: float | None,
    now: float,
    stale_after_s: float,
) -> tuple[str, int]:
    """Session status first. A failed ingestion stays failed.

    Maintenance freshness is checked only when the session status would
    otherwise exit 0 and the entrypoint expects the maintenance process.
    """
    label, code = describe_health(status)
    if code != 0:
        return label, code
    if expect_maintenance and not heartbeat_is_fresh(
        heartbeat_epoch, now=now, stale_after_s=stale_after_s
    ):
        return "unhealthy", 1
    return label, code


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
    session_status = status if isinstance(status, str) else None
    required = expect_maintenance()
    epoch = read_heartbeat_epoch(heartbeat_path()) if required else None
    return assess_health(
        session_status,
        expect_maintenance=required,
        heartbeat_epoch=epoch,
        now=time.time(),
        stale_after_s=stale_after_s(),
    )

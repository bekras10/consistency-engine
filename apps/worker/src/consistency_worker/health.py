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
that file on a short interval, and only while its retention task is still
running. A fresh timestamp by itself is not enough: the file must say
``retention=running`` and name a live process. It also records
``consecutive_failures`` and ``last_success``. Health is ``unhealthy`` when
consecutive failed retention cycles exceed ``MAINTENANCE_MAX_FAILURES``
(default 3), even if the process is alive and the timestamp is fresh. A cycle
that finishes records ``last_success`` and resets the failure count. A
``last_success`` older than ``MAINTENANCE_SUCCESS_MAX_AGE_S`` (default 7200)
is unhealthy even when the process is alive. Before the first success,
``MAINTENANCE_STARTUP_GRACE_S`` (default one retention interval plus 60
seconds, measured from ``started_at``) keeps the process healthy. A stale
success is not covered by that grace. A host ``health`` command that does not
expect maintenance still treats ``completed`` as healthy.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select

from consistency_persistence.schema import IngestionSessionRow
from consistency_persistence.store import PersistenceStore

log = logging.getLogger("consistency.worker.health")

_DEFAULT_HEARTBEAT = Path("/tmp/consistency-worker/heartbeat")
_DEFAULT_STALE_S = 30.0
_DEFAULT_INTERVAL_S = 5.0
_DEFAULT_MAX_FAILURES = 3
_DEFAULT_SUCCESS_MAX_AGE_S = 7200.0
_DEFAULT_RETENTION_INTERVAL_S = 3600.0
_DEFAULT_STARTUP_MARGIN_S = 60.0


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


def success_max_age_s() -> float:
    """Max age of ``last_success`` before a live process is unhealthy."""
    return _positive_float("MAINTENANCE_SUCCESS_MAX_AGE_S", _DEFAULT_SUCCESS_MAX_AGE_S)


def startup_grace_s() -> float:
    """How long a new process may run before the first successful cycle.

    The retention task sleeps for one interval before its first cycle, so the
    default grace is that interval plus 60 seconds. An explicit
    ``MAINTENANCE_STARTUP_GRACE_S`` replaces the default. A recorded
    ``last_success`` that is already stale is unhealthy during the grace window.
    """
    raw = os.environ.get("MAINTENANCE_STARTUP_GRACE_S", "").strip()
    if raw:
        return _positive_float("MAINTENANCE_STARTUP_GRACE_S", _DEFAULT_STARTUP_MARGIN_S)
    interval = _positive_float("RETENTION_INTERVAL_S", _DEFAULT_RETENTION_INTERVAL_S)
    return interval + _DEFAULT_STARTUP_MARGIN_S


def max_consecutive_failures() -> int:
    """Failed retention cycles above this count are unhealthy. Default is 3."""
    raw = os.environ.get("MAINTENANCE_MAX_FAILURES", "").strip()
    if not raw:
        return _DEFAULT_MAX_FAILURES
    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_MAX_FAILURES
    if value < 0:
        return _DEFAULT_MAX_FAILURES
    return value


@dataclass
class RetentionProgress:
    """In-process count. The heartbeat file is what ``health`` reads."""

    consecutive_failures: int = 0
    last_success: float | None = None
    started_at: float | None = None


_progress = RetentionProgress()


def note_maintenance_start(now: float | None = None) -> None:
    """Record the process start once. Later heartbeats keep this timestamp."""
    if _progress.started_at is None:
        _progress.started_at = time.time() if now is None else now


def note_retention_success(now: float | None = None) -> None:
    _progress.consecutive_failures = 0
    _progress.last_success = time.time() if now is None else now


def note_retention_failure() -> None:
    _progress.consecutive_failures += 1


def retention_progress() -> RetentionProgress:
    return _progress


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


@dataclass(frozen=True)
class Heartbeat:
    epoch: float | None
    retention: str | None
    pid: int | None
    consecutive_failures: int = 0
    last_success: float | None = None
    started_at: float | None = None


def read_heartbeat(path: Path) -> Heartbeat:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return Heartbeat(None, None, None)
    epoch: float | None = None
    retention: str | None = None
    pid: int | None = None
    consecutive_failures = 0
    last_success: float | None = None
    started_at: float | None = None
    for line in text.splitlines():
        item = line.strip()
        if not item:
            continue
        if item.startswith("retention="):
            retention = item.split("=", 1)[1]
            continue
        if item.startswith("pid="):
            try:
                pid = int(item.split("=", 1)[1])
            except ValueError:
                pid = None
            continue
        if item.startswith("consecutive_failures="):
            try:
                consecutive_failures = int(item.split("=", 1)[1])
            except ValueError:
                consecutive_failures = 0
            continue
        if item.startswith("last_success="):
            raw_success = item.split("=", 1)[1]
            if raw_success == "none":
                last_success = None
            else:
                try:
                    last_success = float(raw_success)
                except ValueError:
                    last_success = None
            continue
        if item.startswith("started_at="):
            raw_started = item.split("=", 1)[1]
            if raw_started == "none":
                started_at = None
            else:
                try:
                    started_at = float(raw_started)
                except ValueError:
                    started_at = None
            continue
        if epoch is None:
            try:
                epoch = float(item)
            except ValueError:
                continue
    return Heartbeat(epoch, retention, pid, consecutive_failures, last_success, started_at)


def pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def write_heartbeat(
    path: Path,
    *,
    now: float | None = None,
    retention: str = "running",
    pid: int | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = time.time() if now is None else now
    process = os.getpid() if pid is None else pid
    failures = _progress.consecutive_failures
    success = "none" if _progress.last_success is None else f"{_progress.last_success:.6f}"
    if _progress.started_at is None:
        _progress.started_at = stamp
    started = f"{_progress.started_at:.6f}"
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        (
            f"{stamp:.6f}\nretention={retention}\npid={process}\n"
            f"consecutive_failures={failures}\nlast_success={success}\n"
            f"started_at={started}\n"
        ),
        encoding="utf-8",
    )
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
    retention_state: str | None = None,
    process_alive: bool | None = None,
    consecutive_failures: int = 0,
    max_failures: int = _DEFAULT_MAX_FAILURES,
    last_success: float | None = None,
    started_at: float | None = None,
    success_max_age_s: float = _DEFAULT_SUCCESS_MAX_AGE_S,
    startup_grace_s: float = _DEFAULT_RETENTION_INTERVAL_S + _DEFAULT_STARTUP_MARGIN_S,
) -> tuple[str, int]:
    """Session status first. A failed ingestion stays failed.

    Maintenance is checked only when the session status would otherwise exit 0
    and the entrypoint expects the maintenance process. A fresh timestamp is
    not enough: the retention task must still be attested as running inside a
    live process, consecutive failed cycles must not exceed ``max_failures``,
    and the last successful cycle must be newer than ``success_max_age_s``.
    A missing success is healthy only while ``now - started_at`` is inside
    ``startup_grace_s``.
    """
    label, code = describe_health(status)
    if code != 0:
        return label, code
    if not expect_maintenance:
        return label, code
    if retention_state != "running" or process_alive is not True:
        return "unhealthy", 1
    if not heartbeat_is_fresh(heartbeat_epoch, now=now, stale_after_s=stale_after_s):
        return "unhealthy", 1
    if consecutive_failures > max_failures:
        return "unhealthy", 1
    if last_success is not None:
        if (now - last_success) > success_max_age_s:
            return "unhealthy", 1
    elif started_at is None or (now - started_at) > startup_grace_s:
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
    beat = read_heartbeat(heartbeat_path()) if required else Heartbeat(None, None, None)
    alive = pid_is_alive(beat.pid) if beat.pid is not None else None
    return assess_health(
        session_status,
        expect_maintenance=required,
        heartbeat_epoch=beat.epoch,
        now=time.time(),
        stale_after_s=stale_after_s(),
        retention_state=beat.retention,
        process_alive=alive,
        consecutive_failures=beat.consecutive_failures,
        max_failures=max_consecutive_failures(),
        last_success=beat.last_success,
        started_at=beat.started_at,
        success_max_age_s=success_max_age_s(),
        startup_grace_s=startup_grace_s(),
    )

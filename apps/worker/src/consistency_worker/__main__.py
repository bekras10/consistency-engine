"""Worker entry point: ``python -m consistency_worker [run|maintain|health]``.

``run`` (default) starts detection for the configured data source (``DATA_SOURCE``,
default ``synthetic``). After a terminal session (``completed`` or ``failed``) the
process stays up until SIGINT / SIGTERM so a supervisor does not open another
ingestion. The latest session for a fingerprint decides. A later failed or
interrupted reseed is resumed. A latest completed session is not ingested
again unless ``--reseed`` or ``RESEED_SESSION=true``.

``maintain`` runs retention until signaled. The heartbeat is rewritten on a
short interval only while that retention task is still running. If the task
exits or fails, the process records that and exits 1. It does not ingest.

``health`` prints the latest ingestion status and exits 0 for ``running``,
``completed``, and ``interrupted`` when maintenance is fresh or not required.
``failed`` exits 1. A stale maintenance heartbeat exits 1 with ``unhealthy``
even if the last ingestion session completed. ``EXPECT_MAINTENANCE=1`` is what
turns that check on.

Exit status of ``run``: 0 on completion, idle stop, or graceful interrupt; 1 is not
used after a clean failed session because the process stays up for the healthcheck
and exits 0 when signaled. Configuration errors exit 2.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from consistency_connectors.settings import ConfigurationError, Settings
from consistency_connectors.sources.kalshi import KalshiAccessRefusedError
from consistency_worker import logs
from consistency_worker.health import (
    heartbeat_interval_s,
    heartbeat_path,
    report_health,
    write_heartbeat,
)
from consistency_worker.service import SessionStore, WorkerService
from consistency_worker.store import DatabaseSessionStore

log = logging.getLogger("consistency.worker")


async def _store(settings: Settings) -> SessionStore | None:
    if not settings.database_url:
        log.warning("DATABASE_URL is not set: running without persistence")
        return None
    store = DatabaseSessionStore(settings)
    try:
        await store.ping()
    except Exception as exc:
        await store.aclose()
        log.error("database unreachable", extra={"error": type(exc).__name__})
        raise ConfigurationError("database unreachable") from exc
    log.info("persistence enabled", extra=settings.persistence_summary())
    return store


def _signals(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)


async def _run(settings: Settings) -> int:
    stop = asyncio.Event()
    _signals(stop)
    store = await _store(settings)
    try:
        result = await WorkerService(settings, store=store).run(stop)
        if not stop.is_set() and result.status in {"completed", "failed"}:
            log.info(
                "session is terminal; waiting for SIGTERM",
                extra={"session_id": result.session_id, "status": result.status},
            )
            await stop.wait()
        return 0
    finally:
        close = getattr(store, "aclose", None)
        if close is not None:
            await close()


async def _retention_task(store: DatabaseSessionStore) -> None:
    """Run retention unless a test asked this task to exit or fail."""
    fault = os.environ.get("RETENTION_FAULT", "").strip()
    if fault == "exit":
        return
    if fault == "fail":
        raise RuntimeError("injected retention failure")
    await store.retention_loop()


async def _pulse(stop: asyncio.Event, retention: asyncio.Task[None]) -> None:
    """Rewrite the heartbeat only while the retention task is still running.

    A timestamp written after that task has exited would hide the failure.
    Retention may sleep for an hour, so this interval stays short.
    """
    path = heartbeat_path()
    interval = heartbeat_interval_s()
    while not stop.is_set():
        if retention.done():
            return
        try:
            write_heartbeat(path, retention="running")
        except OSError:
            log.warning("maintenance heartbeat was not written", extra={"path": str(path)})
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue
        return


async def _maintain(settings: Settings) -> int:
    if not settings.database_url:
        log.error("DATABASE_URL is required for maintenance")
        return 2
    stop = asyncio.Event()
    _signals(stop)
    store = DatabaseSessionStore(settings)
    try:
        await store.ping()
    except Exception as exc:
        await store.aclose()
        log.error("database unreachable", extra={"error": type(exc).__name__})
        return 2
    task = asyncio.create_task(_retention_task(store), name="retention")
    pulse = asyncio.create_task(_pulse(stop, task), name="maintenance-heartbeat")
    stop_wait = asyncio.create_task(stop.wait())
    await asyncio.wait({task, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
    pulse.cancel()
    await asyncio.gather(pulse, return_exceptions=True)
    if stop.is_set():
        task.cancel()
        stop_wait.cancel()
        await asyncio.gather(task, stop_wait, return_exceptions=True)
        await store.aclose()
        return 0
    stop_wait.cancel()
    await asyncio.gather(stop_wait, return_exceptions=True)
    retention = "failed"
    if task.done() and not task.cancelled():
        failure = task.exception()
        retention = "failed" if failure is not None else "exited"
        if failure is not None:
            log.error("retention task failed", extra={"error": type(failure).__name__})
        else:
            log.error("retention task exited")
    try:
        write_heartbeat(heartbeat_path(), retention=retention)
    except OSError:
        log.warning("maintenance failure was not recorded")
    await store.aclose()
    return 1


async def _health(settings: Settings) -> int:
    if not settings.database_url:
        print("starting")
        return 1
    label, code = await report_health(settings.database_url)
    print(label)
    return code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="consistency-worker")
    parser.add_argument("command", nargs="?", default="run", choices=["run", "maintain", "health"])
    parser.add_argument(
        "--reseed",
        action="store_true",
        help=(
            "Allocate a new ingestion session even when this fingerprint already "
            "completed. Default off. Same as RESEED_SESSION=true."
        ),
    )
    args = parser.parse_args(argv)
    if args.reseed:
        os.environ["RESEED_SESSION"] = "true"
    try:
        settings = Settings()
    except (ConfigurationError, ValueError) as exc:
        sys.stderr.write(f"consistency-worker: configuration refused: {exc}\n")
        return 2
    if args.command == "health":
        return asyncio.run(_health(settings))
    logs.configure(settings.log_level)
    try:
        if args.command == "maintain":
            return asyncio.run(_maintain(settings))
        return asyncio.run(_run(settings))
    except (KalshiAccessRefusedError, NotImplementedError) as exc:
        log.error("kalshi data source refused", extra={"error": str(exc)})
        return 2
    except ConfigurationError as exc:
        log.error("worker configuration refused", extra={"error": str(exc)})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

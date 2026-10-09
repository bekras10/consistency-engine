"""Worker entry point: ``python -m consistency_worker [run]``.

``run`` (default) starts the long-running detection service for the configured data source
(``DATA_SOURCE``, default ``synthetic``). SIGINT / SIGTERM trigger a graceful shutdown. Exit
status: 0 on completion or graceful stop, 1 when the session failed, 2 on configuration errors.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from consistency_connectors.settings import ConfigurationError, Settings
from consistency_connectors.sources.kalshi import KalshiAccessRefusedError
from consistency_worker import logs
from consistency_worker.service import SessionStore, WorkerService

log = logging.getLogger("consistency.worker")


async def _store(settings: Settings) -> SessionStore | None:
    if settings.database_url:
        log.warning("persistence is not wired in this build: running without persistence")
    else:
        log.warning("DATABASE_URL is not set: running without persistence")
    return None


async def _run(settings: Settings) -> int:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    store = await _store(settings)
    try:
        result = await WorkerService(settings, store=store).run(stop)
    finally:
        close = getattr(store, "aclose", None)
        if close is not None:
            await close()
    return 1 if result.status == "failed" else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="consistency-worker")
    parser.add_argument("command", nargs="?", default="run", choices=["run"])
    parser.parse_args(argv)
    try:
        settings = Settings()
    except (ConfigurationError, ValueError) as exc:
        sys.stderr.write(f"consistency-worker: configuration refused: {exc}\n")
        return 2
    logs.configure(settings.log_level)
    try:
        return asyncio.run(_run(settings))
    except (KalshiAccessRefusedError, NotImplementedError) as exc:
        log.error("kalshi data source refused", extra={"error": str(exc)})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

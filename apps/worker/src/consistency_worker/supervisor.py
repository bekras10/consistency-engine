"""Restart policy around :class:`IngestionRunner`.

The runner already retries transient connection errors and heartbeat timeouts itself. When it
stops FAILED (live end of stream, reconnect budget exhausted, source error, recovery failure)
it has published UNSYNCHRONIZED for every affected market before returning, so the books and
every detection built on them are already fail-closed. The supervisor then waits per the
backoff policy and starts a fresh runner on the same ``BookManager`` + listener: the new
subscription's snapshots resynchronize the books, and detections must re-qualify from scratch
(including the minimum duration). Authentication failures are never retried. The restart
counter resets once a restarted runner has processed ``backoff.reset_after_messages`` messages.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from consistency_connectors.ingestion import Backoff, IngestionRunner

log = logging.getLogger("consistency.worker.supervisor")


@dataclass
class SupervisorStats:
    restarts: int = 0
    failures: list[str] = field(default_factory=list)


class Supervisor:
    def __init__(
        self,
        make_runner: Callable[[], IngestionRunner],
        *,
        backoff: Backoff | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.make_runner = make_runner
        self.backoff = backoff or Backoff()
        self._sleep = sleep
        self.stats = SupervisorStats()
        self.runner: IngestionRunner | None = None

    async def run(self) -> str | None:
        """Run until the stream completes (``None``) or a non-retryable / exhausted failure
        (the failure text)."""
        delays = self.backoff.delays()
        attempt = 0
        while True:
            runner = self.make_runner()
            self.runner = runner
            await runner.run()
            if runner.failed is None:
                return None
            self.stats.failures.append(runner.failed)
            if runner.failed.startswith("authentication"):
                log.error(
                    "source authentication failed; not retrying", extra={"failure": runner.failed}
                )
                return runner.failed
            if runner.stats.messages >= self.backoff.reset_after_messages:
                attempt = 0
            if attempt >= len(delays):
                restarts = self.stats.restarts
                return f"restart budget exhausted after {restarts} restarts: {runner.failed}"
            self.stats.restarts += 1
            log.warning(
                "ingestion runner failed; restarting",
                extra={
                    "failure": runner.failed,
                    "attempt": attempt + 1,
                    "delay_s": delays[attempt],
                },
            )
            await self._sleep(delays[attempt])
            attempt += 1

"""ReplayDataSource: deterministic playback of a recorded dataset directory."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path

from consistency_connectors.sources.recorded import RecordedStreamSource
from consistency_core.models.common import DataSourceKind
from consistency_simulation.datasets import load


class ReplayDataSource(RecordedStreamSource):
    def __init__(
        self,
        dataset_dir: Path,
        *,
        speed: float | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        ds = load(dataset_dir)
        self.metadata = ds.metadata
        super().__init__(DataSourceKind.REPLAY, ds.catalog, ds.messages, speed=speed, sleep=sleep)

"""Periodic checkpoints of book + engine state (spec 12.2).

A checkpoint taken after journal entry ``ordinal`` holds the complete ``BookManager`` and
``DetectionEngine`` state at that point. Restoring it and applying entries ``ordinal + 1 ...``
gives exactly the state (and lifecycle events) of applying every entry from the start; the
replay tests assert this. ``state_digest`` covers the deterministic parts only (telemetry such
as ``perf_counter_ns`` inside detection records is excluded).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from pydantic import JsonValue

from consistency_connectors.ingestion import BookManager
from consistency_core.fees import FeeCalculator
from consistency_core.models.common import FrozenModel
from consistency_core.serialization import sha256_of
from consistency_pipeline.engine import DetectionEngine

_TELEMETRY_KEYS = frozenset({"processing_started_ns", "detection_completed_ns"})


class Checkpoint(FrozenModel):
    session_id: str
    ordinal: int
    """Last journal ordinal applied before the snapshot was taken."""
    now_ms: int
    source_position: int | None
    """``StreamMessage.position`` of the last applied message (resume point for recorded
    sources: playback restarts at ``source_position + 1``)."""
    manager_state: dict[str, JsonValue]
    engine_state: dict[str, JsonValue]
    state_digest: str


def strip_telemetry(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: strip_telemetry(v) for k, v in value.items() if k not in _TELEMETRY_KEYS}
    if isinstance(value, list):
        return [strip_telemetry(v) for v in value]
    return value


def state_digest(manager_state: dict[str, Any], engine_state: dict[str, Any]) -> str:
    return sha256_of(
        {"manager": strip_telemetry(manager_state), "engine": strip_telemetry(engine_state)}
    )


def take(
    session_id: str,
    ordinal: int,
    now_ms: int,
    source_position: int | None,
    manager: BookManager,
    engine: DetectionEngine,
) -> Checkpoint:
    ms, es = manager.export_state(), engine.export_state()
    return Checkpoint(
        session_id=session_id,
        ordinal=ordinal,
        now_ms=now_ms,
        source_position=source_position,
        manager_state=ms,
        engine_state=es,
        state_digest=state_digest(ms, es),
    )


def restore(
    cp: Checkpoint,
    fees: FeeCalculator,
    *,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
) -> tuple[BookManager, DetectionEngine]:
    manager = BookManager.from_state(dict(cp.manager_state), clock_ns=clock_ns)
    engine = DetectionEngine.from_state(dict(cp.engine_state), manager, fees, clock_ns=clock_ns)
    return manager, engine

"""Synthetic performance harness. Never contacts Kalshi.

Measures the real worker (ingestion, detection, PostgreSQL) on generated sessions.
Creates database ``consistency_bench`` on the same host and port as ``DATABASE_URL``
so the demo database is not filled with benchmark rows.

Database guard, before any connection, CREATE DATABASE, or Alembic migration:

* An omitted port is port 5432. Port 5432 is refused unless ``BENCHMARK_ALLOW_PORT_5432=1``.
* Hosts other than ``localhost``, ``127.0.0.1``, and ``::1`` are refused unless
  ``BENCHMARK_ALLOW_REMOTE=1``.
* Any other value of those variables, including ``true``, leaves the guard on.
* A refused URL is never migrated and never dropped.

``make benchmark`` runs this script. A missed speed target is reported and still exits 0.
Exit 1 means a mandatory workload timed out, was skipped, failed, missed a recovery
segment, or did not produce the inconsistency lifecycle. Exit 2 means ``DATABASE_URL``
is missing or points at a refused server.

Cold-start throughput is the first observation window after the first listener entry.
Steady-state throughput is every later window of the same length. Session open is
reported on its own and is not averaged into either rate. Repetitions are separate
processes when ``--isolate`` is on (the default outside ``--smoke``).

Stdout is a text table. The JSON file is the raw record.
"""

from __future__ import annotations

import argparse
import asyncio
import cProfile
import io
import json
import logging
import os
import platform
import pstats
import resource
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import asyncpg
from sqlalchemy import text
from sqlalchemy.engine import make_url

import consistency_pipeline.engine as engine_mod
from consistency_connectors.base import ConnectionState
from consistency_connectors.ingestion.book_manager import BookManager
from consistency_connectors.ingestion.runner import IngestionRunner
from consistency_connectors.settings import Settings
from consistency_connectors.sources.synthetic import SyntheticDataSource
from consistency_core.models.common import SyncStatus
from consistency_core.pricing.evaluator import Evaluation
from consistency_core.serialization import sha256_of
from consistency_persistence.recording import DetectionWriter
from consistency_pipeline.lifecycle import DetectionEvent
from consistency_pipeline.replay import compare_outcomes, replay_catalog
from consistency_pipeline.service import Outbox, PipelineListener
from consistency_simulation.exchange import FaultKind, FaultSpec, SessionConfig, SyntheticExchange
from consistency_simulation.families import SIM_EPOCH
from consistency_simulation.stream import playback
from consistency_worker.bootstrap import SessionInputs, fee_calculator, relationships_for
from consistency_worker.service import WorkerResult, WorkerService
from consistency_worker.store import DatabaseSessionStore

ROOT = Path(__file__).resolve().parents[1]
BENCH_DB = "consistency_bench"
TICK_MS = 250
LATENCY_WINDOW = 10_000
LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
INCONSISTENT_SEED = 505
INCONSISTENT_DURATION_MS = 126_000
REQUIRED_LIFECYCLE = ("OPENED", "UPDATED", "RESOLVED", "EXPIRED")
RECOVERY_SEGMENT_KEYS = (
    "gap_detection_ms",
    "recovery_request_ms",
    "snapshot_arrival_ms",
    "full_resynchronization_ms",
)
COLD_SCOPE = (
    "first observation window after the first listener entry; "
    "includes ingestion, detection, PostgreSQL journal, and notification outbox; "
    "excludes session open and is not included in steady_state"
)
STEADY_SCOPE = (
    "later observation windows of the same length; "
    "includes ingestion, detection, PostgreSQL journal, and notification outbox; "
    "excludes session open and the cold-start window"
)
SEGMENT_SCOPE = {
    "gap_detection_ms": "BookManager.process on the message that flags the sequence gap",
    "recovery_request_ms": "source.request_recovery for that gap",
    "snapshot_arrival_ms": (
        "from request_recovery returning until the first desynced market is SYNCHRONIZED"
    ),
    "full_resynchronization_ms": (
        "from that first restoring snapshot until every market desynced by the gap is SYNCHRONIZED"
    ),
}
_SEGMENT_FIELDS = (
    ("gap_detection_s", "gap_detection_ms"),
    ("recovery_request_s", "recovery_request_ms"),
    ("snapshot_arrival_s", "snapshot_arrival_ms"),
    ("full_resynchronization_s", "full_resynchronization_ms"),
)


def _refuse(message: str, code: int = 2) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(code)


def _flag(env: Mapping[str, str], name: str) -> bool:
    return env.get(name) == "1"


def variability(values: Sequence[float]) -> dict[str, float | int | None]:
    """min, median, max, mean, and sample standard deviation. Empty input stays empty."""
    if not values:
        return {"n": 0, "min": None, "median": None, "max": None, "mean": None, "stdev": None}
    ordered = [float(value) for value in values]
    stdev = 0.0 if len(ordered) == 1 else statistics.stdev(ordered)
    return {
        "n": len(ordered),
        "min": round(min(ordered), 4),
        "median": round(float(statistics.median(ordered)), 4),
        "max": round(max(ordered), 4),
        "mean": round(statistics.fmean(ordered), 4),
        "stdev": round(stdev, 4),
    }


def _count_before(samples: Sequence[tuple[float, int]], instant: float) -> int:
    count = 0
    for stamp, cumulative in samples:
        if stamp < instant:
            count = cumulative
        else:
            break
    return count


def observation_windows(
    samples: Sequence[tuple[float, int]],
    *,
    window_s: float,
    wall_s: float,
) -> dict[str, Any]:
    """Split listener entries into one cold window and later steady windows.

    ``samples`` are ``(seconds since worker start, cumulative listener entries)`` in order.
    Windows begin at the first entry, so catalog upsert time is ``session_open_s`` and is
    not part of either rate. The cold window's messages are not copied into the steady list.
    """
    if window_s <= 0:
        raise ValueError("window_s must be positive")
    ready = next((stamp for stamp, count in samples if count > 0), None)
    empty = variability([])
    steady_block: dict[str, Any] = {
        "scope": STEADY_SCOPE,
        "per_window_messages_per_s": [],
        "messages_per_s": empty,
        "window_s": window_s,
    }
    if ready is None:
        return {
            "session_open_s": None,
            "cold_start": {
                "included_in_steady_state": False,
                "scope": COLD_SCOPE,
                "messages": 0,
                "messages_per_s": None,
                "window_s": window_s,
            },
            "steady_state": steady_block,
        }
    cold_messages = _count_before(samples, ready + window_s) - _count_before(samples, ready)
    steady: list[float] = []
    index = 1
    while ready + (index + 1) * window_s <= wall_s + 1e-9:
        start = ready + index * window_s
        messages = _count_before(samples, start + window_s) - _count_before(samples, start)
        steady.append(messages / window_s)
        index += 1
    steady_block["per_window_messages_per_s"] = [round(value, 4) for value in steady]
    steady_block["messages_per_s"] = variability(steady)
    return {
        "session_open_s": round(ready, 4),
        "cold_start": {
            "included_in_steady_state": False,
            "scope": COLD_SCOPE,
            "messages": cold_messages,
            "messages_per_s": round(cold_messages / window_s, 4),
            "window_s": window_s,
        },
        "steady_state": steady_block,
    }


def aggregate_throughput(
    cold_rates: Sequence[float], steady_rates: Sequence[float]
) -> dict[str, Any]:
    """Steady-state median is the headline. Cold rates are not included in it."""
    steady = variability(list(steady_rates))
    return {
        "messages_per_s": steady["median"],
        "cold_start": {
            "included_in_steady_state": False,
            "scope": COLD_SCOPE,
            "messages_per_s": variability(list(cold_rates)),
        },
        "steady_state": {"scope": STEADY_SCOPE, "messages_per_s": steady},
    }


def segments_from_samples(samples: Sequence[Mapping[str, float]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for source, dest in _SEGMENT_FIELDS:
        values = [round(float(sample[source]) * 1000.0, 4) for sample in samples]
        block = variability(values)
        block["samples"] = values
        out[dest] = block
    out["scope"] = dict(SEGMENT_SCOPE)
    return out


def segments_from_times(
    *,
    gap_detection_s: float,
    recovery_request_s: float,
    snapshot_arrival_s: float,
    full_resynchronization_s: float,
) -> dict[str, Any]:
    return segments_from_samples(
        [
            {
                "gap_detection_s": gap_detection_s,
                "recovery_request_s": recovery_request_s,
                "snapshot_arrival_s": snapshot_arrival_s,
                "full_resynchronization_s": full_resynchronization_s,
            }
        ]
    )


def recovery_segments_ok(row: Mapping[str, Any]) -> bool:
    segments = row.get("recovery_segments")
    if not isinstance(segments, dict) or "recovery_ms" in segments:
        return False
    for key in RECOVERY_SEGMENT_KEYS:
        block = segments.get(key)
        if not isinstance(block, dict) or not block.get("n") or block.get("median") is None:
            return False
    return True


def lifecycle_complete(counts: Mapping[str, int]) -> bool:
    return all(int(counts.get(kind, 0)) > 0 for kind in REQUIRED_LIFECYCLE)


def json_contains_float(value: object) -> bool:
    if isinstance(value, float):
        return True
    if isinstance(value, dict):
        return any(json_contains_float(item) for item in value.values())
    if isinstance(value, list):
        return any(json_contains_float(item) for item in value)
    return False


def money_equal(left: Decimal | None, right: Decimal | None) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return Decimal(left) == Decimal(right)


def workload_exit_code(
    workloads: Sequence[Mapping[str, Any]], *, backpressure_bounded: bool = True
) -> int:
    """Timeout and skipped are failures for every workload in the report."""
    for row in workloads:
        status = row.get("status")
        if status in {"timeout", "skipped", "error"}:
            return 1
        if row.get("kind") == "recovery" and not recovery_segments_ok(row):
            return 1
        if row.get("kind") == "inconsistent" and not lifecycle_complete(
            row.get("lifecycle_counts") or {}
        ):
            return 1
        replay = row.get("replay") or {}
        if isinstance(replay, dict) and replay.get("identical") is False:
            return 1
    if not backpressure_bounded:
        return 1
    return 0


def smoke_report_errors(report: Mapping[str, Any]) -> list[str]:
    """Structural checks for the CI smoke. Not a performance target."""
    errors: list[str] = []
    if report.get("synthetic") is not True:
        errors.append("synthetic")
    if report.get("kalshi_contacted") is not False:
        errors.append("kalshi_contacted")
    hardware = report.get("hardware")
    if not isinstance(hardware, dict):
        errors.append("hardware")
    else:
        for key in ("cpu", "memory_bytes", "os", "python"):
            if hardware.get(key) in (None, ""):
                errors.append(f"hardware.{key}")
    workloads = report.get("workloads")
    if not isinstance(workloads, list) or not workloads:
        errors.append("workloads")
        return errors
    saw_recovery = False
    for index, row in enumerate(workloads):
        if not isinstance(row, dict):
            errors.append(f"workloads[{index}]")
            continue
        for key in ("kind", "status", "scope", "cold_start", "steady_state"):
            if key not in row:
                errors.append(f"workloads[{index}].{key}")
        cold = row.get("cold_start")
        if not isinstance(cold, dict) or cold.get("included_in_steady_state") is not False:
            errors.append(f"workloads[{index}].cold_start")
        if row.get("kind") == "recovery":
            saw_recovery = True
            if not recovery_segments_ok(row):
                errors.append(f"workloads[{index}].recovery_segments")
    if not saw_recovery:
        errors.append("recovery")
    return errors


def _linux_hardware(read_text: Callable[[str], str]) -> dict[str, Any]:
    cpuinfo = read_text("/proc/cpuinfo")
    meminfo = read_text("/proc/meminfo")
    cpu = "unknown"
    for line in cpuinfo.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        if key.strip().lower() in {"model name", "model", "hardware"}:
            cpu = value.strip()
            break
    memory = 0
    for line in meminfo.splitlines():
        if line.startswith("MemTotal:"):
            memory = int(line.split()[1]) * 1024
            break
    return {
        "cpu": cpu,
        "memory_bytes": memory,
        "os": f"Linux {platform.release()} ({platform.machine()})",
        "python": platform.python_version(),
        "probe": "procfs",
    }


def _run_command(args: list[str]) -> str:
    return subprocess.check_output(args, text=True).strip()


def probe_hardware(
    *,
    system: str | None = None,
    read_text: Callable[[str], str] | None = None,
    sysctl: Callable[[list[str]], str] | None = None,
) -> dict[str, Any]:
    """macOS uses sysctl. Linux reads /proc/cpuinfo and /proc/meminfo and does not."""
    detected = platform.system() if system is None else system
    if detected == "Linux":
        reader = read_text or (lambda path: Path(path).read_text(encoding="utf-8"))
        return _linux_hardware(reader)
    if detected == "Darwin":
        run = sysctl or _run_command
        version = run(["sw_vers", "-productVersion"])
        return {
            "cpu": run(["sysctl", "-n", "machdep.cpu.brand_string"]),
            "memory_bytes": int(run(["sysctl", "-n", "hw.memsize"])),
            "os": f"Darwin {version} ({platform.machine()})",
            "python": platform.python_version(),
            "probe": "sysctl",
        }
    return {
        "cpu": platform.processor() or platform.machine(),
        "memory_bytes": None,
        "os": f"{detected} {platform.release()} ({platform.machine()})",
        "python": platform.python_version(),
        "probe": "platform",
    }


def _percentile(values: Sequence[int], pct: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("empty sample")
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * (pct / 100.0)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _ms(ns: float | None) -> float | None:
    if ns is None:
        return None
    return round(ns / 1_000_000.0, 4)


def _latency(values: Sequence[int]) -> dict[str, float | int | None]:
    if not values:
        return {"n": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None}
    return {
        "n": len(values),
        "p50_ms": _ms(_percentile(values, 50)),
        "p95_ms": _ms(_percentile(values, 95)),
        "p99_ms": _ms(_percentile(values, 99)),
    }


def _rss_bytes() -> int:
    raw = subprocess.check_output(["ps", "-o", "rss=", "-p", str(os.getpid())], text=True)
    return int(raw.strip()) * 1024


def _cpu() -> tuple[float, float]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime, usage.ru_stime


def _redacted(url: str) -> str:
    parsed = make_url(url)
    return parsed.set(password="***").render_as_string(hide_password=False)


def _effective_port(port: int | None) -> int:
    return 5432 if port is None else port


def _check_url(raw: str, env: Mapping[str, str] | None = None) -> tuple[str, str]:
    """Refuse a bad target before the caller connects. Does not touch the database."""
    selected = os.environ if env is None else env
    if not raw or raw.startswith("sqlite"):
        _refuse("DATABASE_URL must be a postgresql+asyncpg URL. Refusing to invent results.")
    parsed = make_url(raw)
    if parsed.drivername != "postgresql+asyncpg":
        _refuse(f"DATABASE_URL driver is {parsed.drivername}. PostgreSQL via asyncpg is required.")
    port = _effective_port(parsed.port)
    host = (parsed.host or "").strip().lower()
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    if port == 5432 and not _flag(selected, "BENCHMARK_ALLOW_PORT_5432"):
        _refuse(
            "Refusing port 5432. An omitted port is treated as 5432. "
            "Point DATABASE_URL at the Docker Postgres 16 on port 5433, "
            "or set BENCHMARK_ALLOW_PORT_5432=1 to override."
        )
    if host not in LOCAL_HOSTS and not _flag(selected, "BENCHMARK_ALLOW_REMOTE"):
        _refuse(
            f"Refusing non-local database host {host!r}. "
            "Without BENCHMARK_ALLOW_REMOTE=1 the only allowed hosts are "
            "localhost, 127.0.0.1, and ::1."
        )
    bench = parsed.set(database=BENCH_DB, port=port).render_as_string(hide_password=False)
    return raw, bench


class RatedSynthetic(SyntheticDataSource):
    """Same generated stream. ``rate`` > 0 sleeps a fixed gap between messages."""

    def __init__(self, config: SessionConfig, *, rate: float) -> None:
        super().__init__(config, speed=None)
        self.rate = rate

    async def subscribe_orderbooks(self, market_ids: Sequence[str] | None = None):
        del market_ids
        self._state = ConnectionState.CONNECTED
        interval = 0.0 if self.rate <= 0 else 1.0 / self.rate
        next_at = time.perf_counter()
        async for msg in playback(self.messages, speed=None, start_position=self.position + 1):
            if interval > 0:
                delay = next_at - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
                next_at += interval
            self.position = msg.position
            yield msg
        self._state = ConnectionState.DISCONNECTED


class MeasuringWorker(WorkerService):
    def __init__(self, settings: Settings, **kwargs: Any) -> None:
        super().__init__(settings, **kwargs)
        self.detection_ns: list[int] = []
        self.events: list[DetectionEvent] = []
        self._recv_ns: int | None = None
        self.pending_recv: dict[int, int] = {}

    def _log_events(self, events: list[DetectionEvent]) -> None:
        recv = self._recv_ns
        for ev in events:
            ns = ev.timing.internal_latency_ns
            if ns is not None:
                self.detection_ns.append(ns)
            self.events.append(ev)
            if recv is not None:
                self.pending_recv[id(ev)] = recv
        super()._log_events(events)


class RecoveryTracker:
    """Four labeled intervals for one sequence-gap episode. Not one recovery total."""

    def __init__(self) -> None:
        self._open: dict[str, float | None] | None = None
        self._pending: set[str] = set()
        self.episodes: list[dict[str, float]] = []

    def note_gap(self, manager: BookManager, *, started: float, detected: float) -> None:
        self._pending = {
            mid
            for mid, state in manager._markets.items()
            if state.sync is SyncStatus.UNSYNCHRONIZED
        }
        self._open = {
            "gap_detection_s": detected - started,
            "request_started": None,
            "request_finished": None,
            "first_snapshot": None,
        }

    def note_request_start(self, now: float) -> None:
        if self._open is not None and self._open["request_started"] is None:
            self._open["request_started"] = now

    def note_request_end(self, now: float) -> None:
        if self._open is not None and self._open["request_finished"] is None:
            self._open["request_finished"] = now

    def note_books(self, manager: BookManager, now: float) -> None:
        episode = self._open
        if episode is None or not self._pending or episode["request_finished"] is None:
            return
        restored = [
            mid for mid in self._pending if manager._markets[mid].sync is SyncStatus.SYNCHRONIZED
        ]
        if restored and episode["first_snapshot"] is None:
            episode["first_snapshot"] = now
        if len(restored) != len(self._pending) or episode["first_snapshot"] is None:
            return
        request_started = episode["request_started"]
        request_finished = episode["request_finished"]
        first = episode["first_snapshot"]
        gap = episode["gap_detection_s"]
        if (
            not isinstance(request_started, float)
            or not isinstance(request_finished, float)
            or not isinstance(first, float)
            or not isinstance(gap, float)
        ):
            return
        self.episodes.append(
            {
                "gap_detection_s": gap,
                "recovery_request_s": request_finished - request_started,
                "snapshot_arrival_s": first - request_finished,
                "full_resynchronization_s": now - first,
            }
        )
        self._open = None
        self._pending = set()


def inject_sequence_gap(messages: Sequence[Any]) -> tuple[list[Any], dict[str, Any]]:
    """Drop one delivered econ delta so the next delta on that subscription skips a seq.

    The synthetic GAP fault starts a new subscription instead of delivering a skipped
    sequence, so the book manager never sees a gap. Removing one in-order delta leaves
    the later recovery snapshots in place, which resynchronize the desynced markets.
    """
    drop_at: int | None = None
    for index, msg in enumerate(messages[:-1]):
        event = msg.event
        nxt = messages[index + 1].event
        if type(event).__name__ != "OrderBookDeltaEvent":
            continue
        if type(nxt).__name__ != "OrderBookDeltaEvent":
            continue
        conn = getattr(event, "connection_id", None)
        if not isinstance(conn, str) or not conn.startswith("conn-econ"):
            continue
        if conn != getattr(nxt, "connection_id", None) or event.sid != nxt.sid:
            continue
        if nxt.seq == event.seq + 1 and event.seq >= 5:
            drop_at = index
            break
    if drop_at is None:
        raise RuntimeError("econ subscription has no contiguous delta to drop")
    dropped = messages[drop_at]
    kept = [msg for index, msg in enumerate(messages) if index != drop_at]
    return kept, {
        "dropped_position": dropped.position,
        "connection_id": dropped.event.connection_id,
        "sid": dropped.event.sid,
        "seq": dropped.event.seq,
    }


def synthetic_recovery_episode(seed: int = 606) -> dict[str, Any]:
    """Walk the gapped recording and report whether each recovery stage occurs."""
    cfg = _config(
        name="bench-recovery",
        seed=seed,
        instances=1,
        duration_ms=12_000,
        faults=(FaultSpec(FaultKind.GAP, 4_000, "econ"),),
    )
    source = SyntheticDataSource(cfg, speed=None)
    kept, meta = inject_sequence_gap(list(source.messages))
    manager = BookManager(source.catalog.markets, source="synthetic:benchmark")
    gap = False
    requested = False
    snapshot = False
    resynced = False
    pending: set[str] = set()
    for msg in kept:
        before = manager.stats.gaps_detected
        manager.process(msg)
        if manager.stats.gaps_detected > before:
            gap = True
            pending = {
                mid
                for mid, state in manager._markets.items()
                if state.sync is SyncStatus.UNSYNCHRONIZED
            }
            requested = bool(manager.drain_recovery_requests()) and bool(pending)
            continue
        if not gap or not pending:
            continue
        restored = [mid for mid in pending if manager._markets[mid].sync is SyncStatus.SYNCHRONIZED]
        if restored:
            snapshot = True
        if len(restored) == len(pending):
            resynced = True
            break
    return {
        "gap_detected": gap,
        "recovery_requested": requested,
        "snapshot_arrived": snapshot,
        "resynchronized": resynced,
        **meta,
    }


@dataclass
class _Calibration:
    markets_per_instance: int
    messages_per_instance_ms: float
    generate_s: float


def _duration_ms(instances: int, message_target: int, cal: _Calibration) -> int:
    """Simulated duration that should yield about ``message_target`` messages."""
    raw_ms = message_target / (cal.messages_per_instance_ms * instances)
    ticks = max(1, round(raw_ms / TICK_MS))
    return ticks * TICK_MS


def _config(
    *,
    name: str,
    seed: int,
    instances: int,
    duration_ms: int,
    faults: tuple[FaultSpec, ...] = (),
    inject_standard_scenarios: bool = False,
) -> SessionConfig:
    return SessionConfig(
        name=name,
        seed=seed,
        duration_ms=duration_ms,
        description="synthetic benchmark session",
        tick_ms=TICK_MS,
        instances=instances,
        faults=faults,
        inject_standard_scenarios=inject_standard_scenarios,
        scenario_t0_ms=4_000,
    )


def _connect_kwargs(url: str) -> dict[str, Any]:
    parsed = make_url(url)
    return {
        "host": parsed.host,
        "port": _effective_port(parsed.port),
        "user": parsed.username,
        "password": parsed.password,
        "database": parsed.database,
    }


async def _ensure_database(admin_url: str, bench_url: str) -> None:
    conn = await asyncpg.connect(**{**_connect_kwargs(admin_url), "database": "postgres"})
    try:
        exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", BENCH_DB)
        if exists is None:
            await conn.execute(f'CREATE DATABASE "{BENCH_DB}"')
    finally:
        await conn.close()
    env = os.environ.copy()
    env["DATABASE_URL"] = bench_url
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env=env,
        check=True,
    )


async def prepare_benchmark_database(raw: str) -> str:
    """Check the guard first. A refusal raises before CREATE DATABASE or Alembic."""
    _admin, bench = _check_url(raw)
    await _ensure_database(raw, bench)
    return bench


async def _counts(store: DatabaseSessionStore, session_id: str) -> dict[str, int]:
    query = text(
        """
        SELECT
          (SELECT count(*) FROM orderbook_updates WHERE session_id = :sid) AS updates,
          (SELECT count(*) FROM orderbook_snapshots WHERE session_id = :sid) AS snapshots,
          (SELECT count(*) FROM detections WHERE session_id = :sid) AS detections,
          (SELECT count(*) FROM notification_outbox WHERE session_id = :sid) AS outbox,
          (SELECT count(*) FROM session_checkpoints WHERE session_id = :sid) AS checkpoints
        """
    )
    async with store._inner.sessions() as session:
        row = (await session.execute(query, {"sid": session_id})).one()
    return {
        "orderbook_updates": int(row.updates),
        "orderbook_snapshots": int(row.snapshots),
        "detections": int(row.detections),
        "notification_outbox": int(row.outbox),
        "session_checkpoints": int(row.checkpoints),
    }


async def _delete_session(store: DatabaseSessionStore, session_id: str) -> None:
    async with store._inner.sessions() as session, session.begin():
        await session.execute(
            text("DELETE FROM notification_outbox WHERE session_id = :sid"),
            {"sid": session_id},
        )
        await session.execute(
            text("DELETE FROM ingestion_sessions WHERE session_id = :sid"),
            {"sid": session_id},
        )


async def _audit_detections(
    store: DatabaseSessionStore, session_id: str, events: Sequence[DetectionEvent]
) -> dict[str, Any]:
    """Persisted money must match the engine events, and JSON must not contain floats."""
    money = text(
        """
        SELECT detection_id, max_deviation, max_capacity, max_net_edge, certificate_hash,
               record_json
        FROM detections
        WHERE session_id = :sid
        """
    )
    outbox = text(
        """
        SELECT payload
        FROM notification_outbox
        WHERE session_id = :sid
        ORDER BY id
        """
    )
    async with store._inner.sessions() as session:
        rows = (await session.execute(money, {"sid": session_id})).all()
        payloads = (await session.execute(outbox, {"sid": session_id})).scalars().all()
    latest: dict[str, DetectionEvent] = {}
    for ev in events:
        latest[ev.detection_id] = ev
    mismatches: list[str] = []
    float_rows: list[str] = []
    for row in rows:
        if json_contains_float(row.record_json):
            float_rows.append(str(row.detection_id))
        ev = latest.get(str(row.detection_id))
        if ev is None:
            mismatches.append(f"{row.detection_id} missing from engine events")
            continue
        rec = ev.record
        if not money_equal(row.max_net_edge, rec.max_net_edge):
            mismatches.append(f"{row.detection_id} max_net_edge")
        if not money_equal(row.max_deviation, rec.max_deviation):
            mismatches.append(f"{row.detection_id} max_deviation")
        if not money_equal(row.max_capacity, rec.max_capacity):
            mismatches.append(f"{row.detection_id} max_capacity")
        if row.certificate_hash != rec.certificate_hash:
            mismatches.append(f"{row.detection_id} certificate_hash")
    kinds: dict[str, int] = {}
    float_payloads = 0
    for payload in payloads:
        if json_contains_float(payload):
            float_payloads += 1
        if isinstance(payload, dict) and isinstance(payload.get("event"), str):
            kind = str(payload["event"])
            kinds[kind] = kinds.get(kind, 0) + 1
    if len(payloads) != len(events):
        mismatches.append(f"outbox rows {len(payloads)} != engine events {len(events)}")
    return {
        "matches_engine": not mismatches and not float_rows and float_payloads == 0,
        "detections": len(rows),
        "mismatches": mismatches[:8],
        "float_rows": float_rows[:8],
        "float_payloads": float_payloads,
        "outbox_lifecycle": dict(sorted(kinds.items())),
        "notification_outbox_writes": len(payloads),
    }


async def _replay(
    store: DatabaseSessionStore, session_id: str, inputs: SessionInputs
) -> dict[str, Any]:
    async with store._inner.sessions() as session:
        from consistency_persistence.recording import load_journal

        entries = await load_journal(session, session_id)
    if not entries:
        return {"entries": 0, "identical": False, "entries_per_s": None}
    started = time.perf_counter()
    first = replay_catalog(
        entries, inputs.catalog, inputs.relationships, inputs.fees, session_id=session_id
    )
    second_started = time.perf_counter()
    second = replay_catalog(
        entries, inputs.catalog, inputs.relationships, inputs.fees, session_id=session_id
    )
    elapsed = time.perf_counter() - second_started
    compared = compare_outcomes(first, second)
    return {
        "entries": len(entries),
        "identical": compared.identical,
        "event_mismatches": compared.event_mismatches,
        "classification_mismatches": len(compared.classification_mismatches),
        "entries_per_s": None if elapsed <= 0 else round(len(entries) / elapsed, 2),
        "replay_s": round(elapsed, 4),
        "both_passes_s": round(time.perf_counter() - started, 4),
    }


class _SlowSink:
    def __init__(self, delay_s: float) -> None:
        self.delay_s = delay_s
        self.writes = 0

    async def write(self, events: Sequence[DetectionEvent]) -> None:
        del events
        await asyncio.sleep(self.delay_s)
        self.writes += 1


async def _backpressure() -> dict[str, Any]:
    delay_s = 0.01
    batches = 40
    cap = 4
    outbox: Outbox = Outbox(_SlowSink(delay_s), None, maxsize=cap)
    sink = cast(_SlowSink, outbox.sink)
    writer = asyncio.create_task(outbox.run())
    started = time.perf_counter()
    try:
        for _ in range(batches):
            outbox.put_nowait([cast(DetectionEvent, object())])
            await outbox.wait_capacity()
        await outbox.drain()
    finally:
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)
    elapsed = time.perf_counter() - started
    return {
        "maxsize": cap,
        "batches": batches,
        "writes": sink.writes,
        "high_water": outbox.stats.high_water,
        "bounded": outbox.stats.high_water <= cap and sink.writes == batches,
        "elapsed_s": round(elapsed, 4),
        "delay_s": delay_s,
    }


def _blank_phases(window_s: float) -> dict[str, Any]:
    windows = observation_windows([(0.0, 0.0)], window_s=window_s, wall_s=0.0)
    return {
        "scope": STEADY_SCOPE,
        "window_s": window_s,
        "session_open_s": windows["session_open_s"],
        "cold_start": windows["cold_start"],
        "steady_state": windows["steady_state"],
        "full_run_messages_per_s": None,
        "messages_per_s": None,
    }


async def _run_workload(
    *,
    store: DatabaseSessionStore,
    settings: Settings,
    reviews: Path,
    fees_dir: Path,
    run_id: str,
    kind: str,
    target_markets: int | None,
    instances: int,
    duration_ms: int,
    rate: float,
    seed: int,
    faults: tuple[FaultSpec, ...],
    budget_s: float,
    profile: bool,
    do_replay: bool,
    watch_recovery: bool,
    measure_detections: bool,
    inject_gap: bool,
    inject_scenarios: bool,
    window_s: float,
    rep: int,
    require_steady: bool,
) -> dict[str, Any]:
    name = f"bench-{kind}"
    cfg = _config(
        name=name,
        seed=seed,
        instances=instances,
        duration_ms=duration_ms,
        faults=faults,
        inject_standard_scenarios=inject_scenarios,
    )
    generated = time.perf_counter()
    source = RatedSynthetic(cfg, rate=rate) if rate > 0 else SyntheticDataSource(cfg, speed=None)
    gap_meta: dict[str, Any] | None = None
    if inject_gap:
        kept, gap_meta = inject_sequence_gap(list(source.messages))
        source.messages = kept
    generate_s = time.perf_counter() - generated
    discovered = time.perf_counter()
    relationships = relationships_for(source.catalog, reviews, as_of=SIM_EPOCH)
    discover_s = time.perf_counter() - discovered
    markets = len(source.catalog.markets)
    messages = len(source.messages)
    print(
        f"generated {kind} rep={rep} markets={markets} messages={messages} "
        f"duration_ms={duration_ms}",
        file=sys.stderr,
        flush=True,
    )
    record: dict[str, Any] = {
        "kind": kind,
        "target_markets": target_markets,
        "markets": markets,
        "instances": instances,
        "duration_ms": duration_ms,
        "rate_per_s": rate,
        "seed": seed,
        "rep": rep,
        "generated_messages": messages,
        "relationships": len(relationships),
        "generate_s": round(generate_s, 4),
        "discover_s": round(discover_s, 4),
        "tick_ms": TICK_MS,
        "gap_injection": gap_meta,
    }
    if messages > 25_000:
        record["status"] = "skipped"
        record["reason"] = f"{messages} generated messages exceeds the 25000 safety cap"
        record.update(_blank_phases(window_s))
        return record
    if generate_s + discover_s > budget_s:
        record["status"] = "skipped"
        record["reason"] = (
            f"generation plus relationship discovery took {generate_s + discover_s:.1f}s, "
            f"past the {budget_s:.0f}s budget, so the worker was not started"
        )
        record.update(_blank_phases(window_s))
        return record
    rate_token: int | str = int(rate) if rate == int(rate) else format(rate, "f")
    fingerprint = sha256_of(
        {
            "run": run_id,
            "kind": kind,
            "rep": rep,
            "markets": markets,
            "duration_ms": duration_ms,
            "rate": rate_token,
            "seed": seed,
        }
    )
    inputs = SessionInputs(
        source,
        source.catalog,
        relationships,
        fee_calculator(fees_dir),
        "synthetic:benchmark",
        True,
        fingerprint,
    )
    worker = MeasuringWorker(settings, inputs=inputs, store=store)
    tracker = RecoveryTracker() if watch_recovery else None
    samples: list[tuple[float, int]] = []
    origin = {"t": 0.0}
    eval_ns: list[int] = []
    cert_ns: list[int] = []
    persist_ns: list[int] = []
    e2e_ns: list[int] = []
    restores: list[tuple[Any, str, Any]] = []

    def _patch(obj: Any, name: str, value: Any) -> None:
        restores.append((obj, name, getattr(obj, name)))
        setattr(obj, name, value)

    original_evaluate = engine_mod.evaluate

    def _timed_evaluate(*args: Any, **kwargs: Any) -> Any:
        started_ns = time.perf_counter_ns()
        try:
            return original_evaluate(*args, **kwargs)
        finally:
            eval_ns.append(time.perf_counter_ns() - started_ns)

    original_on_message = PipelineListener.on_message

    async def _timed_message(self: PipelineListener, msg: Any, updates: Sequence[Any]) -> None:
        bookish = any(getattr(update, "kind", None) in {"snapshot", "delta"} for update in updates)
        if measure_detections and bookish:
            worker._recv_ns = time.perf_counter_ns()
        try:
            await original_on_message(self, msg, updates)
        finally:
            worker._recv_ns = None
            samples.append((time.perf_counter() - origin["t"], self.stats.entries))

    original_process = BookManager.process

    def _timed_process(self: BookManager, msg: Any) -> Any:
        before = self.stats.gaps_detected
        started = time.perf_counter()
        try:
            return original_process(self, msg)
        finally:
            if tracker is not None:
                now = time.perf_counter()
                if self.stats.gaps_detected > before:
                    tracker.note_gap(self, started=started, detected=now)
                else:
                    tracker.note_books(self, now)

    original_request = IngestionRunner._request_recovery

    async def _timed_request(self: IngestionRunner, req: Any) -> Any:
        if tracker is not None:
            tracker.note_request_start(time.perf_counter())
        try:
            return await original_request(self, req)
        finally:
            if tracker is not None:
                tracker.note_request_end(time.perf_counter())

    original_certificate = Evaluation.certificate_json

    def _timed_certificate(self: Evaluation) -> str:
        started_ns = time.perf_counter_ns()
        try:
            return original_certificate(self)
        finally:
            cert_ns.append(time.perf_counter_ns() - started_ns)

    original_write = DetectionWriter.write

    async def _timed_write(self: DetectionWriter, events: Sequence[DetectionEvent]) -> None:
        started_ns = time.perf_counter_ns()
        try:
            await original_write(self, events)
        finally:
            finished = time.perf_counter_ns()
            persist_ns.append(finished - started_ns)
            for ev in events:
                recv = worker.pending_recv.pop(id(ev), None)
                if recv is not None:
                    e2e_ns.append(finished - recv)

    _patch(engine_mod, "evaluate", _timed_evaluate)
    _patch(PipelineListener, "on_message", _timed_message)
    if tracker is not None:
        _patch(BookManager, "process", _timed_process)
        _patch(IngestionRunner, "_request_recovery", _timed_request)
    if measure_detections:
        _patch(Evaluation, "certificate_json", _timed_certificate)
        _patch(DetectionWriter, "write", _timed_write)
    profiler = cProfile.Profile() if profile else None
    utime, stime = _cpu()
    status = "ok"
    reason: str | None = None
    result: WorkerResult | None = None
    try:
        if profiler is not None:
            profiler.enable()
        origin["t"] = time.perf_counter()
        samples.append((0.0, 0))
        result = await asyncio.wait_for(worker.run(), timeout=budget_s)
    except TimeoutError:
        status = "timeout"
        reason = f"worker still running after {budget_s:.0f}s"
    except Exception as exc:
        status = "error"
        reason = f"{type(exc).__name__}: {exc}"
    finally:
        if profiler is not None:
            profiler.disable()
        for obj, name, previous in reversed(restores):
            setattr(obj, name, previous)
    wall_s = time.perf_counter() - origin["t"]
    windows = observation_windows(samples, window_s=window_s, wall_s=wall_s)
    cpu_u, cpu_s = _cpu()
    cpu_seconds = (cpu_u - utime) + (cpu_s - stime)
    record["status"] = status
    record["reason"] = reason
    record["wall_s"] = round(wall_s, 4)
    record["cpu_seconds"] = round(cpu_seconds, 4)
    record["cpu_percent_one_core"] = None if wall_s <= 0 else round(100.0 * cpu_seconds / wall_s, 2)
    record["rss_bytes"] = _rss_bytes()
    record["ru_maxrss"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    record["scope"] = STEADY_SCOPE
    record["window_s"] = window_s
    record["session_open_s"] = windows["session_open_s"]
    record["cold_start"] = windows["cold_start"]
    record["steady_state"] = windows["steady_state"]
    if tracker is not None:
        record["recovery_segments"] = segments_from_samples(tracker.episodes)
        if status == "ok" and not recovery_segments_ok(record):
            status = "error"
            reason = "recovery episode did not record gap, request, snapshot, and resync"
            record["status"] = status
            record["reason"] = reason
    if measure_detections:
        record["certificate_serialization"] = {
            **_latency(cert_ns),
            "scope": "CPU time in Evaluation.certificate_json (canonical proof JSON)",
        }
        record["detection_persistence"] = {
            **_latency(persist_ns),
            "scope": (
                "DetectionWriter.write through commit, including the notification-outbox "
                "insert in that same transaction"
            ),
        }
        record["end_to_end_latency"] = {
            **_latency(e2e_ns),
            "scope": "book update received until the detection transaction commits",
        }

    async def _drop(session_id: str) -> None:
        if not session_id:
            return
        try:
            await _delete_session(store, session_id)
        except Exception as exc:
            record["cleanup_error"] = f"{type(exc).__name__}: {exc}"

    listener = worker.listener
    runner = None if worker.supervisor is None else worker.supervisor.runner
    if listener is None or runner is None or status != "ok":
        record["full_run_messages_per_s"] = None
        record["messages_per_s"] = None
        if profiler is not None:
            record["profile"] = _profile_text(profiler)
        if result is not None:
            await _drop(result.session_id)
        return record
    if require_steady and not windows["steady_state"]["per_window_messages_per_s"]:
        record["status"] = "error"
        record["reason"] = "no complete steady-state observation window"
        record["full_run_messages_per_s"] = None
        record["messages_per_s"] = None
        if result is not None:
            await _drop(result.session_id)
        return record
    processed = listener.stats.entries
    record["messages_processed"] = processed
    record["source_messages"] = runner.stats.messages
    record["full_run_messages_per_s"] = None if wall_s <= 0 else round(processed / wall_s, 2)
    steady_median = windows["steady_state"]["messages_per_s"]["median"]
    record["messages_per_s"] = steady_median
    record["processing_latency"] = _latency(list(listener.stats.internal_latency_ns))
    record["detection_latency"] = _latency(eval_ns)
    record["detection_event_latency"] = _latency(worker.detection_ns)
    record["outbox_present"] = worker.outbox is not None
    record["gaps_detected"] = listener.manager.stats.gaps_detected
    record["desync_events"] = listener.manager.stats.desync_events
    record["engine"] = worker.listener.engine.stats.as_dict() if worker.listener else {}
    box = worker.outbox
    record["queue"] = {
        "outbox_high_water": None if box is None else box.stats.high_water,
        "outbox_maxsize": None if box is None else box.maxsize,
        "inbound_high_water": runner.stats.inbound_high_water,
        "inbound_maxsize": settings.inbound_queue_maxsize,
        "outbox_events": 0 if box is None else box.stats.events,
        "outbox_write_errors": 0 if box is None else box.stats.write_errors,
    }
    record["exchange_counts"] = dict(source.result.counts)
    session_id = "" if result is None else result.session_id
    record["session_id"] = session_id
    if session_id:
        counts = await _counts(store, session_id)
        record["db_rows"] = counts
        written = (
            counts["orderbook_updates"]
            + counts["orderbook_snapshots"]
            + counts["notification_outbox"]
            + counts["session_checkpoints"]
        )
        record["db_rows_written"] = written
        record["db_rows_per_s"] = None if wall_s <= 0 else round(written / wall_s, 2)
        if measure_detections:
            audit = await _audit_detections(store, session_id, worker.events)
            record["money"] = audit
            record["lifecycle_counts"] = audit["outbox_lifecycle"]
            record["notification_outbox_writes"] = audit["notification_outbox_writes"]
            engine_events = dict(result.events) if result is not None else {}
            record["engine_lifecycle_counts"] = engine_events
            if not audit["matches_engine"] or not lifecycle_complete(audit["outbox_lifecycle"]):
                record["status"] = "error"
                record["reason"] = (
                    "detection rows did not match the engine or a lifecycle kind was zero"
                )
            elif audit["outbox_lifecycle"] != engine_events:
                record["status"] = "error"
                record["reason"] = "committed outbox lifecycle does not match the engine"
        if do_replay and record["status"] == "ok":
            record["replay"] = await _replay(store, session_id, inputs)
        await _drop(session_id)
    if profiler is not None:
        record["profile"] = _profile_text(profiler)
    return record


def _profile_text(profiler: cProfile.Profile) -> str:
    stream = io.StringIO()
    stats = pstats.Stats(profiler, stream=stream).sort_stats("cumulative")
    stats.print_stats(25)
    return stream.getvalue()


async def _postgres_version(url: str) -> str:
    conn = await asyncpg.connect(**_connect_kwargs(url))
    try:
        version = await conn.fetchval("SHOW server_version")
    finally:
        await conn.close()
    return str(version)


def _print_row(row: dict[str, Any]) -> None:
    steady = row.get("steady_state") or {}
    summary = steady.get("messages_per_s") or {}
    median = summary.get("median", row.get("messages_per_s"))
    print(
        f"{row['kind']} markets={row.get('markets')} status={row['status']} "
        f"steady_messages_per_s={median} wall_s={row.get('wall_s')}",
        flush=True,
    )


def _compact_rep(row: Mapping[str, Any]) -> dict[str, Any]:
    cold = row.get("cold_start") or {}
    steady = row.get("steady_state") or {}
    rates = steady.get("per_window_messages_per_s") or []
    return {
        "rep": row.get("rep"),
        "status": row.get("status"),
        "wall_s": row.get("wall_s"),
        "session_open_s": row.get("session_open_s"),
        "cold_messages_per_s": cold.get("messages_per_s") if isinstance(cold, dict) else None,
        "steady_messages_per_s": variability([float(value) for value in rates]),
        "full_run_messages_per_s": row.get("full_run_messages_per_s"),
        "rss_bytes": row.get("rss_bytes"),
        "reason": row.get("reason"),
    }


def _worst_status(statuses: Sequence[str]) -> str:
    rank = {"ok": 0, "skipped": 1, "timeout": 2, "error": 3}
    return max(statuses, key=lambda item: rank.get(item, 3))


def _pool_segments(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    collected: dict[str, list[float]] = {key: [] for key in RECOVERY_SEGMENT_KEYS}
    for row in rows:
        segments = row.get("recovery_segments") or {}
        if not isinstance(segments, dict):
            continue
        for key in RECOVERY_SEGMENT_KEYS:
            block = segments.get(key) or {}
            samples = block.get("samples") if isinstance(block, dict) else None
            if isinstance(samples, list):
                collected[key].extend(float(value) for value in samples)
    pooled = segments_from_samples([])
    for key, values in collected.items():
        block = variability(values)
        block["samples"] = [round(value, 4) for value in values]
        pooled[key] = block
    return pooled


def combine_repetitions(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Headline throughput is the steady-state median across windows and reps."""
    status = _worst_status([str(row.get("status", "error")) for row in rows])
    ok = [row for row in rows if row.get("status") == "ok"]
    base = dict(ok[len(ok) // 2] if ok else rows[0])
    cold_rates: list[float] = []
    steady_rates: list[float] = []
    full_rates: list[float] = []
    opens: list[float] = []
    rss: list[float] = []
    for row in ok:
        cold = row.get("cold_start") or {}
        rate = cold.get("messages_per_s") if isinstance(cold, dict) else None
        if isinstance(rate, (int, float)):
            cold_rates.append(float(rate))
        windows = (row.get("steady_state") or {}).get("per_window_messages_per_s") or []
        steady_rates.extend(float(value) for value in windows)
        if isinstance(row.get("full_run_messages_per_s"), (int, float)):
            full_rates.append(float(row["full_run_messages_per_s"]))
        if isinstance(row.get("session_open_s"), (int, float)):
            opens.append(float(row["session_open_s"]))
        if isinstance(row.get("rss_bytes"), int):
            rss.append(float(row["rss_bytes"]))
    throughput = aggregate_throughput(cold_rates, steady_rates)
    base["status"] = status
    base["repetitions"] = len(rows)
    base["cold_start"] = throughput["cold_start"]
    base["steady_state"] = {
        **throughput["steady_state"],
        "per_window_messages_per_s": [round(value, 4) for value in steady_rates],
        "window_s": rows[0].get("window_s"),
    }
    base["messages_per_s"] = throughput["messages_per_s"]
    base["full_run_messages_per_s"] = variability(full_rates)
    base["session_open_s"] = variability(opens)
    base["rss_variability"] = variability(rss)
    base["scope"] = STEADY_SCOPE
    base["per_rep"] = [_compact_rep(row) for row in rows]
    reasons = [
        str(row.get("reason")) for row in rows if row.get("status") != "ok" and row.get("reason")
    ]
    if status != "ok":
        base["reason"] = "; ".join(reasons) or status
    for row in rows:
        if row.get("replay"):
            base["replay"] = row["replay"]
            break
    if base.get("kind") == "recovery":
        base["recovery_segments"] = _pool_segments(ok)
        if status == "ok" and not recovery_segments_ok(base):
            base["status"] = "error"
            base["reason"] = "recovery episode did not record all four segments"
    if base.get("kind") == "inconsistent" and status == "ok":
        counts = base.get("lifecycle_counts") or {}
        if not lifecycle_complete(counts):
            base["status"] = "error"
            base["reason"] = "inconsistency workload missed a lifecycle kind"
    return base


def _pythonpath() -> str:
    roots = [
        ROOT / "packages/core/src",
        ROOT / "packages/simulation/src",
        ROOT / "packages/connectors/src",
        ROOT / "packages/pipeline/src",
        ROOT / "packages/persistence/src",
        ROOT / "apps/api/src",
        ROOT / "apps/worker/src",
        ROOT,
    ]
    parts = [str(path) for path in roots]
    current = os.environ.get("PYTHONPATH", "")
    if current:
        parts.append(current)
    return os.pathsep.join(parts)


def _child_env() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = _pythonpath()
    env["PYTHONUNBUFFERED"] = "1"
    return env


def _faults_from_spec(raw: Sequence[Mapping[str, Any]]) -> tuple[FaultSpec, ...]:
    return tuple(
        FaultSpec(FaultKind(str(item["kind"])), int(item["at_ms"]), str(item["family"]))
        for item in raw
    )


def _spec(
    *,
    run_id: str,
    kind: str,
    target_markets: int | None,
    instances: int,
    duration_ms: int,
    rate: float,
    seed: int,
    faults: tuple[FaultSpec, ...],
    budget_s: float,
    profile: bool,
    do_replay: bool,
    watch_recovery: bool,
    measure_detections: bool,
    inject_gap: bool,
    inject_scenarios: bool,
    window_s: float,
    rep: int,
    require_steady: bool,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "kind": kind,
        "target_markets": target_markets,
        "instances": instances,
        "duration_ms": duration_ms,
        "rate": rate,
        "seed": seed,
        "faults": [
            {"kind": fault.kind.value, "at_ms": fault.at_ms, "family": fault.family}
            for fault in faults
        ],
        "budget_s": budget_s,
        "profile": profile,
        "do_replay": do_replay,
        "watch_recovery": watch_recovery,
        "measure_detections": measure_detections,
        "inject_gap": inject_gap,
        "inject_scenarios": inject_scenarios,
        "window_s": window_s,
        "rep": rep,
        "require_steady": require_steady,
    }


async def _run_from_spec(
    store: DatabaseSessionStore, settings: Settings, spec: Mapping[str, Any]
) -> dict[str, Any]:
    reviews = ROOT / "fixtures" / "relationships" / "manual-reviews.yaml"
    fees_dir = ROOT / "fixtures" / "fees"
    return await _run_workload(
        store=store,
        settings=settings,
        reviews=reviews,
        fees_dir=fees_dir,
        run_id=str(spec["run_id"]),
        kind=str(spec["kind"]),
        target_markets=spec.get("target_markets"),
        instances=int(spec["instances"]),
        duration_ms=int(spec["duration_ms"]),
        rate=float(spec["rate"]),
        seed=int(spec["seed"]),
        faults=_faults_from_spec(spec.get("faults") or []),
        budget_s=float(spec["budget_s"]),
        profile=bool(spec["profile"]),
        do_replay=bool(spec["do_replay"]),
        watch_recovery=bool(spec["watch_recovery"]),
        measure_detections=bool(spec["measure_detections"]),
        inject_gap=bool(spec["inject_gap"]),
        inject_scenarios=bool(spec["inject_scenarios"]),
        window_s=float(spec["window_s"]),
        rep=int(spec["rep"]),
        require_steady=bool(spec["require_steady"]),
    )


def _spawn(spec: dict[str, Any]) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        spec_path = directory / "spec.json"
        result_path = directory / "result.json"
        payload = {**spec, "result_path": str(result_path)}
        spec_path.write_text(json.dumps(payload), encoding="utf-8")
        proc = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--child", str(spec_path)],
            cwd=ROOT,
            env=_child_env(),
            check=False,
        )
        if not result_path.exists():
            failed = {
                "kind": spec["kind"],
                "status": "error",
                "reason": f"child exited {proc.returncode} without a result",
                "markets": spec.get("target_markets"),
                "rep": spec["rep"],
            }
            failed.update(_blank_phases(float(spec["window_s"])))
            return failed
        return json.loads(result_path.read_text(encoding="utf-8"))


async def _child_main(path: Path) -> int:
    spec = json.loads(path.read_text(encoding="utf-8"))
    logging.getLogger().setLevel(logging.WARNING)
    result_path = Path(str(spec["result_path"]))
    try:
        _admin, bench_url = _check_url(os.environ.get("DATABASE_URL", ""))
        settings = Settings.model_validate(
            {"DATABASE_URL": bench_url, "DATA_SOURCE": "synthetic", "LOG_LEVEL": "WARNING"}
        )
        store = DatabaseSessionStore(settings)
        await store.ping()
        try:
            row = await _run_from_spec(store, settings, spec)
        finally:
            await store.aclose()
        result_path.write_text(json.dumps(row), encoding="utf-8")
        return 0 if row.get("status") == "ok" else 1
    except SystemExit:
        raise
    except Exception as exc:
        result_path.write_text(
            json.dumps(
                {
                    "kind": spec.get("kind"),
                    "status": "error",
                    "reason": f"{type(exc).__name__}: {exc}",
                    "rep": spec.get("rep"),
                    **_blank_phases(float(spec.get("window_s") or 1.0)),
                }
            ),
            encoding="utf-8",
        )
        return 1


async def _repeat(
    spec: dict[str, Any],
    *,
    reps: int,
    isolate: bool,
    store: DatabaseSessionStore | None,
    settings: Settings | None,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for rep in range(reps):
        one = {**spec, "rep": rep, "do_replay": bool(spec["do_replay"]) and rep == 0}
        one["profile"] = bool(spec["profile"]) and rep == 0
        print(f"starting {one['kind']} rep {rep + 1}/{reps}", flush=True)
        if isolate:
            row = _spawn(one)
        else:
            assert store is not None and settings is not None
            row = await _run_from_spec(store, settings, one)
        rows.append(row)
        _print_row(row)
    combined = combine_repetitions(rows)
    _print_row(combined)
    return combined


async def _async_main(args: argparse.Namespace) -> int:
    logging.getLogger().setLevel(logging.WARNING)
    raw = os.environ.get("DATABASE_URL", "")
    bench_url = await prepare_benchmark_database(raw)
    hardware = probe_hardware()
    version = await _postgres_version(bench_url)
    settings = Settings.model_validate(
        {"DATABASE_URL": bench_url, "DATA_SOURCE": "synthetic", "LOG_LEVEL": "WARNING"}
    )
    store = DatabaseSessionStore(settings)
    await store.ping()
    run_id = uuid.uuid4().hex
    probe = _config(name="bench-cal", seed=args.seed, instances=1, duration_ms=1000)
    t0 = time.perf_counter()
    probed = SyntheticExchange(probe).run()
    cal = _Calibration(
        markets_per_instance=len(probed.catalog.markets),
        messages_per_instance_ms=len(probed.messages) / 1000.0,
        generate_s=time.perf_counter() - t0,
    )
    if cal.markets_per_instance <= 0 or cal.messages_per_instance_ms <= 0:
        _refuse("calibration produced no markets or messages", 1)
    workloads: list[dict[str, Any]] = []
    pressure: dict[str, Any] = {"bounded": False}
    try:
        targets = [int(part) for part in args.markets.split(",") if part.strip()]
        for target in targets:
            instances = max(1, (target + cal.markets_per_instance - 1) // cal.markets_per_instance)
            duration = _duration_ms(instances, args.message_target, cal)
            spec = _spec(
                run_id=run_id,
                kind="unpaced",
                target_markets=target,
                instances=instances,
                duration_ms=duration,
                rate=0,
                seed=args.seed,
                faults=(),
                budget_s=args.budget_s,
                profile=args.profile and target == args.focused,
                do_replay=target == args.focused,
                watch_recovery=False,
                measure_detections=False,
                inject_gap=False,
                inject_scenarios=False,
                window_s=args.window_s,
                rep=0,
                require_steady=not args.smoke,
            )
            workloads.append(
                await _repeat(
                    spec,
                    reps=args.reps,
                    isolate=args.isolate,
                    store=store,
                    settings=settings,
                )
            )
        if args.paced_rate > 0:
            instances = max(
                1, (args.focused + cal.markets_per_instance - 1) // cal.markets_per_instance
            )
            duration = _duration_ms(instances, min(args.message_target, 2000), cal)
            spec = _spec(
                run_id=run_id,
                kind="paced",
                target_markets=args.focused,
                instances=instances,
                duration_ms=duration,
                rate=args.paced_rate,
                seed=args.seed,
                faults=(),
                budget_s=args.budget_s,
                profile=False,
                do_replay=False,
                watch_recovery=False,
                measure_detections=False,
                inject_gap=False,
                inject_scenarios=False,
                window_s=args.window_s,
                rep=0,
                require_steady=not args.smoke,
            )
            workloads.append(
                await _repeat(
                    spec, reps=args.reps, isolate=args.isolate, store=store, settings=settings
                )
            )
        if not args.smoke:
            spec = _spec(
                run_id=run_id,
                kind="inconsistent",
                target_markets=cal.markets_per_instance,
                instances=1,
                duration_ms=INCONSISTENT_DURATION_MS,
                rate=0,
                seed=INCONSISTENT_SEED,
                faults=(),
                budget_s=args.budget_s,
                profile=False,
                do_replay=False,
                watch_recovery=False,
                measure_detections=True,
                inject_gap=False,
                inject_scenarios=True,
                window_s=args.window_s,
                rep=0,
                require_steady=True,
            )
            workloads.append(
                await _repeat(
                    spec, reps=args.reps, isolate=args.isolate, store=store, settings=settings
                )
            )
        recovery = _spec(
            run_id=run_id,
            kind="recovery",
            target_markets=None,
            instances=1,
            duration_ms=12_000,
            rate=0,
            seed=args.seed,
            faults=(FaultSpec(FaultKind.GAP, 4_000, "econ"),),
            budget_s=args.budget_s,
            profile=False,
            do_replay=False,
            watch_recovery=True,
            measure_detections=False,
            inject_gap=True,
            inject_scenarios=False,
            window_s=args.window_s,
            rep=0,
            require_steady=False,
        )
        workloads.append(
            await _repeat(
                recovery, reps=args.reps, isolate=args.isolate, store=store, settings=settings
            )
        )
        pressure = await _backpressure()
    finally:
        await store.aclose()
    report = {
        "synthetic": True,
        "kalshi_contacted": False,
        "ci_runs_benchmark": False,
        "ci_runs_smoke": True,
        "smoke": bool(args.smoke),
        "hardware": hardware,
        "postgres": version,
        "database": _redacted(bench_url),
        "command": sys.argv,
        "log_level": "WARNING",
        "message_target": args.message_target,
        "focused_markets": args.focused,
        "paced_rate": args.paced_rate,
        "budget_s": args.budget_s,
        "seed": args.seed,
        "inconsistent_seed": INCONSISTENT_SEED,
        "repetitions": args.reps,
        "isolated_processes": bool(args.isolate),
        "observation_window_s": args.window_s,
        "cold_start_excluded_from_steady_state": True,
        "latency_window": LATENCY_WINDOW,
        "database_guard": {
            "omitted_port_is": 5432,
            "port_5432_requires": "BENCHMARK_ALLOW_PORT_5432=1",
            "allowed_hosts_without_remote_override": sorted(LOCAL_HOSTS),
            "remote_requires": "BENCHMARK_ALLOW_REMOTE=1",
        },
        "calibration": {
            "markets_per_instance": cal.markets_per_instance,
            "messages_per_instance_ms": cal.messages_per_instance_ms,
            "generate_s": round(cal.generate_s, 4),
            "duration_ms": 1000,
            "instances": 1,
        },
        "workloads": workloads,
        "backpressure": pressure,
        "definitions": {
            "messages_per_s": (
                "median steady-state listener entries per second; cold-start windows excluded"
            ),
            "full_run_messages_per_s": (
                "listener entries / entire worker wall time, including session open; "
                "not the steady-state figure"
            ),
            "cold_start": COLD_SCOPE,
            "steady_state": STEADY_SCOPE,
            "session_open_s": (
                "catalog upsert before the first listener entry; excluded from cold and "
                "steady throughput"
            ),
            "processing_latency": "perf_counter around on_message, before outbox backpressure",
            "detection_latency": "perf_counter around pricing evaluate(), one sample per call",
            "certificate_serialization": "CPU time in Evaluation.certificate_json",
            "detection_persistence": (
                "DetectionWriter.write through commit, including the outbox insert"
            ),
            "notification_outbox_writes": "outbox rows committed with detection events",
            "end_to_end_latency": "book update received until the detection transaction commits",
            "db_rows_per_s": "journal, outbox, and checkpoint rows divided by worker wall time",
            "recovery_segments": SEGMENT_SCOPE,
            "replay_entries_per_s": "second deterministic replay_catalog pass",
        },
    }
    if args.smoke and smoke_report_errors(report):
        print(
            "smoke report failed structural checks: " + ", ".join(smoke_report_errors(report)),
            file=sys.stderr,
        )
        code = 1
    else:
        code = workload_exit_code(workloads, backpressure_bounded=bool(pressure.get("bounded")))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {args.output}")
    return code


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic Consistency Engine benchmark")
    parser.add_argument("--markets", default="50,250,1000,5000")
    parser.add_argument("--message-target", type=int, default=4000)
    parser.add_argument("--focused", type=int, default=250)
    parser.add_argument("--paced-rate", type=float, default=500)
    parser.add_argument("--budget-s", type=float, default=180)
    parser.add_argument("--seed", type=int, default=606)
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--window-s", type=float, default=0.5)
    parser.add_argument("--isolate", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--child", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=ROOT / "docs" / "benchmark-results.json")
    args = parser.parse_args()
    if args.child is not None:
        raise SystemExit(asyncio.run(_child_main(args.child)))
    if args.smoke:
        args.markets = "27"
        args.message_target = 400
        args.paced_rate = 0
        args.reps = 1
        args.isolate = False
        args.window_s = 0.25
        args.budget_s = min(args.budget_s, 90.0)
        args.profile = False
    if args.message_target <= 0 or args.budget_s <= 0 or args.reps <= 0 or args.window_s <= 0:
        _refuse("message target, budget, repetitions, and window must be positive")
    if args.paced_rate < 0:
        _refuse("paced rate must be >= 0")
    raise SystemExit(asyncio.run(_async_main(args)))


if __name__ == "__main__":
    main()

"""Synthetic performance harness. Never contacts Kalshi.

Measures the real worker (ingestion, detection, PostgreSQL) on generated sessions.
Creates database ``consistency_bench`` on the same host and port as ``DATABASE_URL``
so the demo database is not filled with benchmark rows. Refuses port 5432.

``make benchmark`` runs this script. A missed speed target is reported and still
exits 0. Exit 1 means a measurement did not complete (timeout, replay mismatch,
unbounded queue, or an exception). Exit 2 means ``DATABASE_URL`` is missing or
points at a refused server. CI does not run this target.

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
import subprocess
import sys
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import asyncpg
from sqlalchemy import text
from sqlalchemy.engine import make_url

import consistency_pipeline.engine as engine_mod
from consistency_connectors.base import ConnectionState
from consistency_connectors.settings import Settings
from consistency_connectors.sources.synthetic import SyntheticDataSource
from consistency_core.serialization import sha256_of
from consistency_pipeline.lifecycle import DetectionEvent
from consistency_pipeline.replay import compare_outcomes, replay_catalog
from consistency_pipeline.service import Outbox, PipelineListener
from consistency_simulation.exchange import FaultKind, FaultSpec, SessionConfig, SyntheticExchange
from consistency_simulation.families import SIM_EPOCH
from consistency_simulation.stream import playback
from consistency_worker.bootstrap import SessionInputs, fee_calculator, relationships_for
from consistency_worker.service import WorkerService
from consistency_worker.store import DatabaseSessionStore

ROOT = Path(__file__).resolve().parents[1]
BENCH_DB = "consistency_bench"
TICK_MS = 250
LATENCY_WINDOW = 10_000


def _refuse(message: str, code: int = 2) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(code)


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


def _hardware() -> dict[str, Any]:
    def sh(*args: str) -> str:
        return subprocess.check_output(args, text=True).strip()

    mem = int(sh("sysctl", "-n", "hw.memsize"))
    return {
        "cpu": sh("sysctl", "-n", "machdep.cpu.brand_string"),
        "memory_bytes": mem,
        "os": f"{platform.system()} {sh('sw_vers', '-productVersion')} ({platform.machine()})",
        "python": platform.python_version(),
    }


def _redacted(url: str) -> str:
    parsed = make_url(url)
    return parsed.set(password="***").render_as_string(hide_password=False)


class RatedSynthetic(SyntheticDataSource):
    """Same generated stream. ``rate`` > 0 sleeps a fixed gap between messages."""

    def __init__(self, config: SessionConfig, *, rate: float) -> None:
        super().__init__(config, speed=None)
        self.rate = rate

    async def subscribe_orderbooks(self, market_ids: Sequence[str] | None = None):
        del market_ids
        self._state = ConnectionState.CONNECTED
        interval = 0.0 if self.rate <= 0 else 1.0 / self.rate
        # Deadline is start-to-start. Processing time counts toward the interval.
        # Resetting the deadline after the sleep would add a full interval on top.
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

    def _log_events(self, events: list[DetectionEvent]) -> None:
        for ev in events:
            ns = ev.timing.internal_latency_ns
            if ns is not None:
                self.detection_ns.append(ns)
        super()._log_events(events)


@dataclass
class _Calibration:
    markets_per_instance: int
    messages_per_instance_ms: float
    generate_s: float


def _duration_ms(instances: int, message_target: int, cal: _Calibration) -> int:
    """Simulated duration that should yield about ``message_target`` messages.

    ``messages_per_instance_ms`` is already a count per millisecond, so ``raw_ms``
    is a duration. Do not scale it by 1000 again.
    """
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
) -> SessionConfig:
    return SessionConfig(
        name=name,
        seed=seed,
        duration_ms=duration_ms,
        description="synthetic benchmark session",
        tick_ms=TICK_MS,
        instances=instances,
        faults=faults,
    )


async def _ensure_database(admin_url: str, bench_url: str) -> None:
    parsed = make_url(admin_url)
    conn = await asyncpg.connect(
        host=parsed.host,
        port=parsed.port,
        user=parsed.username,
        password=parsed.password,
        database="postgres",
    )
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
        entries,
        inputs.catalog,
        inputs.relationships,
        inputs.fees,
        session_id=session_id,
    )
    second_started = time.perf_counter()
    second = replay_catalog(
        entries,
        inputs.catalog,
        inputs.relationships,
        inputs.fees,
        session_id=session_id,
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
    outbox = Outbox(_SlowSink(delay_s), None, maxsize=cap)
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
    watch: bool,
) -> dict[str, Any]:
    name = f"bench-{kind}"
    cfg = _config(name=name, seed=seed, instances=instances, duration_ms=duration_ms, faults=faults)
    generated = time.perf_counter()
    source = RatedSynthetic(cfg, rate=rate) if rate > 0 else SyntheticDataSource(cfg, speed=None)
    generate_s = time.perf_counter() - generated
    discovered = time.perf_counter()
    relationships = relationships_for(source.catalog, reviews, as_of=SIM_EPOCH)
    discover_s = time.perf_counter() - discovered
    markets = len(source.catalog.markets)
    messages = len(source.messages)
    print(
        f"generated {kind} markets={markets} messages={messages} duration_ms={duration_ms}",
        file=sys.stderr,
        flush=True,
    )
    if messages > 25_000:
        return {
            "kind": kind,
            "target_markets": target_markets,
            "markets": markets,
            "instances": instances,
            "duration_ms": duration_ms,
            "generated_messages": messages,
            "status": "skipped",
            "reason": f"{messages} generated messages exceeds the 25000 safety cap",
        }
    record: dict[str, Any] = {
        "kind": kind,
        "target_markets": target_markets,
        "markets": markets,
        "instances": instances,
        "duration_ms": duration_ms,
        "rate_per_s": rate,
        "seed": seed,
        "generated_messages": messages,
        "relationships": len(relationships),
        "generate_s": round(generate_s, 4),
        "discover_s": round(discover_s, 4),
        "tick_ms": TICK_MS,
    }
    if generate_s + discover_s > budget_s:
        record["status"] = "skipped"
        record["reason"] = (
            f"generation plus relationship discovery took {generate_s + discover_s:.1f}s, "
            f"past the {budget_s:.0f}s budget, so the worker was not started"
        )
        return record
    fingerprint = sha256_of(
        {
            "run": run_id,
            "kind": kind,
            "markets": markets,
            "duration_ms": duration_ms,
            "rate": rate,
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
    recovery_s: list[float] = []
    seen_sids: set[str] = set()
    recovery_burst: dict[str, Any] = {"sid": None, "start": None, "end": None, "snapshots": 0}
    update_kinds: dict[str, int] = {}
    eval_ns: list[int] = []
    original_evaluate = engine_mod.evaluate
    original_on_message = PipelineListener.on_message

    def _timed_evaluate(*args: Any, **kwargs: Any) -> Any:
        started_ns = time.perf_counter_ns()
        try:
            return original_evaluate(*args, **kwargs)
        finally:
            eval_ns.append(time.perf_counter_ns() - started_ns)

    def _close_burst() -> None:
        start = recovery_burst["start"]
        end = recovery_burst["end"]
        if isinstance(start, float) and isinstance(end, float):
            recovery_s.append(end - start)
        recovery_burst["sid"] = None
        recovery_burst["start"] = None

    async def _timed_message(self: PipelineListener, msg: Any, updates: Sequence[Any]) -> None:
        # A recorded gap is not delivered. The exchange emits fresh snapshots on a new
        # subscription id. That burst is the recovery the book manager applies.
        event = getattr(msg, "event", None)
        sid = getattr(event, "sid", None)
        event_name = type(event).__name__
        is_snapshot = event_name == "OrderBookSnapshotEvent"
        is_delta = event_name == "OrderBookDeltaEvent"
        for update in updates:
            update_kinds[update.kind] = update_kinds.get(update.kind, 0) + 1
        if (
            is_snapshot
            and isinstance(sid, str)
            and seen_sids
            and sid not in seen_sids
            and recovery_burst["sid"] is None
        ):
            recovery_burst["sid"] = sid
            recovery_burst["start"] = time.perf_counter()
        await original_on_message(self, msg, updates)
        if recovery_burst["sid"] is not None and is_snapshot and sid == recovery_burst["sid"]:
            recovery_burst["snapshots"] = int(recovery_burst["snapshots"]) + 1
            recovery_burst["end"] = time.perf_counter()
        elif recovery_burst["sid"] is not None and is_delta:
            _close_burst()
        if isinstance(sid, str):
            seen_sids.add(sid)

    engine_mod.evaluate = _timed_evaluate
    if watch:
        PipelineListener.on_message = _timed_message  # type: ignore[method-assign]
    profiler = cProfile.Profile() if profile else None
    utime, stime = _cpu()
    started = time.perf_counter()
    status = "ok"
    reason: str | None = None
    try:
        if profiler is not None:
            profiler.enable()
        await asyncio.wait_for(worker.run(), timeout=budget_s)
    except TimeoutError:
        status = "timeout"
        reason = f"worker still running after {budget_s:.0f}s"
    except Exception as exc:
        status = "error"
        reason = f"{type(exc).__name__}: {exc}"
    finally:
        if profiler is not None:
            profiler.disable()
        engine_mod.evaluate = original_evaluate
        PipelineListener.on_message = original_on_message
        if watch and recovery_burst["sid"] is not None:
            _close_burst()
    wall_s = time.perf_counter() - started
    cpu_u, cpu_s = _cpu()
    cpu_seconds = (cpu_u - utime) + (cpu_s - stime)
    record["status"] = status
    record["reason"] = reason
    record["wall_s"] = round(wall_s, 4)
    record["cpu_seconds"] = round(cpu_seconds, 4)
    record["cpu_percent_one_core"] = None if wall_s <= 0 else round(100.0 * cpu_seconds / wall_s, 2)
    record["rss_bytes"] = _rss_bytes()
    record["ru_maxrss"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if watch:
        record["update_kinds"] = update_kinds
        record["recovery_samples"] = len(recovery_s)
        recovery_ns = [int(sample * 1_000_000_000) for sample in recovery_s]
        record["recovery_p50_ms"] = None if not recovery_ns else _ms(_percentile(recovery_ns, 50))
        record["recovery_max_ms"] = None if not recovery_s else round(max(recovery_s) * 1000.0, 4)
    listener = worker.listener
    runner = None if worker.supervisor is None else worker.supervisor.runner
    if listener is None or runner is None or status != "ok":
        if profiler is not None:
            record["profile"] = _profile_text(profiler)
        return record
    processed = listener.stats.entries
    record["messages_processed"] = processed
    record["source_messages"] = runner.stats.messages
    record["messages_per_s"] = None if wall_s <= 0 else round(processed / wall_s, 2)
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
    record["recovery_snapshots"] = int(recovery_burst["snapshots"] or 0)
    # The worker closes the session before returning. The id is stored on the row.
    session_id = await _session_id(store, fingerprint)
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
        if do_replay:
            record["replay"] = await _replay(store, session_id, inputs)
        await _delete_session(store, session_id)
    if profiler is not None:
        record["profile"] = _profile_text(profiler)
    return record


def _profile_text(profiler: cProfile.Profile) -> str:
    stream = io.StringIO()
    stats = pstats.Stats(profiler, stream=stream).sort_stats("cumulative")
    stats.print_stats(25)
    return stream.getvalue()


async def _session_id(store: DatabaseSessionStore, fingerprint: str) -> str:
    query = text(
        """
        SELECT session_id
        FROM ingestion_sessions
        WHERE fingerprint = :fingerprint
        ORDER BY started_at DESC
        LIMIT 1
        """
    )
    async with store._inner.sessions() as session:
        found = (await session.execute(query, {"fingerprint": fingerprint})).scalar_one_or_none()
    return "" if found is None else str(found)


def _check_url(raw: str) -> tuple[str, str]:
    if not raw or raw.startswith("sqlite"):
        _refuse("DATABASE_URL must be a postgresql+asyncpg URL. Refusing to invent results.")
    parsed = make_url(raw)
    if parsed.drivername != "postgresql+asyncpg":
        _refuse(f"DATABASE_URL driver is {parsed.drivername}. PostgreSQL via asyncpg is required.")
    if parsed.port == 5432:
        _refuse("Refusing port 5432. Point DATABASE_URL at the Docker Postgres 16 on port 5433.")
    bench = parsed.set(database=BENCH_DB).render_as_string(hide_password=False)
    return raw, bench


async def _async_main(args: argparse.Namespace) -> int:
    logging.getLogger().setLevel(logging.WARNING)
    raw, bench_url = _check_url(os.environ.get("DATABASE_URL", ""))
    hardware = _hardware()
    await _ensure_database(raw, bench_url)
    version = await _postgres_version(bench_url)
    settings = Settings.model_validate(
        {
            "DATABASE_URL": bench_url,
            "DATA_SOURCE": "synthetic",
            "LOG_LEVEL": "WARNING",
        }
    )
    store = DatabaseSessionStore(settings)
    await store.ping()
    reviews = ROOT / "fixtures" / "relationships" / "manual-reviews.yaml"
    fees_dir = ROOT / "fixtures" / "fees"
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
    failed = False
    targets = [int(part) for part in args.markets.split(",") if part.strip()]
    try:
        for target in targets:
            instances = max(1, (target + cal.markets_per_instance - 1) // cal.markets_per_instance)
            duration = _duration_ms(instances, args.message_target, cal)
            profile = args.profile and target == args.focused
            replay = target == args.focused
            row = await _run_workload(
                store=store,
                settings=settings,
                reviews=reviews,
                fees_dir=fees_dir,
                run_id=run_id,
                kind="unpaced",
                target_markets=target,
                instances=instances,
                duration_ms=duration,
                rate=0,
                seed=args.seed,
                faults=(),
                budget_s=args.budget_s,
                profile=profile,
                do_replay=replay,
                watch=False,
            )
            workloads.append(row)
            _print_row(row)
            if row["status"] == "error":
                failed = True
            replay_row = row.get("replay") or {}
            if replay_row.get("identical") is False:
                failed = True
        if args.paced_rate > 0:
            instances = max(
                1, (args.focused + cal.markets_per_instance - 1) // cal.markets_per_instance
            )
            duration = _duration_ms(instances, min(args.message_target, 2000), cal)
            paced = await _run_workload(
                store=store,
                settings=settings,
                reviews=reviews,
                fees_dir=fees_dir,
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
                watch=False,
            )
            workloads.append(paced)
            _print_row(paced)
            if paced["status"] == "error":
                failed = True
        recovery = await _run_workload(
            store=store,
            settings=settings,
            reviews=reviews,
            fees_dir=fees_dir,
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
            watch=True,
        )
        workloads.append(recovery)
        _print_row(recovery)
        if recovery["status"] != "ok" or not recovery.get("recovery_samples"):
            failed = True
        pressure = await _backpressure()
        if not pressure["bounded"]:
            failed = True
    finally:
        await store.aclose()
    report = {
        "synthetic": True,
        "kalshi_contacted": False,
        "ci_runs_benchmark": False,
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
        "latency_window": LATENCY_WINDOW,
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
            "messages_per_s": "listener entries / worker wall time, including database waits",
            "processing_latency": "perf_counter around on_message, before outbox backpressure",
            "detection_latency": "perf_counter around pricing evaluate(), one sample per call",
            "db_rows_per_s": "journal, outbox, and checkpoint rows divided by worker wall time",
            "recovery": "per-market wall time from unsynchronized to synchronized",
            "replay_entries_per_s": "second deterministic replay_catalog pass",
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {args.output}")
    return 1 if failed else 0


async def _postgres_version(url: str) -> str:
    parsed = make_url(url)
    conn = await asyncpg.connect(
        host=parsed.host,
        port=parsed.port,
        user=parsed.username,
        password=parsed.password,
        database=parsed.database,
    )
    try:
        version = await conn.fetchval("SHOW server_version")
    finally:
        await conn.close()
    return str(version)


def _print_row(row: dict[str, Any]) -> None:
    kind = row["kind"]
    markets = row.get("markets")
    status = row["status"]
    rate = row.get("messages_per_s")
    wall = row.get("wall_s")
    print(
        f"{kind} markets={markets} status={status} messages_per_s={rate} wall_s={wall}",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic Consistency Engine benchmark")
    parser.add_argument("--markets", default="50,250,1000,5000")
    parser.add_argument("--message-target", type=int, default=4000)
    parser.add_argument("--focused", type=int, default=250)
    parser.add_argument("--paced-rate", type=float, default=500)
    parser.add_argument("--budget-s", type=float, default=180)
    parser.add_argument("--seed", type=int, default=606)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "docs" / "benchmark-results.json",
    )
    args = parser.parse_args()
    if args.message_target <= 0 or args.budget_s <= 0:
        _refuse("message target and budget must be positive")
    if args.paced_rate < 0:
        _refuse("paced rate must be >= 0")
    raise SystemExit(asyncio.run(_async_main(args)))


if __name__ == "__main__":
    main()

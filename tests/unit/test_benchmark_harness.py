"""Phase 13 audit regressions for the synthetic benchmark harness.

These tests do not invent throughput numbers. They lock the guard, the exit
status, cold-start separation, recovery segments, and the inconsistency
workload's lifecycle checks.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
import scripts.benchmark as benchmark

_LOCAL = "postgresql+asyncpg://consistency:consistency@127.0.0.1:5433/consistency"


def _clear_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BENCHMARK_ALLOW_PORT_5432", raising=False)
    monkeypatch.delenv("BENCHMARK_ALLOW_REMOTE", raising=False)


def test_omitted_port_is_treated_as_5432(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_flags(monkeypatch)
    with pytest.raises(SystemExit) as caught:
        benchmark._check_url("postgresql+asyncpg://consistency:consistency@127.0.0.1/consistency")
    assert caught.value.code == 2


def test_explicit_5432_is_refused_unless_opted_in(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_flags(monkeypatch)
    with pytest.raises(SystemExit) as caught:
        benchmark._check_url(
            "postgresql+asyncpg://consistency:consistency@127.0.0.1:5432/consistency"
        )
    assert caught.value.code == 2
    monkeypatch.setenv("BENCHMARK_ALLOW_PORT_5432", "1")
    _raw, bench = benchmark._check_url(
        "postgresql+asyncpg://consistency:consistency@127.0.0.1:5432/consistency"
    )
    assert ":5432/" in bench


def test_truthy_words_do_not_enable_the_port_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BENCHMARK_ALLOW_PORT_5432", "true")
    with pytest.raises(SystemExit):
        benchmark._check_url(
            "postgresql+asyncpg://consistency:consistency@127.0.0.1:5432/consistency"
        )


def test_non_local_host_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_flags(monkeypatch)
    with pytest.raises(SystemExit) as caught:
        benchmark._check_url(
            "postgresql+asyncpg://consistency:consistency@db.example.com:5433/consistency"
        )
    assert caught.value.code == 2
    monkeypatch.setenv("BENCHMARK_ALLOW_REMOTE", "1")
    benchmark._check_url(
        "postgresql+asyncpg://consistency:consistency@db.example.com:5433/consistency"
    )


def test_loopback_hosts_on_5433_are_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_flags(monkeypatch)
    benchmark._check_url(_LOCAL)
    benchmark._check_url("postgresql+asyncpg://consistency:consistency@localhost:5433/consistency")
    benchmark._check_url("postgresql+asyncpg://consistency:consistency@[::1]:5433/consistency")


@pytest.mark.asyncio
async def test_guard_failure_does_not_migrate(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_flags(monkeypatch)

    async def migrate(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("migrate")

    monkeypatch.setattr(benchmark, "_ensure_database", migrate)
    with pytest.raises(SystemExit):
        await benchmark.prepare_benchmark_database(
            "postgresql+asyncpg://consistency:consistency@127.0.0.1/consistency"
        )


def test_timeout_workload_exits_nonzero() -> None:
    assert benchmark.workload_exit_code([{"kind": "unpaced", "status": "timeout"}]) == 1


def test_skipped_workload_exits_nonzero() -> None:
    assert benchmark.workload_exit_code([{"kind": "unpaced", "status": "skipped"}]) == 1


def test_successful_smoke_report_exits_zero() -> None:
    report = _smoke_report()
    assert benchmark.smoke_report_errors(report) == []
    assert benchmark.workload_exit_code(report["workloads"]) == 0


def test_steady_state_excludes_the_cold_window() -> None:
    # t=0 is worker start. The first listener entry is at 0.50s (session open).
    # Windows of 1s start there. The 50-message cold window is not a steady sample.
    samples = [(0.0, 0), (0.50, 50), (1.50, 70), (2.50, 90), (3.50, 110)]
    windows = benchmark.observation_windows(samples, window_s=1.0, wall_s=4.0)
    assert windows["session_open_s"] == 0.5
    assert windows["cold_start"]["messages"] == 50
    assert windows["cold_start"]["messages_per_s"] == 50.0
    assert windows["cold_start"]["included_in_steady_state"] is False
    assert windows["steady_state"]["per_window_messages_per_s"] == [20.0, 20.0]
    assert 50.0 not in windows["steady_state"]["per_window_messages_per_s"]
    aggregated = benchmark.aggregate_throughput([100.0], [10.0, 10.0])
    assert aggregated["messages_per_s"] == 10.0
    assert aggregated["cold_start"]["included_in_steady_state"] is False
    assert aggregated["cold_start"]["messages_per_s"]["median"] == 100.0
    assert aggregated["steady_state"]["messages_per_s"]["max"] == 10.0


def test_variability_reports_min_median_max_mean_and_stdev() -> None:
    summary = benchmark.variability([10.0, 30.0, 20.0])
    assert summary["n"] == 3
    assert summary["min"] == 10.0
    assert summary["median"] == 20.0
    assert summary["max"] == 30.0
    assert summary["mean"] == 20.0
    assert summary["stdev"] is not None and summary["stdev"] > 0


def test_harness_records_each_recovery_segment() -> None:
    segments = benchmark.segments_from_times(
        gap_detection_s=0.001,
        recovery_request_s=0.0004,
        snapshot_arrival_s=0.008,
        full_resynchronization_s=0.012,
    )
    row = {"kind": "recovery", "status": "ok", "recovery_segments": segments}
    assert benchmark.recovery_segments_ok(row)
    for key in (
        "gap_detection_ms",
        "recovery_request_ms",
        "snapshot_arrival_ms",
        "full_resynchronization_ms",
    ):
        assert segments[key]["n"] == 1
        assert segments[key]["median"] is not None
    assert "recovery_ms" not in segments
    assert "recovery_p50_ms" not in row
    assert benchmark.workload_exit_code([row]) == 0


def test_missing_recovery_segment_fails_the_benchmark() -> None:
    row = {
        "kind": "recovery",
        "status": "ok",
        "recovery_segments": {"gap_detection_ms": {"n": 1, "median": 1.0}},
    }
    assert benchmark.recovery_segments_ok(row) is False
    assert benchmark.workload_exit_code([row]) == 1


def test_recovery_stream_produces_gap_request_and_resync() -> None:
    episode = benchmark.synthetic_recovery_episode(seed=606)
    assert episode["gap_detected"] is True
    assert episode["recovery_requested"] is True
    assert episode["snapshot_arrived"] is True
    assert episode["resynchronized"] is True


def test_inconsistent_lifecycle_requires_each_kind() -> None:
    complete = {"OPENED": 1, "UPDATED": 2, "RESOLVED": 1, "EXPIRED": 1}
    assert benchmark.lifecycle_complete(complete)
    missing = {"OPENED": 1, "UPDATED": 0, "RESOLVED": 1, "EXPIRED": 1}
    assert benchmark.lifecycle_complete(missing) is False
    row = {"kind": "inconsistent", "status": "ok", "lifecycle_counts": missing}
    assert benchmark.workload_exit_code([row]) == 1


def test_float_money_is_rejected_and_decimals_match() -> None:
    assert benchmark.json_contains_float({"metrics": {"net_edge": 0.1}})
    assert benchmark.json_contains_float({"metrics": {"net_edge": "0.10"}}) is False
    assert benchmark.money_equal(Decimal("0.10"), Decimal("0.1"))
    assert benchmark.money_equal(Decimal("0.10"), Decimal("0.2")) is False
    assert benchmark.money_equal(None, None)


def test_linux_hardware_does_not_call_sysctl() -> None:
    def sysctl(_args: list[str]) -> str:
        raise AssertionError("sysctl")

    def read_text(path: str) -> str:
        if path.endswith("cpuinfo"):
            return "processor\t: 0\nmodel name\t: Test CPU\n"
        if path.endswith("meminfo"):
            return "MemTotal:        8388608 kB\n"
        raise AssertionError(path)

    info = benchmark.probe_hardware(system="Linux", read_text=read_text, sysctl=sysctl)
    assert info["cpu"] == "Test CPU"
    assert info["memory_bytes"] == 8388608 * 1024
    assert info["os"].startswith("Linux")
    assert info["python"]


def test_darwin_hardware_uses_sysctl() -> None:
    calls: list[list[str]] = []

    def sysctl(args: list[str]) -> str:
        calls.append(args)
        if args[-1] == "hw.memsize":
            return "17179869184"
        if args[-1] == "machdep.cpu.brand_string":
            return "Apple M3"
        if args[-1] == "-productVersion":
            return "26.6.2"
        raise AssertionError(args)

    info = benchmark.probe_hardware(system="Darwin", read_text=lambda _p: "", sysctl=sysctl)
    assert info["cpu"] == "Apple M3"
    assert info["memory_bytes"] == 17179869184
    assert "26.6.2" in info["os"]
    assert calls


def _smoke_report() -> dict[str, object]:
    segments = benchmark.segments_from_times(
        gap_detection_s=0.001,
        recovery_request_s=0.0002,
        snapshot_arrival_s=0.004,
        full_resynchronization_s=0.006,
    )
    cold = {
        "included_in_steady_state": False,
        "scope": benchmark.COLD_SCOPE,
        "messages_per_s": {"n": 1, "min": 10.0, "median": 10.0, "max": 10.0},
    }
    steady = {
        "scope": benchmark.STEADY_SCOPE,
        "messages_per_s": {"n": 1, "min": 12.0, "median": 12.0, "max": 12.0},
    }
    return {
        "synthetic": True,
        "kalshi_contacted": False,
        "hardware": {"cpu": "Test", "memory_bytes": 1, "os": "Linux test", "python": "3.12.11"},
        "workloads": [
            {
                "kind": "unpaced",
                "status": "ok",
                "scope": benchmark.STEADY_SCOPE,
                "cold_start": cold,
                "steady_state": steady,
            },
            {
                "kind": "recovery",
                "status": "ok",
                "scope": benchmark.STEADY_SCOPE,
                "cold_start": cold,
                "steady_state": steady,
                "recovery_segments": segments,
            },
        ],
    }

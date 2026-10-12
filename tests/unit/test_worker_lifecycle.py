"""Worker lifecycle contracts that do not need a database."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_worker_healthcheck_reports_lifecycle_not_only_postgres() -> None:
    text = (ROOT / "docker-compose.yml").read_text()
    start = text.index("  worker:")
    end = text.index("  gateway:")
    block = text[start:end]
    assert "consistency_worker" in block
    assert "health" in block
    assert "ping(" not in block


def test_worker_entrypoint_schedules_maintenance_after_ingestion() -> None:
    script = (ROOT / "deploy/worker-entry.sh").read_text()
    assert "consistency_worker maintain" in script
    assert "sleep infinity" not in script


def test_worker_health_distinguishes_completed_running_and_failed() -> None:
    from consistency_worker.health import describe_health

    assert describe_health("completed") == ("completed", 0)
    assert describe_health("running") == ("running", 0)
    assert describe_health("failed") == ("failed", 1)
    assert describe_health("completed")[0] != describe_health("running")[0]
    assert describe_health("failed")[1] == 1


def test_assess_health_separates_session_status_from_maintenance_freshness() -> None:
    from consistency_worker.health import assess_health

    now = 1_000.0
    fresh = now - 1.0
    stale = now - 120.0
    assert assess_health(
        "completed",
        expect_maintenance=True,
        heartbeat_epoch=fresh,
        now=now,
        stale_after_s=30.0,
        retention_state="running",
        process_alive=True,
    ) == ("completed", 0)
    assert assess_health(
        "completed",
        expect_maintenance=True,
        heartbeat_epoch=fresh,
        now=now,
        stale_after_s=30.0,
    ) == ("unhealthy", 1)
    assert assess_health(
        "completed",
        expect_maintenance=True,
        heartbeat_epoch=fresh,
        now=now,
        stale_after_s=30.0,
        retention_state="exited",
        process_alive=True,
    ) == ("unhealthy", 1)
    assert assess_health(
        "completed",
        expect_maintenance=True,
        heartbeat_epoch=fresh,
        now=now,
        stale_after_s=30.0,
        retention_state="running",
        process_alive=False,
    ) == ("unhealthy", 1)
    assert assess_health(
        "completed",
        expect_maintenance=True,
        heartbeat_epoch=stale,
        now=now,
        stale_after_s=30.0,
        retention_state="running",
        process_alive=True,
    ) == ("unhealthy", 1)
    assert assess_health(
        "failed",
        expect_maintenance=True,
        heartbeat_epoch=fresh,
        now=now,
        stale_after_s=30.0,
        retention_state="running",
        process_alive=True,
    ) == ("failed", 1)
    assert assess_health(
        "completed",
        expect_maintenance=False,
        heartbeat_epoch=None,
        now=now,
        stale_after_s=30.0,
    ) == ("completed", 0)
    assert assess_health(
        "completed",
        expect_maintenance=True,
        heartbeat_epoch=fresh,
        now=now,
        stale_after_s=30.0,
        retention_state="running",
        process_alive=True,
        consecutive_failures=4,
        max_failures=3,
    ) == ("unhealthy", 1)
    assert assess_health(
        "completed",
        expect_maintenance=True,
        heartbeat_epoch=fresh,
        now=now,
        stale_after_s=30.0,
        retention_state="running",
        process_alive=True,
        consecutive_failures=3,
        max_failures=3,
    ) == ("completed", 0)


def test_worker_entrypoint_expects_a_maintenance_heartbeat() -> None:
    script = (ROOT / "deploy" / "worker-entry.sh").read_text()
    assert "EXPECT_MAINTENANCE" in script
    assert "MAINTENANCE_HEARTBEAT_PATH" in script
    text = (ROOT / "docker-compose.yml").read_text()
    start = text.index("  worker:")
    end = text.index("  gateway:")
    block = text[start:end]
    assert 'EXPECT_MAINTENANCE: "1"' in block
    assert "MAINTENANCE_HEARTBEAT_PATH" in block

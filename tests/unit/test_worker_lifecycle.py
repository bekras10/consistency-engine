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

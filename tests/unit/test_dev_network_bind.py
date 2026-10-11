"""Host Postgres and the Next dev server bind to loopback. Production Postgres stays unpublished."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _compose_config(
    *files: str, project: str, env: dict[str, str] | None = None
) -> dict[str, object]:
    command = ["docker", "compose", "-p", project]
    for name in files:
        command.extend(["-f", name])
    command.extend(["config", "--format", "json"])
    merged = os.environ.copy()
    if env:
        merged.update(env)
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=merged,
        check=True,
        capture_output=True,
        text=True,
    )
    parsed = json.loads(result.stdout)
    assert isinstance(parsed, dict)
    return parsed


def _service(config: dict[str, object], name: str) -> dict[str, object]:
    services = config["services"]
    assert isinstance(services, dict)
    service = services[name]
    assert isinstance(service, dict)
    return service


def test_host_database_publishes_only_on_loopback() -> None:
    config = _compose_config(
        "compose.host-db.yml",
        project="consistency-bindcheck-config",
        env={"POSTGRES_PORT": "55991"},
    )
    ports = _service(config, "db").get("ports")
    assert isinstance(ports, list) and ports
    for entry in ports:
        assert isinstance(entry, dict)
        assert entry.get("host_ip") == "127.0.0.1"
        assert str(entry.get("published")) == "55991"
        assert entry.get("target") == 5432


def test_production_compose_does_not_publish_postgres() -> None:
    config = _compose_config(
        "docker-compose.yml",
        "compose.smoke.yml",
        project="consistency-bindcheck-prod",
        env={"WEB_PORT": "18080"},
    )
    db = _service(config, "db")
    ports = db.get("ports")
    assert ports in (None, [])
    frontend_ports = _service(config, "frontend").get("ports")
    assert isinstance(frontend_ports, list) and frontend_ports
    for entry in frontend_ports:
        assert isinstance(entry, dict)
        assert entry.get("host_ip") == "127.0.0.1"


def test_dev_script_binds_next_to_loopback() -> None:
    script = (ROOT / "scripts" / "dev.sh").read_text()
    assert "--hostname 127.0.0.1" in script or "-H 127.0.0.1" in script

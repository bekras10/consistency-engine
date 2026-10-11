"""Third-party credentials stay out of PostgreSQL and out of process arguments."""

from __future__ import annotations

import pytest

from consistency_persistence.db import make_engine
from consistency_persistence.reference import assert_no_credentials
from consistency_persistence.schema import Base, DetectionRow


def test_configuration_document_rejects_credentials() -> None:
    with pytest.raises(ValueError, match="credential"):
        assert_no_credentials({"password": "hunter2"})
    with pytest.raises(ValueError, match="credential"):
        assert_no_credentials(
            {"note": "postgresql+asyncpg://consistency:hunter2@127.0.0.1:5433/consistency"}
        )
    with pytest.raises(ValueError, match="credential"):
        assert_no_credentials({"cookie": "ce_replay_capability=abc"})
    assert_no_credentials({"fingerprint": "abc", "source": "synthetic"})


def test_schema_has_no_credential_columns() -> None:
    banned = {"password", "passwd", "secret", "api_key", "token", "authorization"}
    for table in Base.metadata.tables.values():
        for column in table.columns:
            assert column.name.lower() not in banned


def test_detection_id_is_the_primary_key() -> None:
    names = [column.name for column in DetectionRow.__table__.primary_key.columns]
    assert names == ["detection_id"]


def test_engine_checks_connections_before_checkout() -> None:
    engine = make_engine("postgresql+asyncpg://consistency:consistency@127.0.0.1:5433/consistency")
    try:
        assert engine.pool._pre_ping is True
    finally:
        engine.sync_engine.dispose()

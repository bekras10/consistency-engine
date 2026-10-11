"""The health command reads session lifecycle, not only database connectivity."""

from __future__ import annotations

import pytest
from sqlalchemy import update

from consistency_persistence.schema import IngestionSessionRow
from consistency_persistence.store import PersistenceStore
from consistency_worker.bootstrap import fee_calculator
from consistency_worker.health import report_health
from tests.conftest import FIXTURES
from tests.integration.test_postgres_pipeline import _messages

pytestmark = pytest.mark.integration


async def test_report_health_distinguishes_completed_running_and_failed(db: str) -> None:
    catalog, relationships, _unused = _messages()
    store = PersistenceStore(db)
    try:
        assert await report_health(db) == ("starting", 1)
        opened = await store.open(
            preferred_session_id="ses-health",
            fingerprint="sha256:health",
            deterministic=True,
            source_label="synthetic:other",
            source_kind="synthetic",
            catalog=catalog,
            relationships=relationships,
            fees=fee_calculator(FIXTURES / "fees"),
            persist_raw=False,
            pinned=False,
            batch_size=10,
            config_document={"test": "health"},
        )
        assert await report_health(db) == ("running", 0)
        async with store.sessions() as session, session.begin():
            await session.execute(
                update(IngestionSessionRow)
                .where(IngestionSessionRow.session_id == opened.session_id)
                .values(status="completed")
            )
        assert await report_health(db) == ("completed", 0)
        async with store.sessions() as session, session.begin():
            await session.execute(
                update(IngestionSessionRow)
                .where(IngestionSessionRow.session_id == opened.session_id)
                .values(status="failed")
            )
        assert await report_health(db) == ("failed", 1)
    finally:
        await store.aclose()

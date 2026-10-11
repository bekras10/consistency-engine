"""The latest session for a fingerprint decides. An older completed row does not hide a later failure."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update

from consistency_persistence.schema import IngestionSessionRow
from consistency_persistence.store import OpenedPersistence, PersistenceStore
from consistency_worker.bootstrap import fee_calculator
from tests.conftest import FIXTURES
from tests.integration.test_postgres_pipeline import _messages

pytestmark = pytest.mark.integration

_T0 = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)


async def _open(
    store: PersistenceStore,
    *,
    fingerprint: str,
    reseed: bool = False,
) -> OpenedPersistence:
    catalog, relationships, _unused = _messages()
    return await store.open(
        preferred_session_id="ses-precedence",
        fingerprint=fingerprint,
        deterministic=True,
        source_label="replay:unit",
        source_kind="replay",
        catalog=catalog,
        relationships=relationships,
        fees=fee_calculator(FIXTURES / "fees"),
        persist_raw=False,
        pinned=False,
        batch_size=10,
        config_document={"test": fingerprint},
        reseed=reseed,
    )


async def _stamp(
    store: PersistenceStore,
    session_id: str,
    *,
    status: str,
    started_at: datetime,
) -> None:
    async with store.sessions() as session, session.begin():
        await session.execute(
            update(IngestionSessionRow)
            .where(IngestionSessionRow.session_id == session_id)
            .values(status=status, started_at=started_at, ended_at=started_at, detail=status)
        )


async def _rows(store: PersistenceStore, fingerprint: str) -> list[IngestionSessionRow]:
    async with store.sessions() as session:
        rows = (
            await session.execute(
                select(IngestionSessionRow)
                .where(IngestionSessionRow.fingerprint == fingerprint)
                .order_by(IngestionSessionRow.started_at, IngestionSessionRow.session_id)
            )
        ).scalars()
        return list(rows)


async def test_completed_only_fingerprint_skips_ingest(db: str) -> None:
    store = PersistenceStore(db)
    fingerprint = "sha256:precedence-completed-only"
    try:
        first = await _open(store, fingerprint=fingerprint)
        await _stamp(store, first.session_id, status="completed", started_at=_T0)
        second = await _open(store, fingerprint=fingerprint)
        assert second.skip_ingest is True
        assert second.session_id == first.session_id
        rows = await _rows(store, fingerprint)
        assert [row.session_id for row in rows] == [first.session_id]
        assert rows[0].status == "completed"
    finally:
        await store.aclose()


async def test_interrupted_only_fingerprint_resumes(db: str) -> None:
    store = PersistenceStore(db)
    fingerprint = "sha256:precedence-interrupted-only"
    try:
        first = await _open(store, fingerprint=fingerprint)
        await _stamp(store, first.session_id, status="interrupted", started_at=_T0)
        second = await _open(store, fingerprint=fingerprint)
        assert second.skip_ingest is False
        assert second.session_id == first.session_id
        rows = await _rows(store, fingerprint)
        assert [row.session_id for row in rows] == [first.session_id]
        assert rows[0].status == "running"
    finally:
        await store.aclose()


async def test_completed_then_failed_reseed_resumes_without_a_third_id(db: str) -> None:
    store = PersistenceStore(db)
    fingerprint = "sha256:precedence-completed-then-failed"
    try:
        first = await _open(store, fingerprint=fingerprint)
        await _stamp(store, first.session_id, status="completed", started_at=_T0)
        second = await _open(store, fingerprint=fingerprint, reseed=True)
        assert second.session_id != first.session_id
        assert second.skip_ingest is False
        await _stamp(
            store,
            second.session_id,
            status="failed",
            started_at=_T0 + timedelta(seconds=30),
        )
        third = await _open(store, fingerprint=fingerprint)
        assert third.skip_ingest is False
        assert third.session_id == second.session_id
        rows = await _rows(store, fingerprint)
        assert [row.session_id for row in rows] == [first.session_id, second.session_id]
        by_id = {row.session_id: row.status for row in rows}
        assert by_id[first.session_id] == "completed"
        assert by_id[second.session_id] == "running"
    finally:
        await store.aclose()


async def test_explicit_reseed_when_the_latest_is_completed_opens_a_new_session(db: str) -> None:
    store = PersistenceStore(db)
    fingerprint = "sha256:precedence-reseed-latest-completed"
    try:
        older = await _open(store, fingerprint=fingerprint)
        await _stamp(store, older.session_id, status="interrupted", started_at=_T0)
        completed = await _open(store, fingerprint=fingerprint, reseed=True)
        await _stamp(
            store,
            completed.session_id,
            status="completed",
            started_at=_T0 + timedelta(seconds=30),
        )
        opened = await _open(store, fingerprint=fingerprint, reseed=True)
        assert opened.skip_ingest is False
        assert opened.session_id not in {older.session_id, completed.session_id}
        rows = await _rows(store, fingerprint)
        assert len(rows) == 3
        assert rows[-1].session_id == opened.session_id
        assert rows[-1].status == "running"
    finally:
        await store.aclose()


async def test_later_completed_session_is_not_replaced_by_an_older_interrupted_one(
    db: str,
) -> None:
    """An older resumable row must not win over a later completed session."""
    store = PersistenceStore(db)
    fingerprint = "sha256:precedence-later-completed"
    try:
        older = await _open(store, fingerprint=fingerprint)
        await _stamp(store, older.session_id, status="interrupted", started_at=_T0)
        later = await _open(store, fingerprint=fingerprint, reseed=True)
        await _stamp(
            store,
            later.session_id,
            status="completed",
            started_at=_T0 + timedelta(seconds=30),
        )
        opened = await _open(store, fingerprint=fingerprint)
        assert opened.skip_ingest is True
        assert opened.session_id == later.session_id
        rows = await _rows(store, fingerprint)
        assert [row.session_id for row in rows] == [older.session_id, later.session_id]
        by_id = {row.session_id: row.status for row in rows}
        assert by_id[older.session_id] == "interrupted"
        assert by_id[later.session_id] == "completed"
    finally:
        await store.aclose()

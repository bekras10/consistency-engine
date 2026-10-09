"""Session lifecycle on top of the shared repository.

Deterministic sessions left ``interrupted`` (or still ``running`` after a crash) resume from
the latest checkpoint: the journal is truncated after that ordinal, and detections that were
written past it are removed so replay cannot duplicate or orphan them. Live sessions are never
resumed; a ``running`` live session is closed with its open detections invalidated.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from consistency_core.fees import FeeCalculator
from consistency_core.models.market import Catalog
from consistency_core.models.relationship import Relationship
from consistency_core.serialization import sha256_of
from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.recording import (
    BatchJournal,
    CheckpointWriter,
    DetectionWriter,
    VersionStamp,
    latest_checkpoint,
    replace_certificate_projection,
)
from consistency_persistence.reference import (
    upsert_catalog,
    upsert_configuration,
    upsert_data_source,
    upsert_fee_schedules,
    upsert_relationships,
)
from consistency_persistence.retention import (
    RetentionPolicy,
    SessionRetentionView,
    delete_raw_sessions,
    sessions_losing_raw_data,
)
from consistency_persistence.schema import (
    DataSourceRow,
    DetectionLegRow,
    DetectionRow,
    DetectionScenarioRow,
    IngestionSessionRow,
    OrderbookSnapshotRow,
    OrderbookUpdateRow,
    ReplaySessionRow,
    SessionCheckpointRow,
    SystemHealthRow,
)
from consistency_persistence.timeutil import ms_to_dt, now_utc
from consistency_pipeline.checkpoint import Checkpoint
from consistency_pipeline.lifecycle import DetectionRecord

log = logging.getLogger("consistency.persistence")


@dataclass
class OpenedPersistence:
    session_id: str
    journal: BatchJournal
    checkpoints: CheckpointWriter | None
    sink: DetectionWriter
    resume_from: Checkpoint | None


class PersistenceStore:
    def __init__(self, url: str) -> None:
        self.url = url
        self.engine = make_engine(url)
        self.sessions = make_sessionmaker(self.engine)

    async def aclose(self) -> None:
        await self.engine.dispose()

    async def ping(self) -> None:
        async with self.engine.connect() as conn:
            await conn.exec_driver_sql("SELECT 1")

    async def open(
        self,
        *,
        preferred_session_id: str,
        fingerprint: str,
        deterministic: bool,
        source_label: str,
        source_kind: str,
        catalog: Catalog,
        relationships: list[Relationship],
        fees: FeeCalculator,
        persist_raw: bool,
        pinned: bool,
        batch_size: int,
        config_document: dict[str, Any],
    ) -> OpenedPersistence:
        versions = _versions(catalog, relationships, fees)
        config_id = sha256_of(config_document)
        async with self.sessions() as session, session.begin():
            await upsert_data_source(
                session, data_source_id=source_kind, name=source_label, kind=source_kind
            )
            await upsert_catalog(session, catalog, source_kind)
            await upsert_relationships(session, relationships)
            await upsert_fee_schedules(session, list(fees.registry.schedules))
            await upsert_configuration(
                session, version_id=config_id, kind="session-config", document=config_document
            )
            session_id, resume = await _allocate(
                session,
                preferred_session_id=preferred_session_id,
                fingerprint=fingerprint,
                deterministic=deterministic,
                source_label=source_label,
                source_kind=source_kind,
                pinned=pinned,
                persist_raw=persist_raw,
                config_version_id=config_id,
            )
            session.add(
                SystemHealthRow(
                    component="ingestion",
                    status="starting",
                    checked_at=now_utc(),
                    detail={"session_id": session_id, "resumed": resume is not None},
                )
            )
        journal = BatchJournal(
            self.sessions,
            session_id,
            versions,
            batch_size=batch_size,
            enabled=persist_raw,
        )
        checkpoints = CheckpointWriter(self.sessions) if persist_raw else None
        return OpenedPersistence(
            session_id=session_id,
            journal=journal,
            checkpoints=checkpoints,
            sink=DetectionWriter(self.sessions),
            resume_from=resume,
        )

    async def close_session(self, session_id: str, *, status: str, detail: str | None) -> None:
        async with self.sessions() as session, session.begin():
            await session.execute(
                update(IngestionSessionRow)
                .where(IngestionSessionRow.session_id == session_id)
                .values(status=status, ended_at=now_utc(), detail=detail)
            )
            session.add(
                SystemHealthRow(
                    component="ingestion",
                    status=status,
                    checked_at=now_utc(),
                    detail={"session_id": session_id, "detail": detail},
                )
            )

    async def invalidate_open_detections(self, session_id: str, reason: str) -> int:
        async with self.sessions() as session, session.begin():
            result = await session.execute(
                update(DetectionRow)
                .where(
                    DetectionRow.session_id == session_id,
                    DetectionRow.status.in_(("OPEN", "UPDATED")),
                )
                .values(
                    status="INVALIDATED",
                    close_reason=reason,
                    close_detail=reason,
                    closed_at=now_utc(),
                )
            )
        return int(getattr(result, "rowcount", 0) or 0)

    async def run_retention(self, policy: RetentionPolicy) -> list[str]:
        async with self.sessions() as session, session.begin():
            rows = (
                await session.execute(
                    select(IngestionSessionRow, DataSourceRow.kind).join(
                        DataSourceRow,
                        DataSourceRow.data_source_id == IngestionSessionRow.data_source_id,
                    )
                )
            ).all()
            views = [
                SessionRetentionView(
                    session_id=row.session_id,
                    started_at=row.started_at,
                    source_kind=kind,
                    pinned=row.pinned,
                    raw_persisted=row.raw_persisted,
                )
                for row, kind in rows
            ]
            drop = sessions_losing_raw_data(views, now=now_utc(), policy=policy)
            await delete_raw_sessions(session, drop)
            if drop:
                await session.execute(
                    update(IngestionSessionRow)
                    .where(IngestionSessionRow.session_id.in_(drop))
                    .values(raw_persisted=False)
                )
            session.add(
                SystemHealthRow(
                    component="retention",
                    status="ok",
                    checked_at=now_utc(),
                    detail={"sessions_stripped": drop},
                )
            )
        if drop:
            log.info("retention stripped raw data", extra={"sessions": drop})
        return drop

    async def retention_loop(self, interval_s: int, policy: RetentionPolicy) -> None:
        if interval_s <= 0:
            return
        while True:
            await asyncio.sleep(interval_s)
            try:
                await self.run_retention(policy)
            except Exception:
                log.exception("retention job failed")

    async def save_replay_session(
        self,
        *,
        replay_id: str,
        source_session_id: str | None,
        status: str,
        speed: str,
        position_ordinal: int,
        position_ms: int | None,
    ) -> None:
        from decimal import Decimal

        now = now_utc()
        async with self.sessions() as session, session.begin():
            existing = await session.get(ReplaySessionRow, replay_id)
            if existing is None:
                session.add(
                    ReplaySessionRow(
                        replay_id=replay_id,
                        source_session_id=source_session_id,
                        status=status,
                        speed=Decimal(speed),
                        position_ordinal=position_ordinal,
                        position_ms=position_ms,
                        created_at=now,
                        updated_at=now,
                    )
                )
            else:
                existing.status = status
                existing.speed = Decimal(speed)
                existing.position_ordinal = position_ordinal
                existing.position_ms = position_ms
                existing.updated_at = now

    async def list_detection_rows(self, session_id: str) -> list[DetectionRow]:
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(DetectionRow)
                    .where(DetectionRow.session_id == session_id)
                    .order_by(DetectionRow.detection_id)
                )
            ).scalars()
            return list(rows)


def _versions(
    catalog: Catalog, relationships: list[Relationship], fees: FeeCalculator
) -> VersionStamp:
    return VersionStamp(
        fee_schedule_version=sha256_of(fees.schedule_versions()),
        relationship_version=sha256_of([r.model_dump(mode="json") for r in relationships]),
        rules_hashes={m.market_id: m.rules_hash for m in catalog.markets},
    )


async def _allocate(
    session: AsyncSession,
    *,
    preferred_session_id: str,
    fingerprint: str,
    deterministic: bool,
    source_label: str,
    source_kind: str,
    pinned: bool,
    persist_raw: bool,
    config_version_id: str,
) -> tuple[str, Checkpoint | None]:
    existing = await session.get(IngestionSessionRow, preferred_session_id)
    if existing is not None and existing.fingerprint == fingerprint and deterministic:
        if existing.status in ("interrupted", "running"):
            resume = await latest_checkpoint(session, existing.session_id)
            if resume is None:
                await _wipe_runtime(session, existing.session_id)
            else:
                await _truncate_after(session, existing.session_id, resume.ordinal)
                await _reconcile(session, existing.session_id, resume)
            existing.status = "running"
            existing.ended_at = None
            existing.detail = None
            existing.raw_persisted = persist_raw
            existing.pinned = pinned
            return existing.session_id, resume
        if existing.status in ("interrupted", "running"):
            await session.execute(
                update(DetectionRow)
                .where(
                    DetectionRow.session_id == existing.session_id,
                    DetectionRow.status.in_(("OPEN", "UPDATED")),
                )
                .values(
                    status="INVALIDATED",
                    close_reason="SERVICE_RESTART",
                    close_detail="SERVICE_RESTART",
                    closed_at=now_utc(),
                )
            )
            existing.status = "failed"
            existing.detail = "SERVICE_RESTART"
            existing.ended_at = now_utc()
    session_id = preferred_session_id
    suffix = 2
    while await session.get(IngestionSessionRow, session_id) is not None:
        session_id = f"{preferred_session_id}-{suffix}"
        suffix += 1
    session.add(
        IngestionSessionRow(
            session_id=session_id,
            data_source_id=source_kind,
            status="running",
            fingerprint=fingerprint,
            source_label=source_label,
            deterministic=deterministic,
            pinned=pinned,
            raw_persisted=persist_raw,
            started_at=now_utc(),
            config_version_id=config_version_id,
        )
    )
    return session_id, None


async def _wipe_runtime(session: AsyncSession, session_id: str) -> None:
    await session.execute(delete(DetectionRow).where(DetectionRow.session_id == session_id))
    await _truncate_after(session, session_id, -1)


async def _truncate_after(session: AsyncSession, session_id: str, ordinal: int) -> None:
    for model in (OrderbookUpdateRow, OrderbookSnapshotRow, SessionCheckpointRow):
        await session.execute(
            delete(model).where(model.session_id == session_id, model.ordinal > ordinal)
        )


async def _reconcile(session: AsyncSession, session_id: str, checkpoint: Checkpoint) -> None:
    records = _records(checkpoint)
    ids = [r.detection_id for r in records]
    stmt = delete(DetectionRow).where(DetectionRow.session_id == session_id)
    if ids:
        stmt = stmt.where(DetectionRow.detection_id.not_in(ids))
    await session.execute(stmt)
    for rec in records:
        row = await session.get(DetectionRow, rec.detection_id)
        if row is None:
            session.add(_row_from_record(rec))
            if rec.certificate_json is not None:
                await session.flush()
                await replace_certificate_projection(
                    session, rec.detection_id, rec.certificate_json
                )
            continue
        hash_differs = row.certificate_hash != rec.certificate_hash
        if rec.certificate_json is not None:
            stale = hash_differs or row.certificate_json != rec.certificate_json
            row.certificate_json = rec.certificate_json
            row.certificate_version = rec.certificate_version
            if stale:
                await replace_certificate_projection(
                    session, rec.detection_id, rec.certificate_json
                )
        elif hash_differs:
            row.certificate_json = None
            row.certificate_version = None
            await session.execute(
                delete(DetectionLegRow).where(DetectionLegRow.detection_id == rec.detection_id)
            )
            await session.execute(
                delete(DetectionScenarioRow).where(
                    DetectionScenarioRow.detection_id == rec.detection_id
                )
            )
        row.status = rec.status.value
        row.classification = rec.classification.value
        row.peak_classification = rec.peak_classification.value
        row.reason_codes = list(rec.reason_codes)
        row.last_observed_at = ms_to_dt(rec.last_observed_ms)
        row.last_observed_ms = rec.last_observed_ms
        row.last_position = rec.last_position
        row.closed_at = None if rec.closed_at_ms is None else ms_to_dt(rec.closed_at_ms)
        row.close_reason = None if rec.close_reason is None else rec.close_reason.value
        row.close_detail = rec.close_detail
        row.max_deviation = rec.max_deviation
        row.max_capacity = rec.max_capacity
        row.max_net_edge = rec.max_net_edge
        row.event_count = rec.event_count
        row.certificate_hash = rec.certificate_hash
        row.record_json = rec.model_dump(mode="json")


def _records(checkpoint: Checkpoint) -> list[DetectionRecord]:
    slots = checkpoint.engine_state.get("slots")
    if not isinstance(slots, dict):
        return []
    out: list[DetectionRecord] = []
    for slot in slots.values():
        if not isinstance(slot, dict):
            continue
        detection = slot.get("detection")
        if isinstance(detection, dict):
            out.append(DetectionRecord.model_validate(detection))
    return out


def _row_from_record(rec: DetectionRecord) -> DetectionRow:
    return DetectionRow(
        detection_id=rec.detection_id,
        session_id=rec.session_id,
        relationship_id=rec.relationship_id,
        strategy_id=rec.strategy_id,
        template=rec.template,
        ordinal=rec.ordinal,
        status=rec.status.value,
        classification=rec.classification.value,
        peak_classification=rec.peak_classification.value,
        reason_codes=list(rec.reason_codes),
        first_observed_at=ms_to_dt(rec.first_observed_ms),
        last_observed_at=ms_to_dt(rec.last_observed_ms),
        first_observed_ms=rec.first_observed_ms,
        last_observed_ms=rec.last_observed_ms,
        first_position=rec.first_position,
        last_position=rec.last_position,
        closed_at=None if rec.closed_at_ms is None else ms_to_dt(rec.closed_at_ms),
        close_reason=None if rec.close_reason is None else rec.close_reason.value,
        close_detail=rec.close_detail,
        max_deviation=rec.max_deviation,
        max_capacity=rec.max_capacity,
        max_net_edge=rec.max_net_edge,
        event_count=rec.event_count,
        certificate_hash=rec.certificate_hash,
        certificate_json=rec.certificate_json,
        certificate_version=rec.certificate_version,
        record_json=rec.model_dump(mode="json"),
    )

"""Adapter from the worker's session protocol to the shared persistence repository."""

from __future__ import annotations

import os
import time
from collections.abc import Mapping, Sequence
from decimal import Decimal

from consistency_connectors.settings import DataSourceMode, Settings
from consistency_core.serialization import sha256_of
from consistency_persistence.retention import RetentionPolicy
from consistency_persistence.store import OpenedPersistence, PersistenceStore
from consistency_persistence.study_ledger import StudyLedger, episode_identity
from consistency_worker.bootstrap import SessionInputs
from consistency_worker.service import OpenedSession, SessionStatus, default_session_id

_DEMO_LABELS = frozenset({"synthetic:inconsistent", "replay:inconsistent"})


class DatabaseSessionStore:
    def __init__(self, settings: Settings) -> None:
        if not settings.database_url:
            raise ValueError("DATABASE_URL is required")
        self._settings = settings
        self._inner = PersistenceStore(settings.database_url)
        self._ledger = StudyLedger(self._inner.sessions)
        self._study_id = settings.study_id
        self._policy = RetentionPolicy(
            max_age_hours=settings.retention_max_age_hours,
            max_sessions=settings.retention_max_sessions,
            third_party_hours=settings.retention_third_party_hours,
            outbox_keep=settings.outbox_retention_keep,
        )

    async def ping(self) -> None:
        await self._inner.ping()

    async def aclose(self) -> None:
        await self._inner.aclose()

    @property
    def retention_interval_s(self) -> int:
        return self._settings.retention_interval_s

    async def retention_loop(self) -> None:
        await self._inner.retention_loop(self._settings.retention_interval_s, self._policy)

    async def run_retention(self) -> list[str]:
        return await self._inner.run_retention(self._policy)

    async def open_session(self, inputs: SessionInputs, settings: Settings) -> OpenedSession:
        opened: OpenedPersistence = await self._inner.open(
            preferred_session_id=default_session_id(inputs),
            fingerprint=inputs.fingerprint,
            deterministic=inputs.deterministic,
            source_label=inputs.source_label,
            source_kind=inputs.source.kind.value,
            catalog=inputs.catalog,
            relationships=inputs.relationships,
            fees=inputs.fees,
            persist_raw=settings.raw_market_data_persisted,
            pinned=inputs.source_label in _DEMO_LABELS,
            batch_size=settings.journal_batch_size,
            reseed=settings.reseed_session,
            config_document={
                "fingerprint": inputs.fingerprint,
                "source": inputs.source_label,
                "fee_versions": inputs.fees.schedule_versions(),
                "relationships": sha256_of(
                    [r.model_dump(mode="json") for r in inputs.relationships]
                ),
                "raw_persisted": settings.raw_market_data_persisted,
            },
        )
        if settings.study_id and settings.data_source is DataSourceMode.KALSHI_AUTHORIZED:
            await self._ledger.ensure_study(
                study_id=settings.study_id,
                source_kind=inputs.source.kind.value,
                environment=settings.kalshi_environment,
                commit_sha=os.environ.get("GIT_COMMIT", "unknown"),
                config_fingerprint=inputs.fingerprint,
                planned_start=settings.study_start,
                planned_end=settings.study_end,
                raw_retention_authorized=settings.third_party_raw_persistence_authorized,
            )
            await self._ledger.open_session(
                study_id=settings.study_id,
                session_id=opened.session_id,
                source_kind=inputs.source.kind.value,
                eligible=True,
                started_ms=time.time_ns() // 1_000_000,
            )
        return OpenedSession(
            session_id=opened.session_id,
            journal=opened.journal,
            checkpoints=opened.checkpoints,
            sink=opened.sink,
            resume_from=opened.resume_from,
            skip_ingest=opened.skip_ingest,
        )

    async def close_session(
        self, session_id: str, *, status: SessionStatus, detail: str | None
    ) -> None:
        await self._inner.close_session(session_id, status=status, detail=detail)
        if self._study_id:
            await self._ledger.close_session(
                study_id=self._study_id,
                session_id=session_id,
                ended_ms=time.time_ns() // 1_000_000,
            )

    async def record_study_events(self, events: Sequence[Mapping[str, object]]) -> None:
        if not self._study_id:
            return
        await self._ledger.record_events(events)
        for event in events:
            if event.get("kind") != "membership":
                continue
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                continue
            action = str(payload.get("action", ""))
            subject_kind = str(payload.get("subject_kind", ""))
            subject_id = str(event.get("subject_id", ""))
            occurred = event.get("occurred_ms")
            if not isinstance(occurred, int):
                continue
            if action == "enter":
                await self._ledger.enter_membership(
                    study_id=self._study_id,
                    session_id=str(event.get("session_id", "")),
                    subject_kind=subject_kind,
                    subject_id=subject_id,
                    entered_ms=occurred,
                    fingerprint=str(payload.get("fingerprint", "")),
                    verification_status=str(event.get("classification", "")),
                )
            elif action == "exit":
                await self._ledger.exit_membership(
                    study_id=self._study_id,
                    session_id=str(event.get("session_id", "")),
                    subject_kind=subject_kind,
                    subject_id=subject_id,
                    exited_ms=occurred,
                )

    async def record_study_episodes(self, episodes: Sequence[Mapping[str, object]]) -> None:
        if not self._study_id or not episodes:
            return
        stamped: list[dict[str, object]] = []
        for item in episodes:
            detection_id = str(item.get("detection_id", ""))
            body = dict(item)
            body["episode_id"] = episode_identity(self._study_id, detection_id)
            body["study_id"] = self._study_id
            stamped.append(body)
        await self._ledger.upsert_episodes(stamped)

    async def save_replay(
        self,
        *,
        replay_id: str,
        source_session_id: str | None,
        status: str,
        speed: Decimal,
        position_ordinal: int,
        position_ms: int | None,
    ) -> None:
        await self._inner.save_replay_session(
            replay_id=replay_id,
            source_session_id=source_session_id,
            status=status,
            speed=format(speed, "f"),
            position_ordinal=position_ordinal,
            position_ms=position_ms,
        )

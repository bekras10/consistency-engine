"""Adapter from the worker's session protocol to the shared persistence repository."""

from __future__ import annotations

from decimal import Decimal

from consistency_connectors.settings import Settings
from consistency_core.serialization import sha256_of
from consistency_persistence.retention import RetentionPolicy
from consistency_persistence.store import OpenedPersistence, PersistenceStore
from consistency_worker.bootstrap import SessionInputs
from consistency_worker.service import OpenedSession, SessionStatus, default_session_id

_DEMO_LABELS = frozenset({"synthetic:inconsistent", "replay:inconsistent"})


class DatabaseSessionStore:
    def __init__(self, settings: Settings) -> None:
        if not settings.database_url:
            raise ValueError("DATABASE_URL is required")
        self._settings = settings
        self._inner = PersistenceStore(settings.database_url)
        self._policy = RetentionPolicy(
            max_age_hours=settings.retention_max_age_hours,
            max_sessions=settings.retention_max_sessions,
            third_party_hours=settings.retention_third_party_hours,
        )

    async def ping(self) -> None:
        await self._inner.ping()

    async def aclose(self) -> None:
        await self._inner.aclose()

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
        return OpenedSession(
            session_id=opened.session_id,
            journal=opened.journal,
            checkpoints=opened.checkpoints,
            sink=opened.sink,
            resume_from=opened.resume_from,
        )

    async def close_session(
        self, session_id: str, *, status: SessionStatus, detail: str | None
    ) -> None:
        await self._inner.close_session(session_id, status=status, detail=detail)

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

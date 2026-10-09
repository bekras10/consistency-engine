"""In-process replay sessions for the dashboard.

The gateway keeps one :class:`PlaybackService`. HTTP handlers call it directly.
This is not the Phase 11 ``POST /api/v1/replay`` catalog.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from consistency_connectors.ingestion import BookManager
from consistency_core.fees import FeeCalculator, FeeSchedule, FeeScheduleRegistry
from consistency_core.models.market import Market
from consistency_core.models.relationship import Relationship
from consistency_core.money import dec_str
from consistency_persistence.dashboard import apply_book, book_view, current_figures, sync_summary
from consistency_persistence.recording import latest_checkpoint
from consistency_persistence.schema import (
    FeeScheduleRow,
    IngestionSessionRow,
    MarketRow,
    OrderbookUpdateRow,
    RelationshipRow,
    SessionCheckpointRow,
)
from consistency_pipeline.checkpoint import Checkpoint
from consistency_pipeline.journal import JournalEntry
from consistency_pipeline.playback import PlaybackService, PlaybackSession


class BookTail:
    """Latest session's books, advanced from the last checkpoint plus new ordinals."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.session_id: str | None = None
        self.manager: BookManager | None = None
        self.ordinal = -1
        self.source_label: str | None = None

    async def refresh(self, sessions: async_sessionmaker[AsyncSession]) -> BookManager | None:
        async with self._lock:
            async with sessions() as session:
                latest = (
                    await session.execute(
                        select(IngestionSessionRow)
                        .order_by(IngestionSessionRow.started_at.desc())
                        .limit(1)
                    )
                ).scalar_one_or_none()
                if latest is None or not latest.raw_persisted:
                    self.session_id = None
                    self.manager = None
                    self.ordinal = -1
                    return None
                if self.session_id != latest.session_id or self.manager is None:
                    checkpoint = await latest_checkpoint(session, latest.session_id)
                    if checkpoint is None:
                        markets = [
                            Market.model_validate(row.document)
                            for row in (await session.execute(select(MarketRow))).scalars()
                        ]
                        self.manager = BookManager(markets, source=latest.source_label)
                        self.ordinal = -1
                    else:
                        state = cast(dict[str, Any], dict(checkpoint.manager_state))
                        self.manager = BookManager.from_state(state)
                        self.ordinal = checkpoint.ordinal
                    self.session_id = latest.session_id
                    self.source_label = latest.source_label
                entries = await _entries_after(session, latest.session_id, self.ordinal)
            assert self.manager is not None
            for entry in entries:
                apply_book(self.manager, entry)
                self.ordinal = entry.ordinal
            return self.manager

    def summary(self) -> dict[str, object] | None:
        if self.manager is None:
            return None
        body = sync_summary(self.manager)
        body["session_id"] = self.session_id
        body["ordinal"] = self.ordinal
        body["source_label"] = self.source_label
        return body


async def _entries_after(
    session: AsyncSession, session_id: str, ordinal: int
) -> list[JournalEntry]:
    rows = (
        await session.execute(
            select(OrderbookUpdateRow.entry_json)
            .where(
                OrderbookUpdateRow.session_id == session_id,
                OrderbookUpdateRow.ordinal > ordinal,
            )
            .order_by(OrderbookUpdateRow.ordinal)
        )
    ).scalars()
    return [JournalEntry.model_validate(payload) for payload in rows]


async def _load_playback(session: AsyncSession, replay_id: str) -> PlaybackSession:
    ingestion = await session.get(IngestionSessionRow, replay_id)
    if ingestion is None:
        raise KeyError(replay_id)
    if not ingestion.raw_persisted:
        raise ValueError("session has no stored journal")
    markets = [
        Market.model_validate(row.document)
        for row in (await session.execute(select(MarketRow))).scalars()
    ]
    relationships = [
        Relationship.model_validate(row.document)
        for row in (await session.execute(select(RelationshipRow))).scalars()
    ]
    schedules = [
        FeeSchedule.model_validate(row.document)
        for row in (await session.execute(select(FeeScheduleRow))).scalars()
    ]
    entries = await _entries_after(session, replay_id, -1)
    checkpoints = [
        Checkpoint.model_validate(payload)
        for payload in (
            await session.execute(
                select(SessionCheckpointRow.checkpoint_json)
                .where(SessionCheckpointRow.session_id == replay_id)
                .order_by(SessionCheckpointRow.ordinal)
            )
        ).scalars()
    ]
    return PlaybackSession(
        replay_id,
        entries,
        markets,
        relationships,
        FeeCalculator(FeeScheduleRegistry(schedules)),
        session_id=replay_id,
        checkpoints=checkpoints,
    )


def _timeline(session: PlaybackSession) -> list[dict[str, object]]:
    entries = session.entries
    if not entries:
        return []
    step = max(1, len(entries) // 120)
    chosen = list(entries[::step])
    if session.cursor > 0:
        current = entries[session.cursor - 1]
        if all(item.ordinal != current.ordinal for item in chosen):
            chosen.append(current)
    chosen.sort(key=lambda item: item.ordinal)
    return [{"ordinal": item.ordinal, "now_ms": item.now_ms, "kind": item.kind} for item in chosen]


def snapshot(session: PlaybackSession) -> dict[str, Any]:
    manager = session.outcome.manager
    books = [book_view(manager, market_id) for market_id in manager.market_ids]
    detections: list[dict[str, object]] = []
    for record in session.outcome.engine.active_detections():
        dumped = record.model_dump(mode="json")
        figures = current_figures(dumped, record.max_net_edge)
        detections.append(
            {
                "detection_id": record.detection_id,
                "relationship_id": record.relationship_id,
                "status": record.status.value,
                "classification": record.classification.value,
                "net_edge": figures["net_edge"],
                "max_net_edge": figures["max_net_edge"],
                "theoretical_deviation": figures["theoretical_deviation"],
            }
        )
    events = [
        {
            "detection_id": event.detection_id,
            "kind": event.kind.value,
            "at_ms": event.at_ms,
            "position": event.position,
            "classification": None if event.classification is None else event.classification.value,
        }
        for event in session.outcome.events[-40:]
    ]
    first_ms = None if not session.entries else session.entries[0].now_ms
    last_ms = None if not session.entries else session.entries[-1].now_ms
    return {
        "replay_id": session.replay_id,
        "status": session.status,
        "speed": dec_str(session.speed),
        "cursor": session.cursor,
        "entry_count": len(session.entries),
        "position_ms": session.position_ms,
        "first_ms": first_ms,
        "last_ms": last_ms,
        "books": books,
        "detections": detections,
        "events": events,
        "timeline": _timeline(session),
        "sync": sync_summary(manager),
        "transport": "polling",
    }


class ReplayHost:
    def __init__(self) -> None:
        self.service = PlaybackService()
        self._lock = asyncio.Lock()
        self._background: set[asyncio.Task[None]] = set()

    def _finished(self, task: asyncio.Task[None]) -> None:
        self._background.discard(task)
        if not task.cancelled():
            task.exception()

    async def ensure(
        self, sessions: async_sessionmaker[AsyncSession], replay_id: str
    ) -> PlaybackSession:
        async with self._lock:
            try:
                return self.service.get(replay_id)
            except KeyError:
                pass
            async with sessions() as session:
                loaded = await _load_playback(session, replay_id)
            return self.service.open(loaded)

    async def command(
        self,
        sessions: async_sessionmaker[AsyncSession],
        replay_id: str,
        action: str,
        body: dict[str, object] | None = None,
    ) -> dict[str, Any]:
        playback = await self.ensure(sessions, replay_id)
        payload = body or {}
        if action == "start":
            self.service.start(replay_id)
        elif action == "pause":
            self.service.pause(replay_id)
        elif action == "resume":
            # PlaybackService.resume awaits the recording. Schedule it so the
            # button returns while the existing session task keeps the clock.
            task = asyncio.create_task(self.service.resume(replay_id))
            self._background.add(task)
            task.add_done_callback(self._finished)
        elif action == "restart":
            self.service.restart(replay_id)
        elif action == "step":
            self.service.step(replay_id)
        elif action == "seek":
            raw = payload.get("timestamp_ms")
            if isinstance(raw, bool) or not isinstance(raw, int):
                raise ValueError("timestamp_ms must be an integer")
            self.service.seek(replay_id, raw)
        elif action == "speed":
            raw_speed = payload.get("speed")
            if not isinstance(raw_speed, str):
                raise ValueError("speed must be a fixed-point string")
            self.service.set_speed(replay_id, Decimal(raw_speed))
        else:
            raise ValueError(f"unknown replay action {action}")
        return snapshot(playback)

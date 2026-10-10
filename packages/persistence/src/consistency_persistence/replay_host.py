"""In-process replay sessions for the dashboard and the public API.

Every mutation forks a viewer when the id is a recording, so two dashboard tabs
and two API clients each get a cursor. Neither path writes the canonical journal.

Defaults (override with the constructor, or with ``REPLAY_SESSION_TTL_S`` and
``REPLAY_MAX_VIEWERS`` when the arguments are omitted):

- session TTL: 1800 seconds of idle time since the last read or command
- maximum active viewers: 32

A sweep drops idle viewers and cancels their playback tasks. A viewer touched
inside the TTL stays, including one whose task is still playing.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections.abc import Callable
from decimal import Decimal
from typing import Any, cast

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from consistency_connectors.ingestion import BookManager
from consistency_core.fees import FeeCalculator, FeeSchedule, FeeScheduleRegistry
from consistency_core.models.market import Market
from consistency_core.models.relationship import Relationship
from consistency_core.money import dec_str
from consistency_core.serialization import canonical_json
from consistency_persistence.dashboard import apply_book, book_view, current_figures, sync_summary
from consistency_persistence.schema import (
    ConfigurationVersionRow,
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


def _entry_token(entry: JournalEntry) -> str:
    return canonical_json(entry.model_dump(mode="json"))


class BookTail:
    """Latest session's books, advanced from the last checkpoint plus new ordinals.

    A deterministic restart truncates the journal and replays it under the same
    session id. The cache treats that as a rollback when the applied ordinal
    disappears or the entry stored there no longer matches, then rebuilds from
    the latest checkpoint that still agrees with the journal.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.session_id: str | None = None
        self.manager: BookManager | None = None
        self.ordinal = -1
        self.source_label: str | None = None
        self._entry_token: str | None = None

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
                    self._clear()
                    return None
                rebuild = self.session_id != latest.session_id or self.manager is None
                ceiling: int | None = None
                if not rebuild and self.ordinal >= 0:
                    current = await _entry_at(session, latest.session_id, self.ordinal)
                    if current is None:
                        rebuild = True
                        ceiling = await _max_ordinal(session, latest.session_id)
                    elif _entry_token(current) != self._entry_token:
                        rebuild = True
                        ceiling = self.ordinal - 1
                if rebuild:
                    await self._install(session, latest, ceiling)
                entries = await _entries_after(session, latest.session_id, self.ordinal)
            assert self.manager is not None
            for entry in entries:
                apply_book(self.manager, entry)
                self.ordinal = entry.ordinal
                self._entry_token = _entry_token(entry)
            return self.manager

    def _clear(self) -> None:
        self.session_id = None
        self.manager = None
        self.ordinal = -1
        self.source_label = None
        self._entry_token = None

    async def _install(
        self, session: AsyncSession, latest: IngestionSessionRow, ceiling: int | None
    ) -> None:
        max_ordinal = await _max_ordinal(session, latest.session_id)
        limit = max_ordinal if ceiling is None else min(ceiling, max_ordinal)
        checkpoint = await _checkpoint_at_most(session, latest.session_id, limit)
        while checkpoint is not None and checkpoint.ordinal >= 0:
            anchored = await _entry_at(session, latest.session_id, checkpoint.ordinal)
            if anchored is not None:
                break
            checkpoint = await _checkpoint_at_most(
                session, latest.session_id, checkpoint.ordinal - 1
            )
        if checkpoint is None:
            markets = [
                Market.model_validate(row.document)
                for row in (await session.execute(select(MarketRow))).scalars()
            ]
            self.manager = BookManager(markets, source=latest.source_label)
            self.ordinal = -1
            self._entry_token = None
        else:
            state = cast(dict[str, Any], dict(checkpoint.manager_state))
            self.manager = BookManager.from_state(state)
            self.ordinal = checkpoint.ordinal
            anchored = await _entry_at(session, latest.session_id, checkpoint.ordinal)
            self._entry_token = None if anchored is None else _entry_token(anchored)
        self.session_id = latest.session_id
        self.source_label = latest.source_label

    def summary(self) -> dict[str, object] | None:
        if self.manager is None:
            return None
        body = sync_summary(self.manager)
        body["session_id"] = self.session_id
        body["ordinal"] = self.ordinal
        body["source_label"] = self.source_label
        return body


async def _max_ordinal(session: AsyncSession, session_id: str) -> int:
    row = (
        await session.execute(
            select(func.max(OrderbookUpdateRow.ordinal)).where(
                OrderbookUpdateRow.session_id == session_id
            )
        )
    ).one()
    raw = cast(object, row[0])
    if not isinstance(raw, int):
        return -1
    return raw


async def _entry_at(session: AsyncSession, session_id: str, ordinal: int) -> JournalEntry | None:
    payload = (
        await session.execute(
            select(OrderbookUpdateRow.entry_json).where(
                OrderbookUpdateRow.session_id == session_id,
                OrderbookUpdateRow.ordinal == ordinal,
            )
        )
    ).scalar_one_or_none()
    if payload is None:
        return None
    return JournalEntry.model_validate(payload)


async def _checkpoint_at_most(
    session: AsyncSession, session_id: str, ordinal: int
) -> Checkpoint | None:
    if ordinal < 0:
        return None
    payload = (
        await session.execute(
            select(SessionCheckpointRow.checkpoint_json)
            .where(
                SessionCheckpointRow.session_id == session_id,
                SessionCheckpointRow.ordinal <= ordinal,
            )
            .order_by(SessionCheckpointRow.ordinal.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if payload is None:
        return None
    return Checkpoint.model_validate(payload)


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


def _snapshot_list(document: object, key: str) -> list[object] | None:
    if not isinstance(document, dict):
        return None
    raw = document.get(key)
    if not isinstance(raw, list):
        return None
    return list(raw)


async def _pinned_reference(
    session: AsyncSession, replay_id: str
) -> tuple[list[Market], list[Relationship], list[FeeSchedule]] | None:
    """Markets, relationships, and fees at the versions recorded with the session.

    Journal rows store ``relationship_version`` and ``fee_schedule_version``.
    The session-reference configuration row stores the market metadata version,
    which is not a journal column. Documents are content-addressed, so a later
    edit of the current catalog does not change a recorded session.
    """
    ref = await session.get(ConfigurationVersionRow, f"session-reference:{replay_id}")
    if ref is None or ref.kind != "session-reference":
        return None
    body = ref.document
    market_version = body.get("market_metadata_version")
    relationship_version = body.get("relationship_version")
    fee_version = body.get("fee_schedule_version")
    stamp = (
        await session.execute(
            select(
                OrderbookUpdateRow.relationship_version,
                OrderbookUpdateRow.fee_schedule_version,
            )
            .where(OrderbookUpdateRow.session_id == replay_id)
            .order_by(OrderbookUpdateRow.ordinal)
            .limit(1)
        )
    ).first()
    if stamp is not None:
        relationship_version = stamp[0]
        fee_version = stamp[1]
    if not isinstance(market_version, str):
        return None
    if not isinstance(relationship_version, str) or not isinstance(fee_version, str):
        return None
    markets_row = await session.get(ConfigurationVersionRow, market_version)
    relationships_row = await session.get(ConfigurationVersionRow, relationship_version)
    fees_row = await session.get(ConfigurationVersionRow, fee_version)
    if markets_row is None or relationships_row is None or fees_row is None:
        return None
    market_items = _snapshot_list(markets_row.document, "markets")
    relationship_items = _snapshot_list(relationships_row.document, "relationships")
    fee_items = _snapshot_list(fees_row.document, "schedules")
    if market_items is None or relationship_items is None or fee_items is None:
        return None
    return (
        [Market.model_validate(item) for item in market_items],
        [Relationship.model_validate(item) for item in relationship_items],
        [FeeSchedule.model_validate(item) for item in fee_items],
    )


async def _load_playback(session: AsyncSession, replay_id: str) -> PlaybackSession:
    ingestion = await session.get(IngestionSessionRow, replay_id)
    if ingestion is None:
        raise KeyError(replay_id)
    if not ingestion.raw_persisted:
        raise ValueError("session has no stored journal")
    pinned = await _pinned_reference(session, replay_id)
    if pinned is None:
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
    else:
        markets, relationships, schedules = pinned
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
        "recording_id": session.recording_id,
    }


class ReplayCapacityError(Exception):
    """The process is already holding ``REPLAY_MAX_VIEWERS`` playback sessions."""


class ReplayHost:
    def __init__(
        self,
        *,
        ttl_s: float | None = None,
        max_viewers: int | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if ttl_s is None:
            ttl_s = float(os.environ.get("REPLAY_SESSION_TTL_S", "1800"))
        if max_viewers is None:
            max_viewers = int(os.environ.get("REPLAY_MAX_VIEWERS", "32"))
        if ttl_s <= 0 or max_viewers < 1:
            raise ValueError("replay limits must be positive")
        self.ttl_s = ttl_s
        self.max_viewers = max_viewers
        self._clock = time.monotonic if clock is None else clock
        self.service = PlaybackService()
        self._lock = asyncio.Lock()
        self._background: set[asyncio.Task[None]] = set()
        self._seen: dict[str, float] = {}

    def _finished(self, task: asyncio.Task[None]) -> None:
        self._background.discard(task)
        if not task.cancelled():
            task.exception()

    def touch(self, replay_id: str) -> None:
        try:
            self.service.get(replay_id)
        except KeyError:
            return
        self._seen[replay_id] = self._clock()

    def sweep(self) -> list[str]:
        """Drop viewers idle longer than the TTL and cancel their playback tasks."""
        now = self._clock()
        removed: list[str] = []
        for replay_id, seen in list(self._seen.items()):
            if now - seen < self.ttl_s:
                continue
            self._drop(replay_id)
            removed.append(replay_id)
        return removed

    def require_capacity(self) -> None:
        self.sweep()
        if len(self.service) >= self.max_viewers:
            raise ReplayCapacityError("replay viewer cap")

    def _drop(self, replay_id: str) -> None:
        try:
            playback = self.service.get(replay_id)
        except KeyError:
            playback = None
        if playback is not None:
            playback.close()
        self.service.discard(replay_id)
        self._seen.pop(replay_id, None)

    async def preview(
        self, sessions: async_sessionmaker[AsyncSession], replay_id: str
    ) -> PlaybackSession:
        """Snapshot a recording or an open viewer without creating a shared cursor."""
        try:
            playback = self.service.get(replay_id)
        except KeyError:
            playback = None
        if playback is not None:
            self.touch(replay_id)
            return playback
        async with sessions() as session:
            return await _load_playback(session, replay_id)

    async def ensure(
        self, sessions: async_sessionmaker[AsyncSession], replay_id: str
    ) -> PlaybackSession:
        async with self._lock:
            try:
                playback = self.service.get(replay_id)
            except KeyError:
                playback = None
            if playback is not None:
                self.touch(replay_id)
                return playback
            async with sessions() as session:
                loaded = await _load_playback(session, replay_id)
            self.require_capacity()
            opened = self.service.open(loaded)
            self.touch(opened.replay_id)
            return opened

    async def fork(
        self, sessions: async_sessionmaker[AsyncSession], recording_id: str
    ) -> PlaybackSession:
        """Open a new viewer on ``recording_id`` without moving any other cursor."""
        async with self._lock:
            async with sessions() as session:
                loaded = await _load_playback(session, recording_id)
            self.require_capacity()
            viewer = loaded.isolated_copy("viewer-" + uuid.uuid4().hex)
            opened = self.service.open(viewer)
            self.touch(opened.replay_id)
            return opened

    async def mutate(
        self,
        sessions: async_sessionmaker[AsyncSession],
        replay_id: str,
        action: str,
        body: dict[str, object] | None = None,
    ) -> dict[str, Any]:
        """Apply ``action`` on an isolated viewer.

        A recording id forks a new viewer first. An id that is already a viewer
        keeps that cursor. Two callers of the same recording therefore do not
        share one session.
        """
        async with self._lock:
            try:
                playback = self.service.get(replay_id)
            except KeyError:
                playback = None
            if playback is None:
                async with sessions() as session:
                    loaded = await _load_playback(session, replay_id)
                self.require_capacity()
                playback = self.service.open(loaded.isolated_copy("viewer-" + uuid.uuid4().hex))
            self.touch(playback.replay_id)
            self._apply(playback, action, body or {})
            return snapshot(playback)

    async def command(
        self,
        sessions: async_sessionmaker[AsyncSession],
        replay_id: str,
        action: str,
        body: dict[str, object] | None = None,
    ) -> dict[str, Any]:
        playback = await self.ensure(sessions, replay_id)
        self._apply(playback, action, body or {})
        return snapshot(playback)

    def command_existing(
        self, replay_id: str, action: str, body: dict[str, object] | None = None
    ) -> dict[str, Any]:
        """Mutate a viewer that ``fork`` already opened. Does not load a recording."""
        self.sweep()
        playback = self.service.get(replay_id)
        self.touch(replay_id)
        self._apply(playback, action, body or {})
        return snapshot(playback)

    def _apply(self, playback: PlaybackSession, action: str, payload: dict[str, object]) -> None:
        replay_id = playback.replay_id
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

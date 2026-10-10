"""Versioned catalog, replay, and SSE routes. Reads go through persistence."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from consistency_api.http import authorize_replay
from consistency_api.sse import (
    StreamSlots,
    heartbeat_seconds,
    iter_queue,
    max_connections,
    queue_max,
    run_producer,
)
from consistency_connectors.ingestion import BookManager
from consistency_persistence.dashboard import (
    DetectionQuery,
    apply_journal_market,
    get_detection,
    get_market_row,
    get_relationship,
    list_detections,
    list_markets,
    list_relationships,
    read_overview,
    sync_summary,
)
from consistency_persistence.outbox import committed_notifications, contiguous_watermark
from consistency_persistence.replay_host import ReplayCapacityError, ReplayHost, snapshot
from consistency_persistence.schema import DetectionRow, IngestionSessionRow

router = APIRouter()


def sessions_of(request: Request) -> async_sessionmaker[AsyncSession]:
    maker = getattr(request.app.state, "sessions", None)
    if not isinstance(maker, async_sessionmaker):
        raise HTTPException(status_code=503, detail="database_unavailable")
    return maker


def replay_of(request: Request) -> ReplayHost:
    host = getattr(request.app.state, "replay", None)
    if not isinstance(host, ReplayHost):
        raise HTTPException(status_code=503, detail="replay_unavailable")
    return host


def _decimal(value: str | None) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise HTTPException(status_code=422, detail="invalid_request") from exc
    if not parsed.is_finite():
        raise HTTPException(status_code=422, detail="invalid_request")
    return parsed


def _page(items: list[dict[str, object]], offset: int, limit: int) -> dict[str, object]:
    window = items[offset : offset + limit]
    return {"total": len(items), "offset": offset, "limit": limit, "items": window}


def _frame(event_id: int, event: str, data: dict[str, object]) -> str:
    body = json.dumps(data, separators=(",", ":"), default=_json_default)
    return f"id: {event_id}\nevent: {event}\ndata: {body}\n\n"


def _json_default(value: object) -> str:
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(f"stream JSON cannot encode {type(value).__name__}")


async def _books(request: Request, maker: async_sessionmaker[AsyncSession]) -> BookManager | None:
    books = getattr(request.app.state, "books", None)
    if books is None:
        return None
    try:
        refreshed = await books.refresh(maker)
    except Exception:
        return None
    if isinstance(refreshed, BookManager):
        return refreshed
    return None


@router.get("/api/v1/system/status")
async def system_status(request: Request) -> dict[str, object]:
    maker = sessions_of(request)
    manager = await _books(request, maker)
    summary = None if manager is None else sync_summary(manager)
    async with maker() as session:
        overview = await read_overview(session, summary)
    return {
        "status": "ok",
        "database": "ok",
        "data_source": overview["data_source"],
        "source_health": overview["source_health"],
        "sync_health": overview["sync_health"],
    }


@router.get("/api/v1/system/metrics")
async def system_metrics(request: Request) -> dict[str, object]:
    maker = sessions_of(request)
    async with maker() as session:
        overview = await read_overview(session, None)
    return {
        "markets_monitored": overview["markets_monitored"],
        "relationships_total": overview["relationships_total"],
        "relationships_verified": overview["relationships_verified"],
        "detections_total": overview["detections_total"],
        "detections_open": overview["detections_open"],
        "internal_latency_ns": overview["internal_latency_ns"],
    }


@router.get("/api/v1/markets")
async def markets(
    request: Request,
    status: str | None = None,
    category: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    maker = sessions_of(request)
    async with maker() as session:
        listed = await list_markets(session)
    raw = listed["markets"]
    if not isinstance(raw, list):
        raise HTTPException(status_code=500, detail="internal_error")
    manager = await _books(request, maker)
    items: list[dict[str, object]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        apply_journal_market(item, manager)
        if status is not None and item.get("status") != status:
            continue
        if category is not None and item.get("category") != category:
            continue
        items.append(item)
    page = _page(items, offset, limit)
    return {
        "markets": page["items"],
        "total": page["total"],
        "offset": offset,
        "limit": limit,
    }


@router.get("/api/v1/markets/{market_id}")
async def market(request: Request, market_id: str) -> dict[str, object]:
    maker = sessions_of(request)
    async with maker() as session:
        row = await get_market_row(session, market_id)
    if row is None:
        raise HTTPException(status_code=404, detail="not_found")
    manager = await _books(request, maker)
    apply_journal_market(row, manager)
    return row


@router.get("/api/v1/markets/{market_id}/orderbook")
async def orderbook(request: Request, market_id: str) -> dict[str, object]:
    row = await market(request, market_id)
    book = row.get("book")
    return {"market_id": market_id, "orderbook": book}


@router.get("/api/v1/relationships")
async def relationships(
    request: Request,
    relationship_type: str | None = None,
    status: str | None = None,
    category: str | None = None,
    market_id: str | None = None,
    min_members: int | None = Query(default=None, ge=0),
    max_members: int | None = Query(default=None, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    maker = sessions_of(request)
    async with maker() as session:
        listed = await list_relationships(
            session,
            relationship_type=relationship_type,
            status=status,
            category=category,
            market_id=market_id,
            min_members=min_members,
            max_members=max_members,
        )
    raw = listed["relationships"]
    if not isinstance(raw, list):
        raise HTTPException(status_code=500, detail="internal_error")
    items = [item for item in raw if isinstance(item, dict)]
    page = _page(items, offset, limit)
    return {
        "relationships": page["items"],
        "total": page["total"],
        "offset": offset,
        "limit": limit,
    }


@router.get("/api/v1/relationships/{relationship_id}")
async def relationship(request: Request, relationship_id: str) -> dict[str, object]:
    maker = sessions_of(request)
    async with maker() as session:
        row = await get_relationship(session, relationship_id)
    if row is None:
        raise HTTPException(status_code=404, detail="not_found")
    return row


@router.get("/api/v1/detections")
async def detections(
    request: Request,
    classification: str | None = None,
    relationship_type: str | None = None,
    market_id: str | None = None,
    since_ms: int | None = Query(default=None, ge=0),
    until_ms: int | None = Query(default=None, ge=0),
    min_net_edge: str | None = None,
    min_quantity: str | None = None,
    sort: str = "time",
    direction: str = "desc",
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    if direction not in {"asc", "desc"}:
        raise HTTPException(status_code=422, detail="invalid_request")
    maker = sessions_of(request)
    query = DetectionQuery(
        classification=classification,
        relationship_type=relationship_type,
        since_ms=since_ms,
        until_ms=until_ms,
        min_net_edge=_decimal(min_net_edge),
        min_quantity=_decimal(min_quantity),
        market_id=market_id,
        sort=sort,
        direction=direction,
    )
    async with maker() as session:
        listed = await list_detections(session, query, limit=limit, offset=offset)
    return listed


@router.get("/api/v1/detections/{detection_id}")
async def detection(request: Request, detection_id: str) -> dict[str, object]:
    maker = sessions_of(request)
    async with maker() as session:
        row = await get_detection(session, detection_id)
    if row is None:
        raise HTTPException(status_code=404, detail="not_found")
    return row


@router.get("/api/v1/detections/{detection_id}/proof")
async def detection_proof(request: Request, detection_id: str) -> dict[str, object]:
    row = await detection(request, detection_id)
    return {
        "detection_id": detection_id,
        "certificate_hash": row.get("certificate_hash"),
        "certificate": row.get("certificate"),
        "legs": row.get("legs"),
        "scenarios": row.get("scenarios"),
        "technical_report": row.get("technical_report"),
    }


@router.get("/api/v1/replay/sessions")
async def replay_sessions(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    maker = sessions_of(request)
    async with maker() as session:
        rows = (
            (
                await session.execute(
                    select(IngestionSessionRow).order_by(IngestionSessionRow.started_at.desc())
                )
            )
            .scalars()
            .all()
        )
    items: list[dict[str, object]] = [
        {
            "session_id": row.session_id,
            "status": row.status,
            "source_label": row.source_label,
            "deterministic": row.deterministic,
            "raw_persisted": row.raw_persisted,
            "started_at": row.started_at.isoformat(),
        }
        for row in rows
    ]
    page = _page(items, offset, limit)
    return {
        "sessions": page["items"],
        "total": page["total"],
        "offset": offset,
        "limit": limit,
    }


@router.get("/api/v1/replay/sessions/{session_id}")
async def replay_session(request: Request, session_id: str) -> dict[str, object]:
    host = replay_of(request)
    try:
        playback = host.service.get(session_id)
    except KeyError:
        playback = None
    if playback is not None:
        host.touch(session_id)
        body = snapshot(playback)
        body["viewer"] = True
        return body
    maker = sessions_of(request)
    async with maker() as session:
        row = await session.get(IngestionSessionRow, session_id)
    if row is None:
        raise HTTPException(status_code=404, detail="not_found")
    return {
        "session_id": row.session_id,
        "status": row.status,
        "source_label": row.source_label,
        "deterministic": row.deterministic,
        "raw_persisted": row.raw_persisted,
        "started_at": row.started_at.isoformat(),
        "viewer": False,
    }


@router.post("/api/v1/replay/sessions/{session_id}/start")
async def replay_start(request: Request, session_id: str) -> dict[str, object]:
    authorize_replay(request)
    host = replay_of(request)
    maker = sessions_of(request)
    try:
        playback = await host.fork(maker, session_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="not_found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail="journal_unavailable") from exc
    except ReplayCapacityError as exc:
        raise HTTPException(status_code=429, detail="replay_capacity") from exc
    body = host.command_existing(playback.replay_id, "start", {})
    body["viewer"] = True
    return body


@router.post("/api/v1/replay/sessions/{session_id}/pause")
async def replay_pause(request: Request, session_id: str) -> dict[str, object]:
    return _mutate(request, session_id, "pause")


@router.post("/api/v1/replay/sessions/{session_id}/seek")
async def replay_seek(request: Request, session_id: str) -> dict[str, object]:
    payload = await _object_body(request)
    return _mutate(request, session_id, "seek", payload)


@router.post("/api/v1/replay/sessions/{session_id}/resume")
async def replay_resume(request: Request, session_id: str) -> dict[str, object]:
    return _mutate(request, session_id, "resume")


@router.post("/api/v1/replay/sessions/{session_id}/restart")
async def replay_restart(request: Request, session_id: str) -> dict[str, object]:
    return _mutate(request, session_id, "restart")


@router.post("/api/v1/replay/sessions/{session_id}/step")
async def replay_step(request: Request, session_id: str) -> dict[str, object]:
    return _mutate(request, session_id, "step")


@router.post("/api/v1/replay/sessions/{session_id}/speed")
async def replay_speed(request: Request, session_id: str) -> dict[str, object]:
    payload = await _object_body(request)
    return _mutate(request, session_id, "speed", payload)


@router.get("/api/v1/stream")
async def stream(
    request: Request,
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
) -> StreamingResponse:
    maker = sessions_of(request)
    cursor = _event_cursor(last_event_id)
    slots = _sse_slots(request)
    if not await slots.try_acquire():
        raise HTTPException(status_code=503, detail="stream_unavailable")

    async def body() -> AsyncIterator[str]:
        try:
            async for chunk in _tail(maker, cursor):
                yield chunk
        finally:
            await slots.release()

    return StreamingResponse(
        body(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _sse_slots(request: Request) -> StreamSlots:
    limit = max_connections()
    current = getattr(request.app.state, "sse_slots", None)
    if not isinstance(current, StreamSlots) or current.limit != limit:
        current = StreamSlots(limit)
        request.app.state.sse_slots = current
    return current


def _mutate(
    request: Request,
    session_id: str,
    action: str,
    payload: dict[str, object] | None = None,
) -> dict[str, object]:
    authorize_replay(request)
    host = replay_of(request)
    try:
        body = host.command_existing(session_id, action, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="not_found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid_request") from exc
    body["viewer"] = True
    return body


async def _object_body(request: Request) -> dict[str, object]:
    raw = await request.body()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail="invalid_request") from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=422, detail="invalid_request")
    return {str(key): value for key, value in parsed.items()}


def _event_cursor(header: str | None) -> int | None:
    if header is None or header == "":
        return None
    try:
        parsed = int(header)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid_request") from exc
    if parsed < 0:
        raise HTTPException(status_code=422, detail="invalid_request")
    return parsed


async def _detection_snapshot(session: AsyncSession) -> list[dict[str, object]]:
    rows = (
        await session.execute(
            select(
                DetectionRow.detection_id,
                DetectionRow.classification,
                DetectionRow.status,
                DetectionRow.certificate_hash,
                DetectionRow.session_id,
            ).order_by(DetectionRow.detection_id)
        )
    ).all()
    return [
        {
            "detection_id": str(row[0]),
            "classification": str(row[1]),
            "status": str(row[2]),
            "certificate_hash": str(row[3]),
            "session_id": str(row[4]),
        }
        for row in rows
    ]


async def _tail(
    maker: async_sessionmaker[AsyncSession],
    last_event_id: int | None,
) -> AsyncIterator[str]:
    """Tail until the client disconnects or the bounded queue overflows.

    The queue cap is the memory bound for a slow client. Frames that do not fit
    are left in the outbox; the client reconnects with ``Last-Event-ID``.
    """
    cursor = 0 if last_event_id is None else last_event_id - 1
    boundary_checked = last_event_id is None
    sent_open = last_event_id is not None

    async def poll() -> list[str]:
        nonlocal cursor, boundary_checked, sent_open
        if not sent_open:
            sent_open = True
            async with maker() as session:
                watermark, payload = await _resync(session)
            cursor = watermark
            return [_frame(watermark, "resync", payload)]
        async with maker() as session:
            writers, notes = await committed_notifications(session, cursor, limit=100)
        if not boundary_checked and last_event_id is not None:
            boundary_checked = True
            if notes and notes[0].id > last_event_id and not writers:
                async with maker() as session:
                    watermark, payload = await _resync(session)
                cursor = watermark
                return [_frame(watermark, "resync", payload)]
        frames: list[str] = []
        for note in notes:
            frames.append(
                _frame(
                    note.id,
                    note.topic,
                    {"topic": note.topic, "session_id": note.session_id, "payload": note.payload},
                )
            )
            cursor = note.id
        return frames

    queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=queue_max())
    overflow = asyncio.Event()
    producer = asyncio.create_task(
        run_producer(
            queue,
            poll,
            heartbeat_s=heartbeat_seconds(),
            poll_s=0.05,
            overflow=overflow,
        )
    )
    try:
        async for chunk in iter_queue(queue, overflow):
            yield chunk
    finally:
        producer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await producer


async def _resync(session: AsyncSession) -> tuple[int, dict[str, object]]:
    """Detection rows and the outbox watermark from one repeatable-read snapshot.

    A commit that lands while this runs is invisible to both reads. It is
    delivered on the tail after ``watermark``, so an older event is not applied
    on top of a newer snapshot. The watermark is not capped at 10 000 rows.
    """
    await session.connection(execution_options={"isolation_level": "REPEATABLE READ"})
    watermark = await contiguous_watermark(session, 0)
    return watermark, {
        "detections": await _detection_snapshot(session),
        "outbox_id": watermark,
    }

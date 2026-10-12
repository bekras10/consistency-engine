"""Study-scoped observation ledger.

Aggregates are queries over an append-only event log. An idempotency key is the
primary key, so a worker that restarts and writes the same key again does not
double-count. An opportunity episode is one detection id: later book updates of
that id move ``last_ms`` forward and do not insert another episode.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from sqlalchemy import case, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from consistency_persistence.schema import (
    ObservationStudyRow,
    OpportunityEpisodeRow,
    OrderbookUpdateRow,
    StudyEventRow,
    StudyMembershipRow,
    StudySessionRow,
)
from consistency_persistence.timeutil import now_utc

LIVE_SOURCE = "kalshi_authorized"


@dataclass(frozen=True)
class CoverageInterval:
    start_ms: int
    end_ms: int
    synchronized: bool


@dataclass(frozen=True)
class StudyFacts:
    """Everything the report may say about one real study. Synthetic rows are absent."""

    found: bool
    study_id: str
    source_kind: str
    environment: str
    commit_sha: str
    config_fingerprint: str
    planned_start: str
    planned_end: str
    raw_retention_authorized: bool
    replay_available: bool
    eligible_sessions: int
    market_ids: tuple[str, ...]
    relationship_ids: tuple[str, ...]
    verified_relationship_ids: tuple[str, ...]
    membership: tuple[dict[str, object], ...]
    evaluation_rows: tuple[dict[str, object], ...]
    episode_rows: tuple[dict[str, object], ...]
    observed_ms: int | None
    synchronized_ms: int | None
    downtime_ms: int
    desync_ms: int
    coverage_established: bool
    analyzed_updates: int | None
    reconnects: int
    skipped: int
    excluded: int
    fee_verified_checks: int
    fee_unverified_checks: int
    latency_samples: int
    latency_max_ms: int | None
    freshness_samples: int
    exclusions: tuple[str, ...]


def iso_to_ms(value: str) -> int | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return int(parsed.timestamp() * 1000)


def merge_intervals(intervals: Sequence[tuple[int, int]]) -> list[tuple[int, int]]:
    """Half-open intervals merged in start order. Overlaps are not double-counted."""
    ordered = sorted((start, end) for start, end in intervals if end > start)
    if not ordered:
        return []
    merged: list[tuple[int, int]] = [ordered[0]]
    for start, end in ordered[1:]:
        prev_start, prev_end = merged[-1]
        if start > prev_end:
            merged.append((start, end))
        else:
            merged[-1] = (prev_start, max(prev_end, end))
    return merged


def span_ms(intervals: Sequence[tuple[int, int]]) -> int:
    return sum(end - start for start, end in merge_intervals(intervals))


def covers_window(
    intervals: Sequence[tuple[int, int]], *, start_ms: int | None, end_ms: int | None
) -> bool:
    """True only when synchronized coverage reaches both ends of the planned window."""
    if start_ms is None or end_ms is None or end_ms <= start_ms:
        return False
    cursor = start_ms
    for start, end in merge_intervals(intervals):
        if end <= cursor:
            continue
        if start > cursor:
            return False
        cursor = end
        if cursor >= end_ms:
            return True
    return cursor >= end_ms


def _int_field(payload: Mapping[str, object], key: str) -> int | None:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _interval(payload: Mapping[str, object]) -> tuple[int, int] | None:
    start = _int_field(payload, "start_ms")
    end = _int_field(payload, "end_ms")
    if start is None or end is None or end < start:
        return None
    return start, end


def _money(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


class StudyLedger:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def ensure_study(
        self,
        *,
        study_id: str,
        source_kind: str,
        environment: str,
        commit_sha: str,
        config_fingerprint: str,
        planned_start: str,
        planned_end: str,
        raw_retention_authorized: bool,
    ) -> None:
        stmt = pg_insert(ObservationStudyRow).values(
            study_id=study_id,
            source_kind=source_kind,
            environment=environment,
            commit_sha=commit_sha,
            config_fingerprint=config_fingerprint,
            planned_start=planned_start,
            planned_end=planned_end,
            raw_retention_authorized=raw_retention_authorized,
            created_at=now_utc(),
        )
        stmt = stmt.on_conflict_do_nothing(index_elements=["study_id"])
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)

    async def open_session(
        self,
        *,
        study_id: str,
        session_id: str,
        source_kind: str,
        eligible: bool,
        started_ms: int,
    ) -> None:
        stmt = pg_insert(StudySessionRow).values(
            study_id=study_id,
            session_id=session_id,
            source_kind=source_kind,
            eligible=eligible,
            started_ms=started_ms,
            ended_ms=None,
        )
        stmt = stmt.on_conflict_do_nothing(index_elements=["study_id", "session_id"])
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)

    async def close_session(self, *, study_id: str, session_id: str, ended_ms: int) -> None:
        async with self._sessions() as session, session.begin():
            await session.execute(
                update(StudySessionRow)
                .where(
                    StudySessionRow.study_id == study_id,
                    StudySessionRow.session_id == session_id,
                    StudySessionRow.ended_ms.is_(None),
                )
                .values(ended_ms=ended_ms)
            )

    async def enter_membership(
        self,
        *,
        study_id: str,
        session_id: str,
        subject_kind: str,
        subject_id: str,
        entered_ms: int,
        fingerprint: str,
        verification_status: str,
    ) -> None:
        stmt = pg_insert(StudyMembershipRow).values(
            study_id=study_id,
            session_id=session_id,
            subject_kind=subject_kind,
            subject_id=subject_id,
            entered_ms=entered_ms,
            exited_ms=None,
            fingerprint=fingerprint,
            verification_status=verification_status,
        )
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["study_id", "session_id", "subject_kind", "subject_id", "entered_ms"]
        )
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)

    async def exit_membership(
        self,
        *,
        study_id: str,
        session_id: str,
        subject_kind: str,
        subject_id: str,
        exited_ms: int,
    ) -> None:
        async with self._sessions() as session, session.begin():
            await session.execute(
                update(StudyMembershipRow)
                .where(
                    StudyMembershipRow.study_id == study_id,
                    StudyMembershipRow.session_id == session_id,
                    StudyMembershipRow.subject_kind == subject_kind,
                    StudyMembershipRow.subject_id == subject_id,
                    StudyMembershipRow.exited_ms.is_(None),
                )
                .values(exited_ms=exited_ms)
            )

    async def record_events(self, events: Sequence[Mapping[str, object]]) -> None:
        if not events:
            return
        rows = [_event_values(item) for item in events]
        stmt = pg_insert(StudyEventRow).values(rows)
        stmt = stmt.on_conflict_do_nothing(index_elements=["idempotency_key"])
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)

    async def upsert_episodes(self, episodes: Sequence[Mapping[str, object]]) -> None:
        if not episodes:
            return
        rows = [_episode_values(item) for item in episodes]
        stmt = pg_insert(OpportunityEpisodeRow).values(rows)
        excluded = stmt.excluded
        stmt = stmt.on_conflict_do_update(
            index_elements=["episode_id"],
            set_={
                "last_ms": func.greatest(OpportunityEpisodeRow.last_ms, excluded.last_ms),
                "first_ms": func.least(OpportunityEpisodeRow.first_ms, excluded.first_ms),
                "classification": case(
                    (
                        excluded.last_ms >= OpportunityEpisodeRow.last_ms,
                        excluded.classification,
                    ),
                    else_=OpportunityEpisodeRow.classification,
                ),
                "status": case(
                    (excluded.last_ms >= OpportunityEpisodeRow.last_ms, excluded.status),
                    else_=OpportunityEpisodeRow.status,
                ),
                "close_reason": case(
                    (
                        excluded.last_ms >= OpportunityEpisodeRow.last_ms,
                        excluded.close_reason,
                    ),
                    else_=OpportunityEpisodeRow.close_reason,
                ),
                "gross_edge": case(
                    (excluded.last_ms >= OpportunityEpisodeRow.last_ms, excluded.gross_edge),
                    else_=OpportunityEpisodeRow.gross_edge,
                ),
                "fees": case(
                    (excluded.last_ms >= OpportunityEpisodeRow.last_ms, excluded.fees),
                    else_=OpportunityEpisodeRow.fees,
                ),
                "net_edge": case(
                    (excluded.last_ms >= OpportunityEpisodeRow.last_ms, excluded.net_edge),
                    else_=OpportunityEpisodeRow.net_edge,
                ),
                "quantity": case(
                    (excluded.last_ms >= OpportunityEpisodeRow.last_ms, excluded.quantity),
                    else_=OpportunityEpisodeRow.quantity,
                ),
            },
        )
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)

    async def load(self, study_id: str) -> StudyFacts:
        async with self._sessions() as session:
            return await _load(session, study_id)


def _event_values(item: Mapping[str, object]) -> dict[str, object]:
    payload = item.get("payload")
    body = dict(payload) if isinstance(payload, Mapping) else {}
    classification = item.get("classification")
    subject = item.get("subject_id")
    return {
        "idempotency_key": str(item["idempotency_key"]),
        "study_id": str(item["study_id"]),
        "session_id": str(item["session_id"]),
        "kind": str(item["kind"]),
        "occurred_ms": int(cast(int, item["occurred_ms"])),
        "subject_id": "" if subject is None else str(subject),
        "classification": "" if classification is None else str(classification),
        "synchronized": bool(item.get("synchronized", False)),
        "payload": body,
    }


def _decimal_field(item: Mapping[str, object], key: str) -> Decimal | None:
    value = item.get(key)
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool | float):
        raise TypeError(f"{key} must be Decimal or a fixed-point string")
    if isinstance(value, int | str):
        return Decimal(value)
    raise TypeError(f"{key} must be Decimal or a fixed-point string")


def _episode_values(item: Mapping[str, object]) -> dict[str, object]:
    close = item.get("close_reason")
    return {
        "episode_id": str(item["episode_id"]),
        "study_id": str(item["study_id"]),
        "session_id": str(item["session_id"]),
        "detection_id": str(item["detection_id"]),
        "relationship_id": str(item["relationship_id"]),
        "strategy_id": str(item["strategy_id"]),
        "classification": str(item["classification"]),
        "status": str(item["status"]),
        "first_ms": int(cast(int, item["first_ms"])),
        "last_ms": int(cast(int, item["last_ms"])),
        "close_reason": None if close is None else str(close),
        "gross_edge": _decimal_field(item, "gross_edge"),
        "fees": _decimal_field(item, "fees"),
        "net_edge": _decimal_field(item, "net_edge"),
        "quantity": _decimal_field(item, "quantity"),
    }


def _empty(study_id: str) -> StudyFacts:
    return StudyFacts(
        found=False,
        study_id=study_id,
        source_kind="",
        environment="",
        commit_sha="",
        config_fingerprint="",
        planned_start="",
        planned_end="",
        raw_retention_authorized=False,
        replay_available=False,
        eligible_sessions=0,
        market_ids=(),
        relationship_ids=(),
        verified_relationship_ids=(),
        membership=(),
        evaluation_rows=(),
        episode_rows=(),
        observed_ms=None,
        synchronized_ms=None,
        downtime_ms=0,
        desync_ms=0,
        coverage_established=False,
        analyzed_updates=None,
        reconnects=0,
        skipped=0,
        excluded=0,
        fee_verified_checks=0,
        fee_unverified_checks=0,
        latency_samples=0,
        latency_max_ms=None,
        freshness_samples=0,
        exclusions=(),
    )


async def _load(session: AsyncSession, study_id: str) -> StudyFacts:
    study = await session.get(ObservationStudyRow, study_id)
    if study is None or study.source_kind != LIVE_SOURCE:
        return _empty(study_id)
    sessions = (
        (
            await session.execute(
                select(StudySessionRow)
                .where(
                    StudySessionRow.study_id == study_id,
                    StudySessionRow.eligible.is_(True),
                    StudySessionRow.source_kind == LIVE_SOURCE,
                )
                .order_by(StudySessionRow.session_id)
            )
        )
        .scalars()
        .all()
    )
    session_ids = [row.session_id for row in sessions]
    if not session_ids:
        return StudyFacts(
            found=True,
            study_id=study.study_id,
            source_kind=study.source_kind,
            environment=study.environment,
            commit_sha=study.commit_sha,
            config_fingerprint=study.config_fingerprint,
            planned_start=study.planned_start,
            planned_end=study.planned_end,
            raw_retention_authorized=study.raw_retention_authorized,
            replay_available=False,
            eligible_sessions=0,
            market_ids=(),
            relationship_ids=(),
            verified_relationship_ids=(),
            membership=(),
            evaluation_rows=(),
            episode_rows=(),
            observed_ms=None,
            synchronized_ms=None,
            downtime_ms=0,
            desync_ms=0,
            coverage_established=False,
            analyzed_updates=0,
            reconnects=0,
            skipped=0,
            excluded=0,
            fee_verified_checks=0,
            fee_unverified_checks=0,
            latency_samples=0,
            latency_max_ms=None,
            freshness_samples=0,
            exclusions=("no eligible real-market session",),
        )
    events = (
        (
            await session.execute(
                select(StudyEventRow)
                .where(
                    StudyEventRow.study_id == study_id,
                    StudyEventRow.session_id.in_(session_ids),
                )
                .order_by(StudyEventRow.occurred_ms, StudyEventRow.idempotency_key)
            )
        )
        .scalars()
        .all()
    )
    membership = (
        (
            await session.execute(
                select(StudyMembershipRow)
                .where(
                    StudyMembershipRow.study_id == study_id,
                    StudyMembershipRow.session_id.in_(session_ids),
                )
                .order_by(
                    StudyMembershipRow.subject_kind,
                    StudyMembershipRow.subject_id,
                    StudyMembershipRow.entered_ms,
                )
            )
        )
        .scalars()
        .all()
    )
    episodes = (
        (
            await session.execute(
                select(OpportunityEpisodeRow)
                .where(
                    OpportunityEpisodeRow.study_id == study_id,
                    OpportunityEpisodeRow.session_id.in_(session_ids),
                )
                .order_by(OpportunityEpisodeRow.episode_id)
            )
        )
        .scalars()
        .all()
    )
    replay = False
    if study.raw_retention_authorized:
        stored = await session.scalar(
            select(func.count())
            .select_from(OrderbookUpdateRow)
            .where(OrderbookUpdateRow.session_id.in_(session_ids))
        )
        replay = bool(stored)
    return _facts(study, sessions, events, membership, episodes, replay_available=replay)


def _facts(
    study: ObservationStudyRow,
    sessions: Sequence[StudySessionRow],
    events: Sequence[StudyEventRow],
    membership: Sequence[StudyMembershipRow],
    episodes: Sequence[OpportunityEpisodeRow],
    *,
    replay_available: bool,
) -> StudyFacts:
    evaluations: list[dict[str, object]] = []
    coverage: list[tuple[int, int]] = []
    downtime: list[tuple[int, int]] = []
    desync: list[tuple[int, int]] = []
    observed: list[tuple[int, int]] = []
    updates = 0
    reconnects = 0
    skipped = 0
    excluded = 0
    fee_verified = 0
    fee_unverified = 0
    latency: list[int] = []
    freshness = 0
    for event in events:
        payload = event.payload
        if event.kind == "evaluation":
            evaluations.append(
                {
                    "classification": event.classification,
                    "synchronized": event.synchronized,
                    "occurred_ms": event.occurred_ms,
                    "subject_id": event.subject_id,
                }
            )
        elif event.kind == "book_update":
            updates += 1
        elif event.kind == "reconnect":
            reconnects += 1
        elif event.kind == "skip":
            skipped += 1
        elif event.kind == "exclusion":
            excluded += 1
        elif event.kind == "fee_verification":
            if payload.get("verified") is True:
                fee_verified += 1
            else:
                fee_unverified += 1
        elif event.kind == "latency":
            sample = _int_field(payload, "latency_ms")
            if sample is not None:
                latency.append(sample)
        elif event.kind == "freshness":
            freshness += 1
        interval = _interval(payload)
        if interval is None:
            continue
        if event.kind == "coverage" and event.synchronized:
            coverage.append(interval)
            observed.append(interval)
        elif event.kind == "downtime":
            downtime.append(interval)
            observed.append(interval)
        elif event.kind == "desync":
            desync.append(interval)
    markets = tuple(sorted({row.subject_id for row in membership if row.subject_kind == "market"}))
    relationships = tuple(
        sorted({row.subject_id for row in membership if row.subject_kind == "relationship"})
    )
    verified = tuple(
        sorted(
            {
                row.subject_id
                for row in membership
                if row.subject_kind == "relationship" and row.verification_status == "verified"
            }
        )
    )
    membership_rows = tuple(
        cast(
            dict[str, object],
            {
                "subject_kind": row.subject_kind,
                "subject_id": row.subject_id,
                "entered_ms": row.entered_ms,
                "exited_ms": row.exited_ms,
                "fingerprint": row.fingerprint,
                "verification_status": row.verification_status,
                "session_id": row.session_id,
            },
        )
        for row in membership
    )
    episode_rows = tuple(
        cast(
            dict[str, object],
            {
                "detection_id": row.detection_id,
                "relationship_id": row.relationship_id,
                "strategy_id": row.strategy_id,
                "classification": row.classification,
                "status": row.status,
                "first_observed_ms": row.first_ms,
                "last_observed_ms": row.last_ms,
                "close_reason": row.close_reason,
                "gross_edge": _money(row.gross_edge),
                "fees": _money(row.fees),
                "net_edge": _money(row.net_edge),
                "depth_supported_quantity": _money(row.quantity),
            },
        )
        for row in episodes
    )
    planned_start = iso_to_ms(study.planned_start)
    planned_end = iso_to_ms(study.planned_end)
    synchronized = span_ms(coverage) if coverage else 0
    observed_ms = span_ms(observed) if observed else None
    has_observations = bool(evaluations or episodes or coverage or membership)
    return StudyFacts(
        found=True,
        study_id=study.study_id,
        source_kind=study.source_kind,
        environment=study.environment,
        commit_sha=study.commit_sha,
        config_fingerprint=study.config_fingerprint,
        planned_start=study.planned_start,
        planned_end=study.planned_end,
        raw_retention_authorized=study.raw_retention_authorized,
        replay_available=replay_available,
        eligible_sessions=len(sessions),
        market_ids=markets,
        relationship_ids=relationships,
        verified_relationship_ids=verified,
        membership=membership_rows,
        evaluation_rows=tuple(evaluations),
        episode_rows=episode_rows,
        observed_ms=observed_ms if has_observations else None,
        synchronized_ms=synchronized if has_observations else None,
        downtime_ms=span_ms(downtime),
        desync_ms=span_ms(desync),
        coverage_established=covers_window(coverage, start_ms=planned_start, end_ms=planned_end),
        analyzed_updates=updates,
        reconnects=reconnects,
        skipped=skipped,
        excluded=excluded,
        fee_verified_checks=fee_verified,
        fee_unverified_checks=fee_unverified,
        latency_samples=len(latency),
        latency_max_ms=max(latency) if latency else None,
        freshness_samples=freshness,
        exclusions=(),
    )


def episode_identity(study_id: str, detection_id: str) -> str:
    """Stable across restarts. Book updates must reuse the detection id."""
    return f"{study_id}|{detection_id}"


# Imported by callers that stamp reports in UTC. Kept here so tests share one clock helper.
def utc_now() -> datetime:
    return datetime.now(UTC)

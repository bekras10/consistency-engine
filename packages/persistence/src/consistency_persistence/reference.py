"""Upsert reference data: sources, catalog, relationships, reviews, fee schedules."""

from __future__ import annotations

from typing import Any

from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from consistency_core.fees.schedule import FeeSchedule
from consistency_core.models.market import Catalog
from consistency_core.models.relationship import Relationship
from consistency_core.serialization import sha256_of
from consistency_persistence.schema import (
    ConfigurationVersionRow,
    DataSourceRow,
    EventRow,
    FeeScheduleRow,
    MarketPriceRangeRow,
    MarketRow,
    MarketRulesRow,
    RelationshipMemberRow,
    RelationshipReviewRow,
    RelationshipRow,
    SeriesRow,
)
from consistency_persistence.timeutil import now_utc

_CREDENTIAL_KEYS = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "api_key",
        "token",
        "authorization",
        "database_url",
        "replay_api_token",
        "cookie",
    }
)


def _url_has_password(value: str) -> bool:
    _scheme, separator, rest = value.partition("://")
    if not separator:
        return False
    userinfo, at, _host = rest.partition("@")
    return bool(at) and ":" in userinfo and " " not in userinfo


def assert_no_credentials(document: object) -> None:
    """Refuse a document that would store a third-party credential in PostgreSQL."""

    def walk(value: object, key: str = "") -> None:
        if key.lower() in _CREDENTIAL_KEYS:
            raise ValueError("refusing to store a credential in PostgreSQL")
        if isinstance(value, str):
            if _url_has_password(value) or "ce_replay_capability=" in value.lower():
                raise ValueError("refusing to store a credential in PostgreSQL")
            return
        if isinstance(value, dict):
            for item_key, item in value.items():
                walk(item, str(item_key))
            return
        if isinstance(value, list):
            for item in value:
                walk(item, key)

    walk(document)


def _doc(model: Any) -> dict[str, Any]:
    document: dict[str, Any] = model.model_dump(mode="json")
    return document


async def upsert_data_source(
    session: AsyncSession, *, data_source_id: str, name: str, kind: str
) -> None:
    stmt = pg_insert(DataSourceRow).values(
        data_source_id=data_source_id,
        name=name,
        kind=kind,
        created_at=now_utc(),
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[DataSourceRow.data_source_id],
        set_={"name": stmt.excluded.name, "kind": stmt.excluded.kind},
    )
    await session.execute(stmt)


async def upsert_catalog(session: AsyncSession, catalog: Catalog, data_source_id: str) -> None:
    now = now_utc()
    for series in catalog.series:
        stmt = pg_insert(SeriesRow).values(
            series_id=series.series_id,
            data_source_id=data_source_id,
            title=series.title,
            category=series.category,
            provenance=series.provenance.source_kind.value,
            document=_doc(series),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[SeriesRow.series_id],
            set_={
                "title": stmt.excluded.title,
                "category": stmt.excluded.category,
                "provenance": stmt.excluded.provenance,
                "document": stmt.excluded.document,
            },
        )
        await session.execute(stmt)
    for event in catalog.events:
        stmt = pg_insert(EventRow).values(
            event_id=event.event_id,
            ticker=event.event_id,
            series_id=event.series_id,
            title=event.title,
            mutually_exclusive=event.mutually_exclusive,
            outcome_set_complete=event.outcome_set_complete,
            provenance=event.provenance.source_kind.value,
            document=_doc(event),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[EventRow.event_id],
            set_={
                "ticker": stmt.excluded.ticker,
                "title": stmt.excluded.title,
                "mutually_exclusive": stmt.excluded.mutually_exclusive,
                "outcome_set_complete": stmt.excluded.outcome_set_complete,
                "document": stmt.excluded.document,
            },
        )
        await session.execute(stmt)
    for market in catalog.markets:
        stmt = pg_insert(MarketRow).values(
            market_id=market.market_id,
            ticker=market.ticker,
            event_id=market.event_id,
            series_id=market.series_id,
            title=market.title,
            status=market.status.value,
            open_time=market.open_time,
            close_time=market.close_time,
            expiration_time=market.expiration_time,
            quantity_increment=market.quantity_increment,
            rules_version=market.rules_version,
            rules_hash=market.rules_hash,
            provenance=market.provenance.source_kind.value,
            document=_doc(market),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[MarketRow.market_id],
            set_={
                "ticker": stmt.excluded.ticker,
                "title": stmt.excluded.title,
                "status": stmt.excluded.status,
                "open_time": stmt.excluded.open_time,
                "close_time": stmt.excluded.close_time,
                "expiration_time": stmt.excluded.expiration_time,
                "quantity_increment": stmt.excluded.quantity_increment,
                "rules_version": stmt.excluded.rules_version,
                "rules_hash": stmt.excluded.rules_hash,
                "document": stmt.excluded.document,
            },
        )
        await session.execute(stmt)
        await session.execute(
            delete(MarketPriceRangeRow).where(MarketPriceRangeRow.market_id == market.market_id)
        )
        for index, tick in enumerate(market.price_grid.ranges):
            session.add(
                MarketPriceRangeRow(
                    market_id=market.market_id,
                    range_index=index,
                    start_price=tick.start,
                    end_price=tick.end,
                    step=tick.step,
                )
            )
        rules = pg_insert(MarketRulesRow).values(
            market_id=market.market_id,
            rules_version=market.rules_version,
            rules_hash=market.rules_hash,
            rules_text=market.settlement_rules,
            settlement=_doc(market.settlement),
            recorded_at=now,
        )
        rules = rules.on_conflict_do_nothing(constraint="uq_market_rules_hash")
        await session.execute(rules)


async def upsert_relationships(session: AsyncSession, relationships: list[Relationship]) -> None:
    for rel in relationships:
        stmt = pg_insert(RelationshipRow).values(
            relationship_id=rel.relationship_id,
            relationship_type=rel.relationship_type.value,
            verification_status=rel.verification_status.value,
            exhaustive=rel.exhaustive,
            fingerprint=rel.fingerprint,
            created_at=rel.created_at,
            updated_at=rel.updated_at,
            document=_doc(rel),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[RelationshipRow.relationship_id],
            set_={
                "relationship_type": stmt.excluded.relationship_type,
                "verification_status": stmt.excluded.verification_status,
                "exhaustive": stmt.excluded.exhaustive,
                "fingerprint": stmt.excluded.fingerprint,
                "updated_at": stmt.excluded.updated_at,
                "document": stmt.excluded.document,
            },
        )
        await session.execute(stmt)
        await session.execute(
            delete(RelationshipMemberRow).where(
                RelationshipMemberRow.relationship_id == rel.relationship_id
            )
        )
        await session.execute(
            delete(RelationshipReviewRow).where(
                RelationshipReviewRow.relationship_id == rel.relationship_id
            )
        )
        for position, market_id in enumerate(rel.members):
            session.add(
                RelationshipMemberRow(
                    relationship_id=rel.relationship_id,
                    position=position,
                    market_id=market_id,
                )
            )
        for review in rel.reviews:
            session.add(
                RelationshipReviewRow(
                    relationship_id=rel.relationship_id,
                    review_id=review.review_id,
                    reviewer=review.reviewer,
                    decision=review.decision.value,
                    reasoning=review.reasoning,
                    reviewed_at=review.reviewed_at,
                    source=review.source,
                    document=_doc(review),
                )
            )


async def upsert_fee_schedules(session: AsyncSession, schedules: list[FeeSchedule]) -> None:
    for schedule in schedules:
        content_hash = sha256_of(schedule)
        stmt = pg_insert(FeeScheduleRow).values(
            schedule_id=schedule.schedule_id,
            venue=schedule.venue.value,
            content_hash=content_hash,
            effective_from=schedule.effective_from,
            effective_to=schedule.effective_to,
            taker_coefficient=schedule.taker_coefficient,
            maker_coefficient=schedule.maker_coefficient,
            document=_doc(schedule),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[FeeScheduleRow.schedule_id],
            set_={
                "venue": stmt.excluded.venue,
                "content_hash": stmt.excluded.content_hash,
                "effective_from": stmt.excluded.effective_from,
                "effective_to": stmt.excluded.effective_to,
                "taker_coefficient": stmt.excluded.taker_coefficient,
                "maker_coefficient": stmt.excluded.maker_coefficient,
                "document": stmt.excluded.document,
            },
        )
        await session.execute(stmt)


async def upsert_configuration(
    session: AsyncSession, *, version_id: str, kind: str, document: dict[str, Any]
) -> None:
    assert_no_credentials(document)
    stmt = pg_insert(ConfigurationVersionRow).values(
        version_id=version_id,
        kind=kind,
        created_at=now_utc(),
        document=document,
    )
    stmt = stmt.on_conflict_do_nothing(index_elements=[ConfigurationVersionRow.version_id])
    await session.execute(stmt)

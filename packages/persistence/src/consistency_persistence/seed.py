"""Load synthetic reference data (markets, relationships, reviews, fee schedules)."""

from __future__ import annotations

from sqlalchemy import func, select

from consistency_core.fees.schedule import FeeSchedule
from consistency_core.models.market import Catalog
from consistency_core.models.relationship import Relationship
from consistency_persistence.reference import (
    upsert_catalog,
    upsert_data_source,
    upsert_fee_schedules,
    upsert_relationships,
)
from consistency_persistence.schema import (
    FeeScheduleRow,
    MarketRow,
    RelationshipReviewRow,
    RelationshipRow,
)
from consistency_persistence.store import PersistenceStore


async def load_reference(
    url: str,
    catalog: Catalog,
    relationships: list[Relationship],
    schedules: list[FeeSchedule],
    *,
    source_id: str,
    source_name: str,
    source_kind: str,
) -> dict[str, int]:
    store = PersistenceStore(url)
    try:
        async with store.sessions() as session, session.begin():
            await upsert_data_source(
                session, data_source_id=source_id, name=source_name, kind=source_kind
            )
            await upsert_catalog(session, catalog, source_id)
            await upsert_relationships(session, relationships)
            await upsert_fee_schedules(session, schedules)
        async with store.sessions() as session:
            markets = await session.scalar(select(func.count()).select_from(MarketRow))
            rels = await session.scalar(select(func.count()).select_from(RelationshipRow))
            reviews = await session.scalar(select(func.count()).select_from(RelationshipReviewRow))
            fees = await session.scalar(select(func.count()).select_from(FeeScheduleRow))
        return {
            "markets": int(markets or 0),
            "relationships": int(rels or 0),
            "reviews": int(reviews or 0),
            "fee_schedules": int(fees or 0),
        }
    finally:
        await store.aclose()

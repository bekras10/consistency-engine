"""Build a session's inputs (source, catalog, relationships, fees) from settings."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from consistency_connectors.base import MarketDataSource
from consistency_connectors.settings import DataSourceMode, Settings
from consistency_connectors.sources.kalshi import KalshiDataSource
from consistency_connectors.sources.replay import ReplayDataSource
from consistency_connectors.sources.synthetic import SyntheticDataSource
from consistency_core.fees import FeeCalculator, FeeScheduleRegistry
from consistency_core.models.market import Catalog
from consistency_core.models.relationship import Relationship
from consistency_core.relationships.discovery import discover
from consistency_core.relationships.review import apply_reviews, load_reviews
from consistency_core.serialization import sha256_of
from consistency_simulation.families import SIM_EPOCH


@dataclass
class SessionInputs:
    source: MarketDataSource
    catalog: Catalog
    relationships: list[Relationship]
    fees: FeeCalculator
    source_label: str
    deterministic: bool
    """Recorded/synthetic stream: the pipeline clock is the message receipt time and no wall
    clock ticks are injected, so the session is exactly reproducible."""
    fingerprint: str
    """Identity of the input stream + reference data (used to resume an interrupted session)."""


def relationships_for(
    catalog: Catalog, reviews_path: Path, *, as_of: datetime
) -> list[Relationship]:
    rels = discover(catalog, as_of=as_of)
    if reviews_path.exists():
        rels, _ = apply_reviews(rels, load_reviews(reviews_path), catalog, as_of=as_of)
    return rels


def fee_calculator(fee_dir: Path) -> FeeCalculator:
    return FeeCalculator(FeeScheduleRegistry.from_directory(fee_dir))


async def build_inputs(settings: Settings, *, fast: bool = False) -> SessionInputs:
    """``fast``: play recorded/synthetic streams without real-time pacing (tests, seeding)."""
    fees = fee_calculator(Path(settings.fee_schedule_dir))
    reviews = Path(settings.relationship_reviews_path)
    source: MarketDataSource
    match settings.data_source:
        case DataSourceMode.SYNTHETIC:
            src = SyntheticDataSource(
                settings.synthetic_preset,
                speed=None if fast else settings.synthetic_playback_speed,
            )
            source, label, as_of = src, f"synthetic:{settings.synthetic_preset}", SIM_EPOCH
            stream_id: object = {"preset": settings.synthetic_preset, "config": asdict(src.config)}
            deterministic = True
        case DataSourceMode.REPLAY:
            path = Path(settings.replay_dataset_path)
            rsrc = ReplayDataSource(path, speed=None if fast else settings.synthetic_playback_speed)
            source, label, as_of = rsrc, f"replay:{path.name}", SIM_EPOCH
            stream_id = {"dataset": path.name, "metadata": rsrc.metadata}
            deterministic = True
        case DataSourceMode.KALSHI_AUTHORIZED:
            # Guarded: every data method of this adapter refuses (no Kalshi network access).
            source, label, as_of = KalshiDataSource(settings), "kalshi", datetime.now(UTC)
            stream_id = {"live": "kalshi"}
            deterministic = False
    catalog = Catalog(
        series=tuple(await source.get_series()),
        events=tuple(await source.get_events()),
        markets=tuple(await source.get_markets()),
    )
    rels = relationships_for(catalog, reviews, as_of=as_of)
    fingerprint = sha256_of(
        {
            "stream": stream_id,
            "relationships": [r.model_dump(mode="json") for r in rels],
            "fee_versions": fees.schedule_versions(),
        }
    )
    return SessionInputs(source, catalog, rels, fees, label, deterministic, fingerprint)

"""Shared enums and small value types."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import AwareDatetime, BaseModel, ConfigDict


class FrozenModel(BaseModel):
    """Base for immutable, strictly-shaped domain models."""

    model_config = ConfigDict(frozen=True, extra="forbid", validate_default=True)


class Side(StrEnum):
    YES = "yes"
    NO = "no"

    @property
    def opposite(self) -> Side:
        return Side.NO if self is Side.YES else Side.YES


class MarketStatus(StrEnum):
    INITIALIZED = "initialized"
    OPEN = "open"
    PAUSED = "paused"
    CLOSED = "closed"
    SETTLED = "settled"
    REMOVED = "removed"


class OutcomeType(StrEnum):
    BINARY = "binary"


class SyncStatus(StrEnum):
    """Trust state of a locally reconstructed book."""

    AWAITING_SNAPSHOT = "awaiting_snapshot"
    SYNCHRONIZED = "synchronized"
    UNSYNCHRONIZED = "unsynchronized"


class HealthStatus(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class DataSourceKind(StrEnum):
    SYNTHETIC = "synthetic"
    REPLAY = "replay"
    KALSHI_AUTHORIZED = "kalshi_authorized"


class Provenance(FrozenModel):
    """Where a record came from. ``dataset_id``/``seed`` identify synthetic material."""

    source_kind: DataSourceKind
    source_id: str
    dataset_id: str | None = None
    generator_version: str | None = None
    seed: int | None = None
    retrieved_at: AwareDatetime | None = None


def require_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None or ts.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return ts

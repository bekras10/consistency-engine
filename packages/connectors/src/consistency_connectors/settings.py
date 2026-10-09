"""Environment-validated runtime settings and the data-access startup guard.

The guard is deliberately strict: the Kalshi adapter is refused unless *both*
``ENABLE_KALSHI_API`` and ``KALSHI_AUTHORIZATION_CONFIRMED`` are true, and any
configuration that enables live trading is refused outright (out of scope).
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DataSourceMode(StrEnum):
    SYNTHETIC = "synthetic"
    REPLAY = "replay"
    KALSHI_AUTHORIZED = "kalshi_authorized"


class ConfigurationError(RuntimeError):
    """Raised when the environment requests a forbidden or inconsistent mode."""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore", frozen=True)

    data_source: DataSourceMode = Field(default=DataSourceMode.SYNTHETIC, alias="DATA_SOURCE")
    enable_kalshi_api: bool = Field(default=False, alias="ENABLE_KALSHI_API")
    kalshi_authorization_confirmed: bool = Field(
        default=False, alias="KALSHI_AUTHORIZATION_CONFIRMED"
    )
    enable_live_trading: bool = Field(default=False, alias="ENABLE_LIVE_TRADING")
    replay_dataset_path: str = Field(default="fixtures/datasets/smoke", alias="REPLAY_DATASET_PATH")
    synthetic_seed: int = Field(default=20260115, alias="SYNTHETIC_SEED")
    synthetic_playback_speed: float = Field(default=1.0, alias="SYNTHETIC_PLAYBACK_SPEED")
    synthetic_preset: str = Field(default="inconsistent", alias="SYNTHETIC_PRESET")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # Reference data
    fee_schedule_dir: str = Field(default="fixtures/fees", alias="FEE_SCHEDULE_DIR")
    relationship_reviews_path: str = Field(
        default="fixtures/relationships/manual-reviews.yaml", alias="RELATIONSHIP_REVIEWS_PATH"
    )

    # Persistence (PostgreSQL via asyncpg). Unset -> the worker runs without persistence and
    # says so in its logs and health; it never substitutes another database.
    database_url: str | None = Field(default=None, alias="DATABASE_URL")
    persist_market_data: bool = Field(default=True, alias="PERSIST_MARKET_DATA")
    """Record the session journal (snapshots/updates) for synthetic and replay sources."""
    third_party_raw_persistence_authorized: bool = Field(
        default=False, alias="THIRD_PARTY_RAW_PERSISTENCE_AUTHORIZED"
    )
    """Gate for storing raw third-party (e.g. Kalshi) market data; see docs/compliance.md."""
    retention_max_age_hours: int = Field(default=168, alias="RETENTION_MAX_AGE_HOURS")
    retention_max_sessions: int = Field(default=20, alias="RETENTION_MAX_SESSIONS")
    retention_third_party_hours: int = Field(default=0, alias="RETENTION_THIRD_PARTY_HOURS")
    retention_interval_s: int = Field(default=3600, alias="RETENTION_INTERVAL_S")

    # Pipeline
    sweep_interval_ms: int = Field(default=100, alias="SWEEP_INTERVAL_MS")
    checkpoint_every: int = Field(default=2000, alias="CHECKPOINT_EVERY")
    journal_batch_size: int = Field(default=500, alias="JOURNAL_BATCH_SIZE")
    outbox_maxsize: int = Field(default=1024, alias="OUTBOX_MAXSIZE")
    update_queue_maxsize: int = Field(default=10_000, alias="UPDATE_QUEUE_MAXSIZE")
    inbound_queue_maxsize: int = Field(default=10_000, alias="INBOUND_QUEUE_MAXSIZE")
    heartbeat_timeout_s: float = Field(default=10.0, alias="HEARTBEAT_TIMEOUT_S")
    supervisor_max_restarts: int = Field(default=8, alias="SUPERVISOR_MAX_RESTARTS")
    supervisor_backoff_base_s: float = Field(default=0.5, alias="SUPERVISOR_BACKOFF_BASE_S")
    supervisor_backoff_max_s: float = Field(default=30.0, alias="SUPERVISOR_BACKOFF_MAX_S")

    @model_validator(mode="after")
    def _guard(self) -> Settings:
        if self.enable_live_trading:
            raise ConfigurationError(
                "ENABLE_LIVE_TRADING=true is refused: live order placement is out of scope."
            )
        if self.data_source is DataSourceMode.KALSHI_AUTHORIZED and not self.kalshi_access_allowed:
            raise ConfigurationError(
                "DATA_SOURCE=kalshi_authorized requires ENABLE_KALSHI_API=true and "
                "KALSHI_AUTHORIZATION_CONFIRMED=true (operator must hold authorization)."
            )
        if self.synthetic_playback_speed <= 0:
            raise ConfigurationError("SYNTHETIC_PLAYBACK_SPEED must be positive.")
        for name in (
            "sweep_interval_ms",
            "checkpoint_every",
            "journal_batch_size",
            "outbox_maxsize",
            "update_queue_maxsize",
            "inbound_queue_maxsize",
        ):
            if getattr(self, name) <= 0:
                raise ConfigurationError(f"{name.upper()} must be positive.")
        if min(self.retention_max_age_hours, self.retention_max_sessions) < 0:
            raise ConfigurationError("retention limits must be >= 0.")
        return self

    @property
    def kalshi_access_allowed(self) -> bool:
        return self.enable_kalshi_api and self.kalshi_authorization_confirmed

    def redacted(self) -> dict[str, str]:
        """Configuration summary safe to expose via health endpoints (no secrets held here)."""
        return {
            "data_source": self.data_source.value,
            "enable_kalshi_api": str(self.enable_kalshi_api).lower(),
            "kalshi_authorization_confirmed": str(self.kalshi_authorization_confirmed).lower(),
            "enable_live_trading": str(self.enable_live_trading).lower(),
        }

    def persistence_summary(self) -> dict[str, str]:
        return {
            "persistence": "postgresql" if self.database_url else "disabled",
            "raw_market_data_persisted": str(self.raw_market_data_persisted).lower(),
        }

    @property
    def is_third_party_source(self) -> bool:
        return self.data_source is DataSourceMode.KALSHI_AUTHORIZED

    @property
    def raw_market_data_persisted(self) -> bool:
        """Journal recording gate: on for synthetic/replay (configurable), and for third-party
        sources only when explicitly authorized (default off; docs/compliance.md)."""
        if not self.persist_market_data:
            return False
        if self.is_third_party_source:
            return self.third_party_raw_persistence_authorized
        return True

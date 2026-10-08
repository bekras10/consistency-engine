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
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

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

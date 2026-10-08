"""Series, events and markets (exchange-agnostic)."""

from __future__ import annotations

import hashlib
import json

from pydantic import AwareDatetime, Field, JsonValue, model_validator

from consistency_core.models.common import (
    FrozenModel,
    MarketStatus,
    OutcomeType,
    Provenance,
)
from consistency_core.models.settlement import SettlementSpec
from consistency_core.money import ONE, QUANTITY_DP, ZERO, Dec, decimal_places
from consistency_core.ticks import PriceGrid


class Series(FrozenModel):
    series_id: str
    title: str
    category: str
    provenance: Provenance


class Event(FrozenModel):
    event_id: str
    series_id: str
    title: str
    mutually_exclusive: bool | None = None
    """Exchange/rules assertion that at most one market in the event settles YES."""
    outcome_set_complete: bool | None = None
    """Evidence that the listed markets cover every admissible outcome (e.g. a catch-all)."""
    outcome_universe: tuple[str, ...] | None = None
    """Declared outcome identifiers when the event is categorical."""
    provenance: Provenance


class MarketRules(FrozenModel):
    market_id: str
    rules_version: str
    rules_text: str
    settlement: SettlementSpec

    @property
    def rules_hash(self) -> str:
        return compute_rules_hash(self.rules_text, self.settlement)


def compute_rules_hash(rules_text: str, settlement: SettlementSpec) -> str:
    payload = {"rules_text": rules_text, "settlement": settlement.model_dump(mode="json")}
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()


class Market(FrozenModel):
    market_id: str
    ticker: str
    event_id: str
    series_id: str
    title: str
    description: str = ""
    status: MarketStatus
    open_time: AwareDatetime
    close_time: AwareDatetime
    expiration_time: AwareDatetime
    settlement_source: str
    settlement_rules: str
    rules_version: str
    outcome_type: OutcomeType = OutcomeType.BINARY
    price_grid: PriceGrid
    quantity_increment: Dec = ONE
    settlement: SettlementSpec = Field(default_factory=SettlementSpec)
    exchange_metadata: dict[str, JsonValue] = Field(default_factory=dict)
    provenance: Provenance

    @model_validator(mode="after")
    def _check(self) -> Market:
        if not (self.open_time <= self.close_time <= self.expiration_time):
            raise ValueError("require open_time <= close_time <= expiration_time")
        if self.quantity_increment <= ZERO or decimal_places(self.quantity_increment) > (
            QUANTITY_DP
        ):
            raise ValueError("quantity_increment must be positive with <= 2 decimal places")
        if self.settlement.settlement_source not in (None, self.settlement_source):
            raise ValueError("structured settlement source disagrees with settlement_source")
        return self

    @property
    def rules(self) -> MarketRules:
        return MarketRules(
            market_id=self.market_id,
            rules_version=self.rules_version,
            rules_text=self.settlement_rules,
            settlement=self.settlement,
        )

    @property
    def rules_hash(self) -> str:
        return compute_rules_hash(self.settlement_rules, self.settlement)

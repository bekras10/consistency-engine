"""JSON payloads for broadcast (Decimals as fixed-point strings, never JSON numbers)."""

from __future__ import annotations

from typing import Any

from consistency_connectors.ingestion import BookManager, BookUpdate
from consistency_core.models.common import Side
from consistency_core.money import dec_str
from consistency_pipeline.lifecycle import DetectionEvent

DETECTION_TOPIC = "detection"
MARKET_TOPIC = "market"
SYSTEM_TOPIC = "system"


def detection_event_payload(ev: DetectionEvent) -> dict[str, Any]:
    rec = ev.record
    return {
        "event": ev.kind.value,
        "seq": ev.seq,
        "at_ms": ev.at_ms,
        "position": ev.position,
        "event_classification": None if ev.classification is None else ev.classification.value,
        "event_reason_codes": list(ev.reason_codes),
        "event_certificate_hash": ev.certificate_hash,
        "detection": rec.model_dump(mode="json"),
    }


def market_update_payload(update: BookUpdate, manager: BookManager) -> dict[str, Any]:
    out: dict[str, Any] = {
        "market_id": update.market_id,
        "kind": update.kind,
        "sync_status": update.sync_status.value,
        "reason": update.reason,
        "interrupted": update.interrupted,
        "position": update.position,
        "received_ts_ms": update.timing.received_ts_ms,
    }
    if manager.has_market(update.market_id):
        book = manager.book(update.market_id)
        for side in (Side.YES, Side.NO):
            bid = book.best_bid(side)
            ask = book.best_ask(side)
            out[f"best_{side.value}_bid"] = None if bid is None else dec_str(bid)
            out[f"best_{side.value}_ask"] = None if ask is None else dec_str(ask)
    return out

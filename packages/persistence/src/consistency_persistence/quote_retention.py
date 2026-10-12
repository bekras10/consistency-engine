"""Derived evidence kept when raw third-party books are not authorized.

A proof certificate is built from the live book. Storing that certificate unchanged
stores the ask curve. When raw retention is not authorized, the stored certificate
drops the curve and the consumed-level ladder. The fields that remain are still
third-party quote data. They are listed in ``RETAINED_QUOTE_FIELDS``. Bit-exact
replay needs the journal, which stays empty unless raw persistence was authorized.
"""

from __future__ import annotations

import hashlib

from consistency_core.pricing.certificate import ProofCertificate
from consistency_core.serialization import canonical_json
from consistency_pipeline.lifecycle import DetectionEvent, DetectionRecord

DERIVED_EVIDENCE_NOTE = (
    "Derived evidence profile: full ask curves (books.asks) and consumed level "
    "ladders (evaluation.legs.walk.levels) are omitted because raw order-book "
    "retention is not authorized. Retained third-party quote fields are best ask "
    "prices (top_of_book.best_asks), book content hashes, and walk aggregates "
    "(premium, vwap, marginal price, filled quantity). These are quote data. "
    "Bit-exact historical replay is unavailable."
)

RETAINED_QUOTE_FIELDS: tuple[str, ...] = (
    "top_of_book.best_asks",
    "books.book_hash",
    "books.exchange_ts_ms",
    "books.received_ts_ms",
    "evaluation.legs.walk.total_premium",
    "evaluation.legs.walk.vwap",
    "evaluation.legs.walk.marginal_price",
    "evaluation.legs.walk.filled_quantity",
)

OMITTED_QUOTE_FIELDS: tuple[str, ...] = (
    "books.asks",
    "evaluation.legs.walk.levels",
)


def redact_certificate_document(document: str) -> tuple[str, str]:
    """Return canonical JSON and its ``sha256:`` digest with ladders removed."""
    cert = ProofCertificate.model_validate_json(document)
    books = tuple(book.model_copy(update={"asks": ()}) for book in cert.books)
    evaluation = cert.evaluation
    if evaluation is not None:
        legs = tuple(
            leg.model_copy(update={"walk": leg.walk.model_copy(update={"levels": ()})})
            for leg in evaluation.legs
        )
        evaluation = evaluation.model_copy(update={"legs": legs})
    notes = cert.notes
    if DERIVED_EVIDENCE_NOTE not in notes:
        notes = (*notes, DERIVED_EVIDENCE_NOTE)
    redacted = cert.model_copy(update={"books": books, "evaluation": evaluation, "notes": notes})
    text = canonical_json(redacted)
    digest = "sha256:" + hashlib.sha256(text.encode()).hexdigest()
    return text, digest


def redact_detection_event(event: DetectionEvent) -> DetectionEvent:
    """Copy one lifecycle event so the stored proof matches the redacted bytes."""
    record = _redact_record(event.record)
    event_hash = event.certificate_hash
    if event.certificate_hash == event.record.certificate_hash:
        event_hash = record.certificate_hash
    elif event.certificate_hash == event.record.closing_certificate_hash:
        event_hash = record.closing_certificate_hash
    return event.model_copy(update={"record": record, "certificate_hash": event_hash})


def _redact_pair(document: str | None, digest: str | None) -> tuple[str | None, str | None]:
    if document is None:
        return None, digest
    text, hashed = redact_certificate_document(document)
    return text, hashed


def _redact_record(record: DetectionRecord) -> DetectionRecord:
    certificate_json, certificate_hash = _redact_pair(
        record.certificate_json, record.certificate_hash
    )
    closing_json, closing_hash = _redact_pair(
        record.closing_certificate_json, record.closing_certificate_hash
    )
    return record.model_copy(
        update={
            "certificate_json": certificate_json,
            "certificate_hash": certificate_hash,
            "closing_certificate_json": closing_json,
            "closing_certificate_hash": closing_hash,
        }
    )

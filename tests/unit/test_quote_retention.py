"""Certificates drop ask ladders when raw retention is off, and say what remains."""

from __future__ import annotations

import hashlib

from consistency_core.pricing.certificate import ProofCertificate
from consistency_persistence.quote_retention import (
    DERIVED_EVIDENCE_NOTE,
    OMITTED_QUOTE_FIELDS,
    RETAINED_QUOTE_FIELDS,
    redact_certificate_document,
)
from tests.unit.test_detection_pipeline import Harness


def test_redaction_removes_ladders_and_documents_remaining_quotes() -> None:
    harness = Harness()
    harness.open_books()
    events = harness.heartbeats(30)
    assert events
    original = events[0].record.certificate_json
    assert original is not None
    before = ProofCertificate.model_validate_json(original)
    assert before.books
    assert before.books[0].asks
    text, digest = redact_certificate_document(original)
    after = ProofCertificate.model_validate_json(text)
    assert after.books[0].asks == ()
    assert after.top_of_book is not None
    assert after.top_of_book.best_asks
    assert after.evaluation is not None
    assert all(leg.walk.levels == () for leg in after.evaluation.legs)
    assert DERIVED_EVIDENCE_NOTE in after.notes
    assert digest == "sha256:" + hashlib.sha256(text.encode()).hexdigest()
    assert "books.asks" in OMITTED_QUOTE_FIELDS
    assert "top_of_book.best_asks" in RETAINED_QUOTE_FIELDS
    stored_levels = [level for book in after.books for level in book.asks]
    assert stored_levels == []

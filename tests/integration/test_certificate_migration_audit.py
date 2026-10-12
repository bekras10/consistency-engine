"""Upgrade must repair or quarantine detections written by the old close path.

Revision 0005 only adds closing-certificate columns. A row from the broken writer
has the qualifying hash and a later evaluation's JSON. After upgrade to head,
a verified checkpoint must restore that qualifying proof, or the row must be
marked uncertified. The row is not deleted, and a hash is not rewritten to match
the wrong document.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from consistency_persistence.db import make_engine
from consistency_pipeline.checkpoint import state_digest
from tests.integration.test_certificate_close_integrity import _close_case, _digest
from tests.integration.test_schema import _alembic

pytestmark = pytest.mark.integration


def _checkpoint(
    session_id: str, slots: dict[str, object], *, digest: str | None = None
) -> tuple[str, str]:
    manager: dict[str, object] = {}
    engine = {"slots": slots}
    real = state_digest(manager, engine)
    used = real if digest is None else digest
    payload = {
        "session_id": session_id,
        "ordinal": 1,
        "now_ms": 1,
        "source_position": 1,
        "manager_state": manager,
        "engine_state": engine,
        "state_digest": used,
    }
    return used, json.dumps(payload)


async def test_upgrade_recovers_or_quarantines_pre_0005_mismatched_certificate(db: str) -> None:
    _harness, closed = _close_case("EXPIRED")
    assert closed.evaluation is not None
    qualifying = closed.record.certificate_json
    assert qualifying is not None
    qualifying_hash = closed.record.certificate_hash
    closing = closed.evaluation.certificate_json()
    assert _digest(qualifying) == qualifying_hash
    assert _digest(closing) != qualifying_hash
    fake_hash = "sha256:" + "ab" * 32
    assert fake_hash != qualifying_hash
    assert _digest(closing) != fake_hash

    _alembic(db, "downgrade", "0004_outbox_retention")
    try:
        now = datetime.now(UTC)
        session_id = "ses-audit-pre005"
        recover_slots = {
            "recover": {
                "detection": {
                    "detection_id": "det-audit-recover",
                    "certificate_json": qualifying,
                    "certificate_hash": qualifying_hash,
                }
            },
            "rehash": {
                "detection": {
                    "detection_id": "det-audit-rehash",
                    "certificate_json": qualifying,
                    "certificate_hash": qualifying_hash,
                }
            },
        }
        good_digest, good_payload = _checkpoint(session_id, recover_slots)
        _bad_digest, bad_payload = _checkpoint(
            session_id,
            {
                "quarantine": {
                    "detection": {
                        "detection_id": "det-audit-quarantine",
                        "certificate_json": qualifying,
                    }
                }
            },
            digest="sha256:" + "00" * 32,
        )
        engine = make_engine(db)
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO data_sources (data_source_id, name, kind, created_at)
                    VALUES ('src-audit', 'audit', 'synthetic', :now)
                    """
                ),
                {"now": now},
            )
            await conn.execute(
                text(
                    """
                    INSERT INTO ingestion_sessions (
                        session_id, data_source_id, status, fingerprint, source_label,
                        deterministic, pinned, raw_persisted, started_at
                    ) VALUES (
                        :sid, 'src-audit', 'completed', 'sha256:audit', 'synthetic:audit',
                        true, false, true, :now
                    )
                    """
                ),
                {"sid": session_id, "now": now},
            )
            rows = [
                ("det-audit-recover", qualifying_hash, closing, 1),
                ("det-audit-rehash", fake_hash, closing, 2),
                ("det-audit-quarantine", qualifying_hash, closing, 3),
                ("det-audit-intact", qualifying_hash, qualifying, 4),
            ]
            for detection_id, cert_hash, cert_json, ordinal in rows:
                await conn.execute(
                    text(
                        """
                        INSERT INTO detections (
                            detection_id, session_id, relationship_id, strategy_id, template,
                            ordinal, status, classification, peak_classification, reason_codes,
                            first_observed_at, last_observed_at, first_observed_ms, last_observed_ms,
                            first_position, last_position, event_count, certificate_hash,
                            certificate_json, record_json
                        ) VALUES (
                            :detection_id, :sid, 'rel-audit', 'strat-audit', 'implication',
                            :ordinal, 'EXPIRED', 'FEE_ADJUSTED_CANDIDATE', 'FEE_ADJUSTED_CANDIDATE',
                            CAST('[]' AS jsonb),
                            :now, :now, 1, 2, 1, 2, 2, :cert_hash, :cert_json,
                            CAST(:record_json AS jsonb)
                        )
                        """
                    ),
                    {
                        "detection_id": detection_id,
                        "sid": session_id,
                        "ordinal": ordinal,
                        "now": now,
                        "cert_hash": cert_hash,
                        "cert_json": cert_json,
                        "record_json": json.dumps({"detection_id": detection_id}),
                    },
                )
            await conn.execute(
                text(
                    """
                    INSERT INTO session_checkpoints (
                        session_id, ordinal, now_ms, source_position, state_digest,
                        checkpoint_json, created_at
                    ) VALUES (
                        :sid, 1, 1, 1, :digest, CAST(:payload AS jsonb), :now
                    )
                    """
                ),
                {"sid": session_id, "digest": good_digest, "payload": good_payload, "now": now},
            )
            await conn.execute(
                text(
                    """
                    INSERT INTO session_checkpoints (
                        session_id, ordinal, now_ms, source_position, state_digest,
                        checkpoint_json, created_at
                    ) VALUES (
                        :sid, 2, 2, 2, :digest, CAST(:payload AS jsonb), :now
                    )
                    """
                ),
                {
                    "sid": session_id,
                    "digest": "sha256:" + "00" * 32,
                    "payload": bad_payload,
                    "now": now,
                },
            )
        await engine.dispose()

        _alembic(db, "upgrade", "head")

        engine = make_engine(db)
        async with engine.connect() as conn:
            found = (
                (
                    await conn.execute(
                        text(
                            """
                        SELECT detection_id, certificate_hash, certificate_json,
                               closing_certificate_hash, closing_certificate_json,
                               proof_status
                        FROM detections
                        WHERE session_id = :sid
                        ORDER BY detection_id
                        """
                        ),
                        {"sid": session_id},
                    )
                )
                .mappings()
                .all()
            )
        await engine.dispose()
        by_id = {row["detection_id"]: row for row in found}
        assert set(by_id) == {
            "det-audit-intact",
            "det-audit-quarantine",
            "det-audit-recover",
            "det-audit-rehash",
        }

        intact = by_id["det-audit-intact"]
        assert intact["certificate_json"] == qualifying
        assert intact["certificate_hash"] == qualifying_hash
        assert _digest(intact["certificate_json"]) == intact["certificate_hash"]
        assert intact["proof_status"] == "certified"

        recovered = by_id["det-audit-recover"]
        assert recovered["certificate_json"] == qualifying
        assert recovered["certificate_hash"] == _digest(recovered["certificate_json"])
        assert recovered["certificate_hash"] == qualifying_hash
        assert recovered["proof_status"] == "certified"
        assert recovered["closing_certificate_json"] == closing
        assert recovered["closing_certificate_hash"] == _digest(closing)
        assert recovered["certificate_json"] != recovered["closing_certificate_json"]

        rehashed = by_id["det-audit-rehash"]
        assert rehashed["certificate_json"] == qualifying
        assert rehashed["certificate_hash"] == _digest(qualifying)
        assert rehashed["certificate_hash"] != fake_hash
        assert rehashed["certificate_hash"] != _digest(closing)
        assert rehashed["proof_status"] == "certified"

        quarantined = by_id["det-audit-quarantine"]
        assert quarantined["proof_status"] == "uncertified"
        assert quarantined["certificate_hash"] == qualifying_hash
        assert quarantined["certificate_json"] == closing
        assert _digest(quarantined["certificate_json"]) != quarantined["certificate_hash"]
    finally:
        _alembic(db, "upgrade", "head")

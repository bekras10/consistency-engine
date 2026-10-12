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
from sqlalchemy import event, text

from consistency_persistence.certificate_audit import audit_stored_proofs
from consistency_persistence.db import make_engine
from consistency_persistence.recording import BatchJournal
from consistency_persistence.store import _versions
from consistency_pipeline.checkpoint import state_digest
from consistency_pipeline.lifecycle import EventKind
from consistency_worker.bootstrap import fee_calculator
from tests.conftest import FIXTURES
from tests.integration.test_certificate_close_integrity import _close_case, _digest, _persist
from tests.integration.test_postgres_pipeline import _with_reference
from tests.integration.test_schema import _alembic
from tests.unit.test_detection_pipeline import Harness, _candidate

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
        # The stored hash matches no checkpoint document. A different checkpoint
        # whose own bytes hash cleanly is not the original observation.
        assert rehashed["proof_status"] == "uncertified"
        assert rehashed["certificate_hash"] == fake_hash
        assert rehashed["certificate_json"] == closing
        assert rehashed["certificate_json"] != qualifying
        assert _digest(rehashed["certificate_json"]) != rehashed["certificate_hash"]
        assert rehashed["closing_certificate_json"] is None
        assert rehashed["closing_certificate_hash"] is None

        quarantined = by_id["det-audit-quarantine"]
        assert quarantined["proof_status"] == "uncertified"
        assert quarantined["certificate_hash"] == qualifying_hash
        assert quarantined["certificate_json"] == closing
        assert _digest(quarantined["certificate_json"]) != quarantined["certificate_hash"]
    finally:
        _alembic(db, "upgrade", "head")


def _run_audit(sync_conn: object) -> None:
    audit_stored_proofs(sync_conn)  # type: ignore[arg-type]


async def _rows(db: str, session_id: str) -> dict[str, dict[str, object]]:
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
    return {str(row["detection_id"]): dict(row) for row in found}


async def test_earlier_checkpoint_is_not_certified_as_the_final_proof(db: str) -> None:
    """Two later quotes exist. The earlier checkpoint is not the final proof.

    The stored hash is the final qualifying certificate. Neither checkpoint
    document is that certificate. The audit must not adopt the earlier quote,
    or the newer non-matching quote, and must not rewrite the stored hash.
    """
    harness = Harness()
    _candidate(harness)
    opened = next(event for event in harness.events if event.kind is EventKind.OPENED)
    updated = next(event for event in harness.events if event.kind is EventKind.UPDATED)
    assert opened.record.certificate_json is not None
    assert updated.record.certificate_json is not None
    early = opened.record.certificate_json
    final = updated.record.certificate_json
    final_hash = updated.record.certificate_hash
    assert early != final
    assert _digest(early) != final_hash
    assert _digest(final) == final_hash
    _harness, closed = _close_case("EXPIRED")
    assert closed.evaluation is not None
    newer = closed.evaluation.certificate_json()
    assert _digest(newer) != final_hash
    assert newer != early

    now = datetime.now(UTC)
    session_id = "ses-audit-earlier"
    early_digest, early_payload = _checkpoint(
        session_id,
        {
            "early": {
                "detection": {
                    "detection_id": "det-audit-earlier",
                    "certificate_json": early,
                    "certificate_hash": _digest(early),
                }
            }
        },
    )
    newer_digest, newer_payload = _checkpoint(
        session_id,
        {
            "newer": {
                "detection": {
                    "detection_id": "det-audit-earlier",
                    "certificate_json": newer,
                    "certificate_hash": _digest(newer),
                }
            }
        },
    )
    engine = make_engine(db)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO data_sources (data_source_id, name, kind, created_at)
                VALUES ('src-earlier', 'earlier', 'synthetic', :now)
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
                    :sid, 'src-earlier', 'completed', 'sha256:earlier', 'synthetic:earlier',
                    true, false, true, :now
                )
                """
            ),
            {"sid": session_id, "now": now},
        )
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
                    'det-audit-earlier', :sid, 'rel-audit', 'strat-audit', 'implication',
                    1, 'UPDATED', 'FEE_ADJUSTED_CANDIDATE', 'FEE_ADJUSTED_CANDIDATE',
                    CAST('[]' AS jsonb),
                    :now, :now, 1, 2, 1, 2, 2, :cert_hash, :cert_json,
                    CAST(:record_json AS jsonb)
                )
                """
            ),
            {
                "sid": session_id,
                "now": now,
                "cert_hash": final_hash,
                "cert_json": newer,
                "record_json": json.dumps(
                    {
                        "detection_id": "det-audit-earlier",
                        "classification": "FEE_ADJUSTED_CANDIDATE",
                    }
                ),
            },
        )
        await conn.execute(
            text(
                """
                INSERT INTO session_checkpoints (
                    session_id, ordinal, now_ms, source_position, state_digest,
                    checkpoint_json, created_at
                ) VALUES
                    (:sid, 1, 1, 1, :early_digest, CAST(:early_payload AS jsonb), :now),
                    (:sid, 2, 2, 2, :newer_digest, CAST(:newer_payload AS jsonb), :now)
                """
            ),
            {
                "sid": session_id,
                "now": now,
                "early_digest": early_digest,
                "early_payload": early_payload,
                "newer_digest": newer_digest,
                "newer_payload": newer_payload,
            },
        )
    await engine.dispose()

    engine = make_engine(db)
    async with engine.begin() as conn:
        await conn.run_sync(_run_audit)
    await engine.dispose()

    row = (await _rows(db, session_id))["det-audit-earlier"]
    assert row["proof_status"] == "uncertified"
    assert row["certificate_hash"] == final_hash
    assert row["certificate_json"] == newer
    assert row["certificate_json"] != early
    assert row["certificate_json"] != final
    assert _digest(str(row["certificate_json"])) != row["certificate_hash"]


async def test_journal_replay_restores_the_final_quote_not_the_earlier_checkpoint(
    db: str,
) -> None:
    harness = Harness()
    _candidate(harness)
    opened = next(event for event in harness.events if event.kind is EventKind.OPENED)
    updated = next(event for event in harness.events if event.kind is EventKind.UPDATED)
    assert opened.record.certificate_json is not None
    assert updated.record.certificate_json is not None
    early = opened.record.certificate_json
    store = await _persist(db, "replay-link", harness.events)
    session_id = "ses-cert-replay-link"
    try:
        rows = await store.list_detection_rows(session_id)
        assert len(rows) == 1
        stored = rows[0]
        final = stored.certificate_json
        final_hash = stored.certificate_hash
        assert final is not None
        assert final == updated.record.certificate_json
        assert early != final
        assert _digest(early) != final_hash
        fees = fee_calculator(FIXTURES / "fees")
        versions = _versions(_with_reference(harness.catalog), list(harness.rels), fees)
        batch = BatchJournal(
            store.sessions,
            session_id,
            versions,
            batch_size=500,
            enabled=True,
        )
        for entry in harness.journal:
            batch.append(entry)
        await batch.flush()
        early_digest, early_payload = _checkpoint(
            session_id,
            {
                "early": {
                    "detection": {
                        "detection_id": stored.detection_id,
                        "certificate_json": early,
                        "certificate_hash": _digest(early),
                    }
                }
            },
        )
        now = datetime.now(UTC)
        async with store.sessions() as session, session.begin():
            await session.execute(
                text(
                    """
                    UPDATE detections
                    SET certificate_json = :early
                    WHERE detection_id = :detection_id
                    """
                ),
                {"early": early, "detection_id": stored.detection_id},
            )
            await session.execute(
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
                {
                    "sid": session_id,
                    "digest": early_digest,
                    "payload": early_payload,
                    "now": now,
                },
            )
        engine = make_engine(db)
        async with engine.begin() as conn:
            await conn.run_sync(_run_audit)
        await engine.dispose()
        repaired = (await _rows(db, session_id))[stored.detection_id]
        assert repaired["proof_status"] == "certified"
        assert repaired["certificate_hash"] == final_hash
        assert repaired["certificate_json"] == final
        assert repaired["certificate_json"] != early
        assert _digest(str(repaired["certificate_json"])) == repaired["certificate_hash"]
    finally:
        await store.aclose()


async def test_unrelated_checkpoints_are_not_loaded_to_repair_one_detection(db: str) -> None:
    _harness, closed = _close_case("EXPIRED")
    assert closed.record.certificate_json is not None
    qualifying = closed.record.certificate_json
    qualifying_hash = closed.record.certificate_hash
    closing = closed.evaluation.certificate_json() if closed.evaluation is not None else ""
    assert closing
    now = datetime.now(UTC)
    good_session = "ses-audit-one"
    other_session = "ses-audit-bulk"
    digest, payload = _checkpoint(
        good_session,
        {
            "one": {
                "detection": {
                    "detection_id": "det-audit-one",
                    "certificate_json": qualifying,
                    "certificate_hash": qualifying_hash,
                }
            }
        },
    )
    engine = make_engine(db)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO data_sources (data_source_id, name, kind, created_at)
                VALUES ('src-one', 'one', 'synthetic', :now)
                """
            ),
            {"now": now},
        )
        for session_id in (good_session, other_session):
            await conn.execute(
                text(
                    """
                    INSERT INTO ingestion_sessions (
                        session_id, data_source_id, status, fingerprint, source_label,
                        deterministic, pinned, raw_persisted, started_at
                    ) VALUES (
                        :sid, 'src-one', 'completed', :fingerprint, 'synthetic:one',
                        true, false, true, :now
                    )
                    """
                ),
                {"sid": session_id, "fingerprint": f"sha256:{session_id}", "now": now},
            )
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
                    'det-audit-one', :sid, 'rel-audit', 'strat-audit', 'implication',
                    1, 'EXPIRED', 'FEE_ADJUSTED_CANDIDATE', 'FEE_ADJUSTED_CANDIDATE',
                    CAST('[]' AS jsonb),
                    :now, :now, 1, 2, 1, 2, 2, :cert_hash, :cert_json,
                    CAST(:record_json AS jsonb)
                )
                """
            ),
            {
                "sid": good_session,
                "now": now,
                "cert_hash": qualifying_hash,
                "cert_json": closing,
                "record_json": json.dumps({"detection_id": "det-audit-one"}),
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
            {"sid": good_session, "digest": digest, "payload": payload, "now": now},
        )
        bulk = json.dumps({"pad": "x" * 4000, "manager_state": {}, "engine_state": {"slots": {}}})
        for ordinal in range(1, 41):
            await conn.execute(
                text(
                    """
                    INSERT INTO session_checkpoints (
                        session_id, ordinal, now_ms, source_position, state_digest,
                        checkpoint_json, created_at
                    ) VALUES (
                        :sid, :ordinal, :now_ms, :position, :digest,
                        CAST(:payload AS jsonb), :now
                    )
                    """
                ),
                {
                    "sid": other_session,
                    "ordinal": ordinal,
                    "now_ms": ordinal,
                    "position": ordinal,
                    "digest": "sha256:" + "11" * 32,
                    "payload": bulk,
                    "now": now,
                },
            )

    captured: list[tuple[str, object]] = []

    def _capture(sync_conn: object) -> None:
        def receive(
            _connection: object,
            _cursor: object,
            statement: str,
            parameters: object,
            _context: object,
            _executemany: bool,
        ) -> None:
            if "session_checkpoints" in statement.lower():
                captured.append((statement, parameters))

        event.listen(sync_conn, "before_cursor_execute", receive)
        try:
            audit_stored_proofs(sync_conn)  # type: ignore[arg-type]
        finally:
            event.remove(sync_conn, "before_cursor_execute", receive)

    async with engine.begin() as conn:
        await conn.run_sync(_capture)
    await engine.dispose()

    assert captured, "repair did not read session_checkpoints"
    for statement, parameters in captured:
        folded = statement.lower()
        assert "session_id" in folded
        assert "limit" in folded
        blob = repr(parameters)
        assert good_session in blob
        assert other_session not in blob
    repaired = (await _rows(db, good_session))["det-audit-one"]
    assert repaired["proof_status"] == "certified"
    assert repaired["certificate_json"] == qualifying
    assert repaired["certificate_hash"] == qualifying_hash


async def test_one_detection_failure_does_not_undo_an_earlier_certified_repair(
    db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _harness, closed = _close_case("EXPIRED")
    assert closed.record.certificate_json is not None
    assert closed.evaluation is not None
    qualifying = closed.record.certificate_json
    qualifying_hash = closed.record.certificate_hash
    closing = closed.evaluation.certificate_json()
    now = datetime.now(UTC)
    session_id = "ses-audit-isolate"
    digest, payload = _checkpoint(
        session_id,
        {
            "keep": {
                "detection": {
                    "detection_id": "det-audit-a-keep",
                    "certificate_json": qualifying,
                    "certificate_hash": qualifying_hash,
                }
            },
            "fail": {
                "detection": {
                    "detection_id": "det-audit-b-fail",
                    "certificate_json": qualifying,
                    "certificate_hash": qualifying_hash,
                }
            },
        },
    )
    engine = make_engine(db)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO data_sources (data_source_id, name, kind, created_at)
                VALUES ('src-isolate', 'isolate', 'synthetic', :now)
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
                    :sid, 'src-isolate', 'completed', 'sha256:isolate', 'synthetic:isolate',
                    true, false, true, :now
                )
                """
            ),
            {"sid": session_id, "now": now},
        )
        for detection_id, ordinal in (("det-audit-a-keep", 1), ("det-audit-b-fail", 2)):
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
                    "cert_hash": qualifying_hash,
                    "cert_json": closing,
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
            {"sid": session_id, "digest": digest, "payload": payload, "now": now},
        )
    await engine.dispose()

    from consistency_persistence import certificate_audit

    real = certificate_audit._projection

    def _boom(connection: object, detection_id: str, certificate_json: str) -> None:
        if detection_id == "det-audit-b-fail":
            raise RuntimeError("injected repair failure")
        real(connection, detection_id, certificate_json)  # type: ignore[arg-type]

    monkeypatch.setattr(certificate_audit, "_projection", _boom)
    engine = make_engine(db)
    async with engine.begin() as conn:
        await conn.run_sync(_run_audit)
    await engine.dispose()
    found = await _rows(db, session_id)
    kept = found["det-audit-a-keep"]
    failed = found["det-audit-b-fail"]
    assert kept["proof_status"] == "certified"
    assert kept["certificate_json"] == qualifying
    assert kept["certificate_hash"] == qualifying_hash
    assert failed["proof_status"] == "uncertified"
    assert failed["certificate_hash"] == qualifying_hash
    assert failed["certificate_json"] == closing

"""Repair or quarantine certificate rows left by the pre-0005 writer.

That writer kept the qualifying hash and stored a later evaluation's JSON in
``certificate_json``. This audit does not delete rows and does not rewrite a
stored hash so that it matches a different observation.

A document is certified only when there is a defensible link to the original
qualifying observation:

* SHA-256 of the reconstructed certificate equals the hash already stored.
  The hash is not replaced with the hash of some other checkpoint.
* The certificate's classification equals the detection row's classification.
* When ``record_json`` carries quoted metrics, those figures equal the
  certificate. When legs or scenarios are stored, they equal the certificate.
  Absent projection rows are not a contradiction: the stored hash already
  commits to the certificate bytes. A different document is never substituted
  to fill that gap.
* The preferred reconstruction is a deterministic replay of that session's
  journal. A verified checkpoint is used only when its document passes the
  same link. If neither source links, ``proof_status`` is ``uncertified`` and
  the original hash and JSON stay where they are.

Each affected detection is repaired inside its own savepoint. A failure rolls
that savepoint back and marks the row ``uncertified`` without changing its hash
or certificate JSON. That quarantine is its own savepoint. If the quarantine
write also fails, the audit raises and the upgrade aborts; the row is not left
``certified`` by a migration that reported success. When quarantine succeeds,
detections already certified in earlier savepoints stay certified. Rows whose
stored pairs already match are not rewritten. Re-running the audit is safe:
consistent rows are skipped, and a row that still cannot be linked stays
uncertified. If the migration process is killed before Alembic commits,
PostgreSQL rolls the revision back and the next upgrade retries.

A document displaced out of ``certificate_json`` is stored as the closing proof
only when those bytes hash to the closing hash and parse as a closing
certificate. Malformed displaced bytes are kept on the detection record for
investigation and are not given a closing hash. The qualifying repair can
still be ``certified`` when that link holds.

Checkpoints are read for one affected session at a time, in pages. Sessions
with no mismatched proof are not read.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from consistency_connectors.ingestion import BookManager
from consistency_core.fees import FeeCalculator, FeeSchedule, FeeScheduleRegistry
from consistency_core.models.market import Market
from consistency_core.models.relationship import Relationship
from consistency_core.pricing.certificate import ProofCertificate
from consistency_core.serialization import canonical_json
from consistency_pipeline.checkpoint import state_digest
from consistency_pipeline.driver import JournalApplier
from consistency_pipeline.engine import DetectionEngine
from consistency_pipeline.journal import JournalEntry

_CERTIFIED = "certified"
_UNCERTIFIED = "uncertified"
_CHECKPOINT_PAGE = 50


def _sha256(document: str) -> str:
    return "sha256:" + hashlib.sha256(document.encode()).hexdigest()


def _pair_matches(document: str | None, digest: str | None) -> bool:
    if document is None and digest is None:
        return True
    if document is None or digest is None:
        return False
    return _sha256(document) == digest


def _classification(document: str) -> str | None:
    try:
        body = json.loads(document)
    except json.JSONDecodeError:
        return None
    if not isinstance(body, dict):
        return None
    value = body.get("classification")
    return value if isinstance(value, str) else None


def _as_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    if isinstance(value, dict):
        return value
    return None


def _dec_eq(stored: object, expected: object) -> bool:
    if stored is None and expected is None:
        return True
    if stored is None or expected is None:
        return False
    try:
        return Decimal(str(stored)) == Decimal(str(expected))
    except (ArithmeticError, ValueError):
        return False


def _quoted(cert: ProofCertificate) -> dict[str, object]:
    evaluation = cert.evaluation
    top = cert.top_of_book
    capacity = cert.capacity
    return {
        "theoretical_deviation": None if top is None else top.pre_fee_edge_per_unit,
        "gross_edge": None if evaluation is None else evaluation.gross_profit,
        "total_fees": None if evaluation is None else evaluation.total_net_fees,
        "net_edge": None if evaluation is None else evaluation.net_profit,
        "execution_adjusted_edge": None
        if evaluation is None
        else evaluation.execution_adjusted_profit,
        "reported_quantity": None if evaluation is None else evaluation.quantity,
        "depth_supported_quantity": None if capacity is None else capacity.max_supported_quantity,
        "worst_case_payoff": None if evaluation is None else evaluation.min_payoff,
    }


def _metrics_agree(cert: ProofCertificate, record: object) -> bool:
    body = _as_object(record)
    if body is None:
        return True
    metrics = body.get("metrics")
    if not isinstance(metrics, dict):
        return True
    quoted = _quoted(cert)
    for key, expected in quoted.items():
        if key not in metrics or metrics[key] is None:
            continue
        if not _dec_eq(metrics[key], expected):
            return False
    return True


def _legs_agree(connection: Connection, detection_id: str, cert: ProofCertificate) -> bool:
    rows = (
        connection.execute(
            text(
                """
                SELECT market_id, side, ratio, quantity, premium, leg_cost
                FROM detection_legs
                WHERE detection_id = :id
                ORDER BY leg_index
                """
            ),
            {"id": detection_id},
        )
        .mappings()
        .all()
    )
    if not rows:
        return True
    portfolio = list(cert.portfolio.legs)
    executed = list(cert.evaluation.legs) if cert.evaluation is not None else []
    if len(rows) != len(portfolio):
        return False
    for index, row in enumerate(rows):
        leg = portfolio[index]
        walked = executed[index] if index < len(executed) else None
        if row["market_id"] != leg.market_id or row["side"] != leg.side.value:
            return False
        if not _dec_eq(row["ratio"], leg.ratio):
            return False
        if walked is None:
            if (
                row["quantity"] is not None
                or row["premium"] is not None
                or row["leg_cost"] is not None
            ):
                return False
            continue
        if not _dec_eq(row["quantity"], walked.quantity):
            return False
        if not _dec_eq(row["premium"], walked.walk.total_premium):
            return False
        if not _dec_eq(row["leg_cost"], walked.leg_cost):
            return False
    return True


def _scenarios_agree(connection: Connection, detection_id: str, cert: ProofCertificate) -> bool:
    rows = (
        connection.execute(
            text(
                """
                SELECT state_json, payoff_per_unit, is_worst
                FROM detection_scenarios
                WHERE detection_id = :id
                ORDER BY scenario_index
                """
            ),
            {"id": detection_id},
        )
        .mappings()
        .all()
    )
    if not rows:
        return True
    payoff = cert.payoff
    if payoff is None:
        return False
    states = list(payoff.states) if payoff.states else []
    expected: list[tuple[str, object, bool]]
    if states:
        expected = [
            (canonical_json(state.state), state.payoff_per_unit, state.state == payoff.worst_state)
            for state in states
        ]
    else:
        expected = [(canonical_json(payoff.worst_state), payoff.min_payoff_per_unit, True)]
    if len(rows) != len(expected):
        return False
    for row, (state_json, payoff_per_unit, is_worst) in zip(rows, expected, strict=True):
        if row["state_json"] != state_json or bool(row["is_worst"]) is not is_worst:
            return False
        if not _dec_eq(row["payoff_per_unit"], payoff_per_unit):
            return False
    return True


def _linked(
    connection: Connection,
    detection_id: str,
    document: str,
    *,
    stored_hash: str,
    classification: str | None,
    record: object,
    qualifying: bool,
) -> bool:
    """True when ``document`` is the observation named by ``stored_hash``.

    Qualifying proofs also have to agree with the row's classification, and
    with stored metrics, legs, and scenarios when those projections exist.
    A closing document is a different observation: its hash must match the
    stored closing hash, and its classification must match when that column
    is set. Qualifying legs are not a closing proof.
    """
    if _sha256(document) != stored_hash:
        return False
    try:
        cert = ProofCertificate.model_validate_json(document)
    except ValueError:
        return False
    if classification is not None and cert.classification.value != classification:
        return False
    if not qualifying:
        return True
    if classification is None:
        return False
    if not _metrics_agree(cert, record):
        return False
    if not _legs_agree(connection, detection_id, cert):
        return False
    return _scenarios_agree(connection, detection_id, cert)


def _document_for(row: Any, detection_id: str, *, closing: bool) -> str | None:
    payload = _as_object(row["checkpoint_json"])
    if payload is None:
        return None
    manager = payload.get("manager_state")
    engine = payload.get("engine_state")
    if not isinstance(manager, dict) or not isinstance(engine, dict):
        return None
    digest = state_digest(manager, engine)
    column_digest = row["state_digest"]
    embedded = payload.get("state_digest")
    if digest != column_digest or (isinstance(embedded, str) and embedded != digest):
        return None
    slots = engine.get("slots")
    if not isinstance(slots, dict):
        return None
    for slot in slots.values():
        if not isinstance(slot, dict):
            continue
        detection = slot.get("detection")
        if not isinstance(detection, dict):
            continue
        if detection.get("detection_id") != detection_id:
            continue
        key = "closing_certificate_json" if closing else "certificate_json"
        document = detection.get(key)
        if isinstance(document, str) and document:
            return document
    return None


def _checkpoint_document(
    connection: Connection,
    session_id: str,
    detection_id: str,
    *,
    stored_hash: str,
    classification: str | None,
    record: object,
    closing: bool,
) -> str | None:
    offset = 0
    while True:
        rows = (
            connection.execute(
                text(
                    """
                    SELECT ordinal, state_digest, checkpoint_json
                    FROM session_checkpoints
                    WHERE session_id = :session_id
                    ORDER BY ordinal DESC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {
                    "session_id": session_id,
                    "limit": _CHECKPOINT_PAGE,
                    "offset": offset,
                },
            )
            .mappings()
            .all()
        )
        if not rows:
            return None
        for row in rows:
            document = _document_for(row, detection_id, closing=closing)
            if document is None:
                continue
            if _linked(
                connection,
                detection_id,
                document,
                stored_hash=stored_hash,
                classification=classification,
                record=record,
                qualifying=not closing,
            ):
                return document
        if len(rows) < _CHECKPOINT_PAGE:
            return None
        offset += _CHECKPOINT_PAGE


def _snapshot_list(document: object, key: str) -> list[object] | None:
    body = _as_object(document)
    if body is None:
        return None
    raw = body.get(key)
    if not isinstance(raw, list):
        return None
    return list(raw)


def _version_document(connection: Connection, version_id: str) -> object | None:
    return connection.execute(
        text("SELECT document FROM configuration_versions WHERE version_id = :id"),
        {"id": version_id},
    ).scalar()


def _replay_certificates(connection: Connection, session_id: str) -> dict[str, str]:
    """Qualifying certificate JSON from a full journal replay, keyed by detection id.

    An empty journal, or a session whose catalog cannot be loaded, returns an
    empty map. A replay error is not a reason to adopt a checkpoint instead of
    reporting that replay was unavailable.
    """
    count = connection.execute(
        text("SELECT COUNT(*) FROM orderbook_updates WHERE session_id = :sid"),
        {"sid": session_id},
    ).scalar()
    if not isinstance(count, int) or count <= 0:
        return {}
    try:
        loaded = _load_replay_inputs(connection, session_id)
        if loaded is None:
            return {}
        markets, relationships, schedules, entries = loaded
        if not entries or not markets:
            return {}
        label = connection.execute(
            text("SELECT source_label FROM ingestion_sessions WHERE session_id = :sid"),
            {"sid": session_id},
        ).scalar()
        source = label if isinstance(label, str) and label else "replay"
        manager = BookManager(markets, source=source)
        engine = DetectionEngine(
            manager,
            relationships,
            FeeCalculator(FeeScheduleRegistry(schedules)),
            session_id=session_id,
        )
        JournalApplier(manager, engine).apply_all(entries)
    except (ValueError, KeyError, TypeError):
        return {}
    found: dict[str, str] = {}
    slots = engine.export_state().get("slots")
    if not isinstance(slots, dict):
        return {}
    for slot in slots.values():
        if not isinstance(slot, dict):
            continue
        detection = slot.get("detection")
        if not isinstance(detection, dict):
            continue
        detection_id = detection.get("detection_id")
        document = detection.get("certificate_json")
        if isinstance(detection_id, str) and isinstance(document, str) and document:
            found[detection_id] = document
            # The certificate hash does not depend on the session id embedded in
            # detection_id. A row retargeted onto a stored session still links.
            found[_sha256(document)] = document
    return found


def _load_replay_inputs(
    connection: Connection, session_id: str
) -> tuple[list[Market], list[Relationship], list[FeeSchedule], list[JournalEntry]] | None:
    reference = _as_object(_version_document(connection, f"session-reference:{session_id}"))
    stamp = connection.execute(
        text(
            """
            SELECT relationship_version, fee_schedule_version
            FROM orderbook_updates
            WHERE session_id = :sid
            ORDER BY ordinal
            LIMIT 1
            """
        ),
        {"sid": session_id},
    ).first()
    market_version = None if reference is None else reference.get("market_metadata_version")
    relationship_version = None if reference is None else reference.get("relationship_version")
    fee_version = None if reference is None else reference.get("fee_schedule_version")
    if stamp is not None:
        relationship_version = stamp[0]
        fee_version = stamp[1]
    if (
        not isinstance(market_version, str)
        or not isinstance(relationship_version, str)
        or not isinstance(fee_version, str)
    ):
        return None
    market_items = _snapshot_list(_version_document(connection, market_version), "markets")
    relationship_items = _snapshot_list(
        _version_document(connection, relationship_version), "relationships"
    )
    fee_items = _snapshot_list(_version_document(connection, fee_version), "schedules")
    if market_items is None or relationship_items is None or fee_items is None:
        return None
    payloads = connection.execute(
        text(
            """
            SELECT entry_json
            FROM orderbook_updates
            WHERE session_id = :sid
            ORDER BY ordinal
            """
        ),
        {"sid": session_id},
    ).scalars()
    entries = [JournalEntry.model_validate(payload) for payload in payloads]
    return (
        [Market.model_validate(item) for item in market_items],
        [Relationship.model_validate(item) for item in relationship_items],
        [FeeSchedule.model_validate(item) for item in fee_items],
        entries,
    )


def _projection(connection: Connection, detection_id: str, certificate_json: str) -> None:
    try:
        cert = ProofCertificate.model_validate_json(certificate_json)
    except ValueError:
        return
    connection.execute(
        text("DELETE FROM detection_legs WHERE detection_id = :id"),
        {"id": detection_id},
    )
    connection.execute(
        text("DELETE FROM detection_scenarios WHERE detection_id = :id"),
        {"id": detection_id},
    )
    executed = list(cert.evaluation.legs) if cert.evaluation is not None else []
    for index, leg in enumerate(cert.portfolio.legs):
        walked = executed[index] if index < len(executed) else None
        connection.execute(
            text(
                """
                INSERT INTO detection_legs (
                    detection_id, leg_index, market_id, side, ratio, quantity, premium, leg_cost
                ) VALUES (
                    :detection_id, :leg_index, :market_id, :side,
                    :ratio, :quantity, :premium, :leg_cost
                )
                """
            ),
            {
                "detection_id": detection_id,
                "leg_index": index,
                "market_id": leg.market_id,
                "side": leg.side.value,
                "ratio": leg.ratio,
                "quantity": None if walked is None else walked.quantity,
                "premium": None if walked is None else walked.walk.total_premium,
                "leg_cost": None if walked is None else walked.leg_cost,
            },
        )
    payoff = cert.payoff
    if payoff is None:
        return
    states = list(payoff.states) if payoff.states else []
    if states:
        for index, state in enumerate(states):
            connection.execute(
                text(
                    """
                    INSERT INTO detection_scenarios (
                        detection_id, scenario_index, state_json, payoff_per_unit, is_worst
                    ) VALUES (
                        :detection_id, :scenario_index, :state_json, :payoff_per_unit, :is_worst
                    )
                    """
                ),
                {
                    "detection_id": detection_id,
                    "scenario_index": index,
                    "state_json": canonical_json(state.state),
                    "payoff_per_unit": state.payoff_per_unit,
                    "is_worst": state.state == payoff.worst_state,
                },
            )
        return
    connection.execute(
        text(
            """
            INSERT INTO detection_scenarios (
                detection_id, scenario_index, state_json, payoff_per_unit, is_worst
            ) VALUES (
                :detection_id, 0, :state_json, :payoff_per_unit, true
            )
            """
        ),
        {
            "detection_id": detection_id,
            "state_json": canonical_json(payoff.worst_state),
            "payoff_per_unit": payoff.min_payoff_per_unit,
        },
    )


def _accepted_closing(document: str) -> tuple[str, str, str] | None:
    """Return the document, its hash, and its classification when it is a proof.

    The hash is SHA-256 of these exact bytes. A document that does not parse
    as a closing certificate is not given a hash.
    """
    try:
        cert = ProofCertificate.model_validate_json(document)
    except ValueError:
        return None
    return document, _sha256(document), cert.classification.value


def _record_json(raw: Any, *, drop: tuple[str, ...] = (), **fields: str | None) -> str:
    parsed = _as_object(raw)
    body = {} if parsed is None else dict(parsed)
    for key in drop:
        body.pop(key, None)
    for key, value in fields.items():
        if value is not None:
            body[key] = value
    return json.dumps(body)


def _affected(connection: Connection) -> list[Any]:
    rows = connection.execute(
        text(
            """
            SELECT detection_id, session_id, classification, certificate_hash, certificate_json,
                   closing_classification, closing_certificate_hash, closing_certificate_json,
                   record_json
            FROM detections
            ORDER BY session_id, detection_id
            """
        )
    ).mappings()
    affected: list[Any] = []
    for row in rows:
        qualifying_json = row["certificate_json"]
        qualifying_hash = row["certificate_hash"]
        closing_json = row["closing_certificate_json"]
        closing_hash = row["closing_certificate_hash"]
        qualifying_document = qualifying_json if isinstance(qualifying_json, str) else None
        closing_document = closing_json if isinstance(closing_json, str) else None
        stored_qualifying_hash = qualifying_hash if isinstance(qualifying_hash, str) else None
        stored_closing_hash = closing_hash if isinstance(closing_hash, str) else None
        qualifying_ok = (
            qualifying_document is not None
            and stored_qualifying_hash is not None
            and _sha256(qualifying_document) == stored_qualifying_hash
        )
        if qualifying_ok and _pair_matches(closing_document, stored_closing_hash):
            continue
        affected.append(row)
    return affected


def audit_stored_proofs(connection: Connection) -> None:
    """Check mismatched proofs. Leave matching pairs untouched.

    Only sessions that contain a mismatched or missing proof are read.
    Checkpoints for one of those sessions are paged. A repair failure
    quarantines that detection and leaves earlier savepoints certified. If that
    quarantine write fails, this function raises so the upgrade aborts.
    """
    grouped: dict[str, list[Any]] = defaultdict(list)
    for row in _affected(connection):
        grouped[str(row["session_id"])].append(row)
    for session_id in sorted(grouped):
        try:
            replayed = _replay_certificates(connection, session_id)
        except Exception:
            replayed = {}
        for row in grouped[session_id]:
            _repair_isolated(connection, row, replayed)


def _repair_isolated(connection: Connection, row: Any, replayed: dict[str, str]) -> None:
    detection_id = str(row["detection_id"])
    nested = connection.begin_nested()
    try:
        _repair_row(connection, row, replayed)
        nested.commit()
    except Exception:
        nested.rollback()
        rescue = connection.begin_nested()
        try:
            _quarantine(connection, detection_id)
            rescue.commit()
        except Exception as quarantine_error:
            rescue.rollback()
            raise RuntimeError(
                f"{detection_id} certificate quarantine write failed"
            ) from quarantine_error


def _repair_row(connection: Connection, row: Any, replayed: dict[str, str]) -> None:
    detection_id = str(row["detection_id"])
    session_id = str(row["session_id"])
    classification = row["classification"]
    if not isinstance(classification, str):
        _quarantine(connection, detection_id)
        return
    qualifying_json = row["certificate_json"]
    qualifying_hash = row["certificate_hash"]
    closing_json = row["closing_certificate_json"]
    closing_hash = row["closing_certificate_hash"]
    qualifying_document = qualifying_json if isinstance(qualifying_json, str) else None
    closing_document = closing_json if isinstance(closing_json, str) else None
    stored_qualifying_hash = qualifying_hash if isinstance(qualifying_hash, str) else None
    stored_closing_hash = closing_hash if isinstance(closing_hash, str) else None
    qualifying_ok = (
        qualifying_document is not None
        and stored_qualifying_hash is not None
        and _sha256(qualifying_document) == stored_qualifying_hash
    )
    closing_ok = _pair_matches(closing_document, stored_closing_hash)
    new_qualifying = qualifying_document
    new_qualifying_hash = stored_qualifying_hash
    new_closing = closing_document
    new_closing_hash = stored_closing_hash
    new_closing_class = row["closing_classification"]
    displaced: str | None = None
    withheld_displaced: str | None = None
    if not qualifying_ok:
        if stored_qualifying_hash is None:
            _quarantine(connection, detection_id)
            return
        found = replayed.get(detection_id)
        if found is None:
            found = replayed.get(stored_qualifying_hash)
        if found is None or not _linked(
            connection,
            detection_id,
            found,
            stored_hash=stored_qualifying_hash,
            classification=classification,
            record=row["record_json"],
            qualifying=True,
        ):
            found = _checkpoint_document(
                connection,
                session_id,
                detection_id,
                stored_hash=stored_qualifying_hash,
                classification=classification,
                record=row["record_json"],
                closing=False,
            )
        if found is None:
            _quarantine(connection, detection_id)
            return
        if qualifying_document is not None and qualifying_document != found:
            displaced = qualifying_document
        new_qualifying = found
        new_qualifying_hash = stored_qualifying_hash
    if closing_document is None and stored_closing_hash is None and displaced is not None:
        accepted = _accepted_closing(displaced)
        if accepted is None:
            # Keep the bytes for investigation. Do not advertise them as a proof.
            withheld_displaced = displaced
        else:
            new_closing, new_closing_hash, new_closing_class = accepted
    elif not closing_ok:
        if stored_closing_hash is None:
            _quarantine(connection, detection_id)
            return
        closing_class = row["closing_classification"]
        found_closing = _checkpoint_document(
            connection,
            session_id,
            detection_id,
            stored_hash=stored_closing_hash,
            classification=closing_class if isinstance(closing_class, str) else None,
            record=row["record_json"],
            closing=True,
        )
        if found_closing is None:
            if new_qualifying is not None and new_qualifying != qualifying_document:
                _write_qualifying(
                    connection,
                    detection_id,
                    qualifying=new_qualifying,
                    qualifying_hash=stored_qualifying_hash or _sha256(new_qualifying),
                    record_json=row["record_json"],
                    status=_UNCERTIFIED,
                )
            else:
                _quarantine(connection, detection_id)
            return
        new_closing = found_closing
        new_closing_hash = stored_closing_hash
        parsed = _classification(found_closing)
        if parsed is not None:
            new_closing_class = parsed
    if new_qualifying is None or new_qualifying_hash != _sha256(new_qualifying):
        _quarantine(connection, detection_id)
        return
    if new_closing is not None:
        checked = _accepted_closing(new_closing)
        if checked is None or checked[1] != new_closing_hash:
            withheld_displaced = new_closing if withheld_displaced is None else withheld_displaced
            new_closing = None
            new_closing_hash = None
            new_closing_class = None
    elif new_closing_hash is not None:
        _quarantine(connection, detection_id)
        return
    connection.execute(
        text(
            """
            UPDATE detections
            SET certificate_json = :qualifying,
                certificate_hash = :qualifying_hash,
                closing_certificate_json = :closing,
                closing_certificate_hash = :closing_hash,
                closing_classification = :closing_class,
                proof_status = :status,
                record_json = CAST(:record_json AS jsonb)
            WHERE detection_id = :detection_id
            """
        ),
        {
            "detection_id": detection_id,
            "qualifying": new_qualifying,
            "qualifying_hash": new_qualifying_hash,
            "closing": new_closing,
            "closing_hash": new_closing_hash,
            "closing_class": new_closing_class if isinstance(new_closing_class, str) else None,
            "status": _CERTIFIED,
            "record_json": _record_json(
                row["record_json"],
                drop=(
                    ("closing_certificate_json", "closing_certificate_hash")
                    if withheld_displaced is not None
                    else ()
                ),
                certificate_json=new_qualifying,
                certificate_hash=new_qualifying_hash,
                closing_certificate_json=new_closing,
                closing_certificate_hash=new_closing_hash,
                displaced_document=withheld_displaced,
            ),
        },
    )
    if new_qualifying != qualifying_document:
        _projection(connection, detection_id, new_qualifying)


def _quarantine(connection: Connection, detection_id: str) -> None:
    connection.execute(
        text(
            """
            UPDATE detections
            SET proof_status = :status
            WHERE detection_id = :detection_id
            """
        ),
        {"detection_id": detection_id, "status": _UNCERTIFIED},
    )


def _write_qualifying(
    connection: Connection,
    detection_id: str,
    *,
    qualifying: str,
    qualifying_hash: str,
    record_json: Any,
    status: str,
) -> None:
    if qualifying_hash != _sha256(qualifying):
        _quarantine(connection, detection_id)
        return
    connection.execute(
        text(
            """
            UPDATE detections
            SET certificate_json = :qualifying,
                certificate_hash = :qualifying_hash,
                proof_status = :status,
                record_json = CAST(:record_json AS jsonb)
            WHERE detection_id = :detection_id
            """
        ),
        {
            "detection_id": detection_id,
            "qualifying": qualifying,
            "qualifying_hash": qualifying_hash,
            "status": status,
            "record_json": _record_json(
                record_json,
                certificate_json=qualifying,
                certificate_hash=qualifying_hash,
            ),
        },
    )
    _projection(connection, detection_id, qualifying)

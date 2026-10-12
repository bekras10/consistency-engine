"""Repair or quarantine certificate rows left by the pre-0005 writer.

That writer kept the qualifying hash and stored a later evaluation's JSON in
``certificate_json``. This audit does not delete rows.

* A document whose SHA-256 equals its stored hash is left alone.
* Otherwise the qualifying proof is taken from a checkpoint whose ``state_digest``
  recomputes. The stored JSON becomes that proof, and the hash becomes the hash
  of those bytes. The displaced document, when the closing columns are empty, is
  kept as the closing certificate with the hash of those same bytes.
* A hash is never rewritten so that it matches a document that was not
  reconstructed. A document is never replaced by something other than that proof.
* If no verified checkpoint contains the detection, ``proof_status`` is
  ``uncertified`` and the mismatched bytes stay where they are.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from consistency_core.pricing.certificate import ProofCertificate
from consistency_core.serialization import canonical_json
from consistency_pipeline.checkpoint import state_digest

_CERTIFIED = "certified"
_UNCERTIFIED = "uncertified"


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


@dataclass(frozen=True)
class _Snapshot:
    qualifying: str | None
    closing: str | None


def _as_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    if isinstance(value, dict):
        return value
    return None


def _verified_snapshots(connection: Connection) -> dict[str, dict[str, list[_Snapshot]]]:
    """Session id, then detection id, newest verified checkpoint first."""
    rows = connection.execute(
        text(
            """
            SELECT session_id, ordinal, state_digest, checkpoint_json
            FROM session_checkpoints
            ORDER BY session_id, ordinal DESC
            """
        )
    ).mappings()
    found: dict[str, dict[str, list[_Snapshot]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        payload = _as_object(row["checkpoint_json"])
        if payload is None:
            continue
        manager = payload.get("manager_state")
        engine = payload.get("engine_state")
        if not isinstance(manager, dict) or not isinstance(engine, dict):
            continue
        digest = state_digest(manager, engine)
        column_digest = row["state_digest"]
        embedded = payload.get("state_digest")
        if digest != column_digest or (isinstance(embedded, str) and embedded != digest):
            continue
        slots = engine.get("slots")
        if not isinstance(slots, dict):
            continue
        session_id = str(row["session_id"])
        for slot in slots.values():
            if not isinstance(slot, dict):
                continue
            detection = slot.get("detection")
            if not isinstance(detection, dict):
                continue
            detection_id = detection.get("detection_id")
            if not isinstance(detection_id, str):
                continue
            qualifying = detection.get("certificate_json")
            closing = detection.get("closing_certificate_json")
            found[session_id][detection_id].append(
                _Snapshot(
                    qualifying if isinstance(qualifying, str) and qualifying else None,
                    closing if isinstance(closing, str) and closing else None,
                )
            )
    return found


def _choose(stored_hash: str | None, snapshots: list[_Snapshot], *, closing: bool) -> str | None:
    documents = [item.closing if closing else item.qualifying for item in snapshots]
    present = [item for item in documents if item]
    if stored_hash is not None:
        for item in present:
            if _sha256(item) == stored_hash:
                return item
    return present[0] if present else None


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


def _record_json(raw: Any, **fields: str | None) -> str:
    parsed = _as_object(raw)
    body = {} if parsed is None else dict(parsed)
    for key, value in fields.items():
        if value is not None:
            body[key] = value
    return json.dumps(body)


def audit_stored_proofs(connection: Connection) -> None:
    """Check every detection. Repair from a verified checkpoint, or quarantine."""
    snapshots = _verified_snapshots(connection)
    rows = connection.execute(
        text(
            """
            SELECT detection_id, session_id, certificate_hash, certificate_json,
                   closing_classification, closing_certificate_hash, closing_certificate_json,
                   record_json
            FROM detections
            """
        )
    ).mappings()
    for row in rows:
        detection_id = str(row["detection_id"])
        session_id = str(row["session_id"])
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
        if qualifying_ok and closing_ok:
            continue
        history = snapshots.get(session_id, {}).get(detection_id, [])
        new_qualifying = qualifying_document
        new_qualifying_hash = stored_qualifying_hash
        new_closing = closing_document
        new_closing_hash = stored_closing_hash
        new_closing_class = row["closing_classification"]
        displaced: str | None = None
        if not qualifying_ok:
            found = _choose(stored_qualifying_hash, history, closing=False)
            if found is None or stored_qualifying_hash is None:
                _quarantine(connection, detection_id)
                continue
            if qualifying_document is not None and qualifying_document != found:
                displaced = qualifying_document
            new_qualifying = found
            new_qualifying_hash = _sha256(found)
        if closing_document is None and stored_closing_hash is None and displaced is not None:
            new_closing = displaced
            new_closing_hash = _sha256(displaced)
            parsed = _classification(displaced)
            if parsed is not None:
                new_closing_class = parsed
        elif not closing_ok:
            found_closing = _choose(stored_closing_hash, history, closing=True)
            if found_closing is None:
                if new_qualifying is not None and new_qualifying != qualifying_document:
                    _write_qualifying(
                        connection,
                        detection_id,
                        qualifying=new_qualifying,
                        qualifying_hash=new_qualifying_hash or _sha256(new_qualifying),
                        record_json=row["record_json"],
                        status=_UNCERTIFIED,
                    )
                else:
                    _quarantine(connection, detection_id)
                continue
            new_closing = found_closing
            new_closing_hash = _sha256(found_closing)
            parsed = _classification(found_closing)
            if parsed is not None:
                new_closing_class = parsed
        if new_qualifying is None or new_qualifying_hash != _sha256(new_qualifying):
            _quarantine(connection, detection_id)
            continue
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
                    certificate_json=new_qualifying,
                    certificate_hash=new_qualifying_hash,
                    closing_certificate_json=new_closing,
                    closing_certificate_hash=new_closing_hash,
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

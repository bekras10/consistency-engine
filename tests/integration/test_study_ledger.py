"""Hand-derived study aggregates from PostgreSQL, including a restart."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from consistency_connectors.kalshi.study import build_report, report_csv
from consistency_persistence.db import make_engine, make_sessionmaker
from consistency_persistence.study_ledger import StudyLedger

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 11, tzinfo=UTC)
STUDY = "study-alpha"
SESSION = "ses-live"


def _eval(
    key: str,
    at: int,
    classification: str,
    *,
    synchronized: bool,
    subject: str = "rel",
) -> dict[str, object]:
    return {
        "idempotency_key": key,
        "study_id": STUDY,
        "session_id": SESSION,
        "kind": "evaluation",
        "occurred_ms": at,
        "subject_id": subject,
        "classification": classification,
        "synchronized": synchronized,
        "payload": {},
    }


async def _report(database_url: str) -> dict[str, object]:
    engine = make_engine(database_url)
    try:
        facts = await StudyLedger(make_sessionmaker(engine)).load(STUDY)
    finally:
        await engine.dispose()
    return build_report(
        study_id=facts.study_id,
        study_start=facts.planned_start,
        study_end=facts.planned_end,
        now=NOW,
        commit=facts.commit_sha,
        config_fingerprint=facts.config_fingerprint,
        environment=facts.environment,
        market_ids=facts.market_ids,
        relationship_ids=facts.relationship_ids,
        verified_relationship_ids=facts.verified_relationship_ids,
        fee_schedule_ids=["kalshi-2026-07-07"],
        raw_retention_authorized=facts.raw_retention_authorized,
        replay_available=facts.replay_available,
        rows=facts.episode_rows,
        analyzed_updates=facts.analyzed_updates,
        synchronized_evaluations=None,
        observed_ms=facts.observed_ms,
        synchronized_ms=facts.synchronized_ms,
        coverage_established=facts.coverage_established,
        eligible_sessions=facts.eligible_sessions,
        evaluation_rows=facts.evaluation_rows,
        membership=facts.membership,
        exclusions=facts.exclusions,
        ledger={
            "downtime_ms": facts.downtime_ms,
            "desync_ms": facts.desync_ms,
            "reconnects": facts.reconnects,
            "skipped": facts.skipped,
            "excluded": facts.excluded,
            "fee_verified_checks": facts.fee_verified_checks,
            "fee_unverified_checks": facts.fee_unverified_checks,
            "latency_samples": facts.latency_samples,
            "latency_max_ms": facts.latency_max_ms,
            "freshness_samples": facts.freshness_samples,
        },
    )


async def test_study_report_matches_hand_derived_ledger_totals(db: str) -> None:
    """Arithmetic, written here and not copied from the query:

    synchronized coverage [1000, 5000) and [7000, 9000) = 4000 + 2000 = 6000 ms
    downtime [5000, 7000) = 2000 ms
    observed union [1000, 9000) = 8000 ms
    6000 + 2000 = 8000
    desync [3000, 3500) = 500 ms
    evaluations = 8, of which 7 are synchronized (the unsynchronized row is the exception)
    non-overlapping funnel 1+1+1+1+1+1+2 = 8
    opportunity episodes = 4 (two fee-adjusted evaluations share one detection id)
    book updates = 3 and do not add episodes
    planned window is 2 days and the end is already past, so status is INCOMPLETE
    """
    engine = make_engine(db)
    ledger = StudyLedger(make_sessionmaker(engine))
    await ledger.ensure_study(
        study_id=STUDY,
        source_kind="kalshi_authorized",
        environment="production",
        commit_sha="abc123",
        config_fingerprint="cfg-9",
        planned_start="2026-10-01T00:00:00+00:00",
        planned_end="2026-10-03T00:00:00+00:00",
        raw_retention_authorized=False,
    )
    await ledger.open_session(
        study_id=STUDY,
        session_id=SESSION,
        source_kind="kalshi_authorized",
        eligible=True,
        started_ms=1000,
    )
    await ledger.open_session(
        study_id=STUDY,
        session_id="ses-synthetic",
        source_kind="synthetic",
        eligible=False,
        started_ms=1000,
    )
    await ledger.ensure_study(
        study_id="study-synthetic",
        source_kind="synthetic",
        environment="synthetic",
        commit_sha="nope",
        config_fingerprint="syn",
        planned_start="2026-10-01T00:00:00+00:00",
        planned_end="2026-10-03T00:00:00+00:00",
        raw_retention_authorized=False,
    )
    await ledger.open_session(
        study_id="study-synthetic",
        session_id="ses-other",
        source_kind="synthetic",
        eligible=True,
        started_ms=1,
    )
    await ledger.upsert_episodes(
        [
            {
                "episode_id": "study-synthetic|noise",
                "study_id": "study-synthetic",
                "session_id": "ses-other",
                "detection_id": "noise",
                "relationship_id": "noise",
                "strategy_id": "noise",
                "classification": "FEE_ADJUSTED_CANDIDATE",
                "status": "OPEN",
                "first_ms": 1,
                "last_ms": 2,
                "close_reason": None,
                "gross_edge": "9",
                "fees": "0",
                "net_edge": "9",
                "quantity": "9",
            }
        ]
    )
    evaluations = [
        _eval("e1", 1000, "NO_OPPORTUNITY", synchronized=True),
        _eval("e2", 2000, "INVALID_RELATIONSHIP", synchronized=True),
        _eval("e3", 3000, "UNSYNCHRONIZED_DATA", synchronized=False),
        _eval("e4", 4000, "THEORETICAL_ONLY", synchronized=True),
        _eval("e5", 5000, "FEE_UNVERIFIED", synchronized=True),
        _eval("e6", 6000, "DEPTH_SUPPORTED", synchronized=True),
        _eval("e7", 7000, "FEE_ADJUSTED_CANDIDATE", synchronized=True, subject="R-verified"),
        _eval("e8", 8000, "FEE_ADJUSTED_CANDIDATE", synchronized=True, subject="R-verified"),
    ]
    await ledger.record_events(evaluations)
    await ledger.record_events(
        [
            {
                "idempotency_key": f"book-{index}",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "book_update",
                "occurred_ms": 7000 + index,
                "subject_id": "d-fee",
                "classification": "",
                "synchronized": True,
                "payload": {},
            }
            for index in range(3)
        ]
    )
    await ledger.record_events(
        [
            {
                "idempotency_key": "cov-1",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "coverage",
                "occurred_ms": 5000,
                "subject_id": SESSION,
                "classification": "",
                "synchronized": True,
                "payload": {"start_ms": 1000, "end_ms": 5000},
            },
            {
                "idempotency_key": "cov-2",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "coverage",
                "occurred_ms": 9000,
                "subject_id": SESSION,
                "classification": "",
                "synchronized": True,
                "payload": {"start_ms": 7000, "end_ms": 9000},
            },
            {
                "idempotency_key": "down-1",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "downtime",
                "occurred_ms": 7000,
                "subject_id": SESSION,
                "classification": "",
                "synchronized": False,
                "payload": {"start_ms": 5000, "end_ms": 7000},
            },
            {
                "idempotency_key": "desync-1",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "desync",
                "occurred_ms": 3500,
                "subject_id": "M-A",
                "classification": "",
                "synchronized": False,
                "payload": {"start_ms": 3000, "end_ms": 3500},
            },
            {
                "idempotency_key": "re-1",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "reconnect",
                "occurred_ms": 3600,
                "subject_id": SESSION,
                "classification": "",
                "synchronized": False,
                "payload": {},
            },
            {
                "idempotency_key": "skip-1",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "skip",
                "occurred_ms": 1100,
                "subject_id": "M-X",
                "classification": "",
                "synchronized": False,
                "payload": {"reason": "family incomplete"},
            },
            {
                "idempotency_key": "ex-1",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "exclusion",
                "occurred_ms": 1200,
                "subject_id": "M-Y",
                "classification": "",
                "synchronized": False,
                "payload": {"reason": "market closed"},
            },
            {
                "idempotency_key": "fee-1",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "fee_verification",
                "occurred_ms": 7000,
                "subject_id": "R-verified",
                "classification": "FEE_ADJUSTED_CANDIDATE",
                "synchronized": True,
                "payload": {"verified": True},
            },
            {
                "idempotency_key": "fee-0",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "fee_verification",
                "occurred_ms": 5000,
                "subject_id": "R-verified",
                "classification": "FEE_UNVERIFIED",
                "synchronized": True,
                "payload": {"verified": False},
            },
            {
                "idempotency_key": "lat-1",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "latency",
                "occurred_ms": 4000,
                "subject_id": SESSION,
                "classification": "",
                "synchronized": True,
                "payload": {"latency_ms": 12},
            },
            {
                "idempotency_key": "lat-2",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "latency",
                "occurred_ms": 8000,
                "subject_id": SESSION,
                "classification": "",
                "synchronized": True,
                "payload": {"latency_ms": 40},
            },
            {
                "idempotency_key": "fresh-1",
                "study_id": STUDY,
                "session_id": SESSION,
                "kind": "freshness",
                "occurred_ms": 4000,
                "subject_id": SESSION,
                "classification": "",
                "synchronized": True,
                "payload": {"max_book_age_ms": 80},
            },
        ]
    )
    await ledger.enter_membership(
        study_id=STUDY,
        session_id=SESSION,
        subject_kind="market",
        subject_id="M-A",
        entered_ms=1000,
        fingerprint="hash-a",
        verification_status="",
    )
    await ledger.exit_membership(
        study_id=STUDY,
        session_id=SESSION,
        subject_kind="market",
        subject_id="M-A",
        exited_ms=5000,
    )
    await ledger.enter_membership(
        study_id=STUDY,
        session_id=SESSION,
        subject_kind="market",
        subject_id="M-B",
        entered_ms=5000,
        fingerprint="hash-b",
        verification_status="",
    )
    await ledger.enter_membership(
        study_id=STUDY,
        session_id=SESSION,
        subject_kind="relationship",
        subject_id="R-verified",
        entered_ms=1000,
        fingerprint="fp-verified",
        verification_status="verified",
    )
    await ledger.enter_membership(
        study_id=STUDY,
        session_id=SESSION,
        subject_kind="relationship",
        subject_id="R-old",
        entered_ms=2000,
        fingerprint="fp-old",
        verification_status="candidate_review",
    )
    await ledger.exit_membership(
        study_id=STUDY,
        session_id=SESSION,
        subject_kind="relationship",
        subject_id="R-old",
        exited_ms=4000,
    )
    await ledger.upsert_episodes(
        [
            _episode("d-theo", "THEORETICAL_ONLY", 4000, 4500, "0.0100", "0.0020", "0.0080"),
            _episode("d-unverified", "FEE_UNVERIFIED", 5000, 5200, "0.0200", "0.0040", "0.0160"),
            _episode("d-depth", "DEPTH_SUPPORTED", 6000, 6100, "0.0300", "0.0050", "0.0250"),
            _episode("d-fee", "FEE_ADJUSTED_CANDIDATE", 7000, 8000, "0.0400", "0.0100", "0.0300"),
        ]
    )
    # Restart: the same keys and an older episode timestamp must not change totals.
    await ledger.record_events(evaluations)
    await ledger.upsert_episodes(
        [_episode("d-fee", "THEORETICAL_ONLY", 7000, 7500, "0.0010", "0.0000", "0.0010")]
    )
    await engine.dispose()

    report = await _report(db)
    assert report["window_status"] == "INCOMPLETE"
    assert report["window_status"] != "COMPLETE"
    assert report["coverage_established"] is False
    assert report["commit"] == "abc123"
    assert report["config_fingerprint"] == "cfg-9"
    assert report["observed_duration_ms"] == 8000
    assert report["synchronized_monitoring_ms"] == 6000
    assert report["eligible_sessions"] == 1
    assert report["analyzed_updates"] == 3
    assert report["distinct_episodes"] == 4
    assert report["fee_verified_positive_candidates"] == 1
    assert report["unverified_fee_candidates"] == 1
    assert report["depth_supported_episodes"] == 1
    assert report["theoretical_inconsistency_episodes"] == 4
    assert report["replay_available"] is False
    assert report["markets"] == ["M-A", "M-B"]
    assert report["verified_relationships"] == ["R-verified"]
    funnel = report["evaluation_funnel"]
    assert isinstance(funnel, dict)
    assert funnel["population_count"] == 8
    assert funnel["synchronized_evaluations"] == 7
    assert funnel["non_overlapping_sums_to_population"] is True
    assert funnel["non_overlapping"] == {
        "rule_verification_rejection": 1,
        "market_data_quality_exclusion": 1,
        "no_theoretical_deviation": 1,
        "theoretical_deviation": 1,
        "depth_supported": 1,
        "fee_unverified": 1,
        "fee_verified": 2,
    }
    assert report["synchronized_evaluations"] == 7
    ledger_body = report["ledger"]
    assert isinstance(ledger_body, dict)
    assert ledger_body["downtime_ms"] == 2000
    assert ledger_body["desync_ms"] == 500
    assert ledger_body["reconnects"] == 1
    assert ledger_body["skipped"] == 1
    assert ledger_body["excluded"] == 1
    assert ledger_body["fee_verified_checks"] == 1
    assert ledger_body["fee_unverified_checks"] == 1
    assert ledger_body["latency_samples"] == 2
    assert ledger_body["latency_max_ms"] == 40
    assert ledger_body["freshness_samples"] == 1
    warnings = report["coverage_warnings"]
    assert isinstance(warnings, list)
    assert any("incomplete_coverage" in str(item) for item in warnings)
    top = report["top_fee_verified_candidates"]
    assert isinstance(top, list) and len(top) == 1
    assert top[0]["last_ms"] == 8000
    assert top[0]["net_edge"] == "0.0300"
    text = report_csv(report)
    assert "INCOMPLETE" in text
    assert "fee_verified,2" in text
    membership = report["membership_intervals"]
    assert isinstance(membership, list)
    exited = next(item for item in membership if item["subject_id"] == "M-A")
    assert exited["entered_ms"] == 1000
    assert exited["exited_ms"] == 5000
    still_in = next(item for item in membership if item["subject_id"] == "M-B")
    assert still_in["exited_ms"] is None

    empty_engine = make_engine(db)
    try:
        empty = StudyLedger(make_sessionmaker(empty_engine))
        await empty.ensure_study(
            study_id="study-empty",
            source_kind="kalshi_authorized",
            environment="production",
            commit_sha="abc123",
            config_fingerprint="cfg-9",
            planned_start="2026-10-01T00:00:00+00:00",
            planned_end="2026-10-03T00:00:00+00:00",
            raw_retention_authorized=False,
        )
        facts = await empty.load("study-empty")
    finally:
        await empty_engine.dispose()
    empty_report = build_report(
        study_id=facts.study_id,
        study_start=facts.planned_start,
        study_end=facts.planned_end,
        now=NOW,
        commit=facts.commit_sha,
        config_fingerprint=facts.config_fingerprint,
        environment=facts.environment,
        market_ids=facts.market_ids,
        relationship_ids=facts.relationship_ids,
        verified_relationship_ids=facts.verified_relationship_ids,
        fee_schedule_ids=["kalshi-2026-07-07"],
        raw_retention_authorized=False,
        replay_available=facts.replay_available,
        rows=facts.episode_rows,
        analyzed_updates=facts.analyzed_updates,
        synchronized_evaluations=None,
        observed_ms=facts.observed_ms,
        synchronized_ms=facts.synchronized_ms,
        coverage_established=facts.coverage_established,
        eligible_sessions=facts.eligible_sessions,
        evaluation_rows=facts.evaluation_rows,
    )
    assert empty_report["window_status"] == "INCOMPLETE"
    assert empty_report["fee_verified_positive_candidates"] == 0
    assert facts.eligible_sessions == 0


def _episode(
    detection_id: str,
    classification: str,
    first_ms: int,
    last_ms: int,
    gross: str,
    fees: str,
    net: str,
) -> dict[str, object]:
    return {
        "episode_id": f"{STUDY}|{detection_id}",
        "study_id": STUDY,
        "session_id": SESSION,
        "detection_id": detection_id,
        "relationship_id": "R-verified",
        "strategy_id": "s1",
        "classification": classification,
        "status": "UPDATED",
        "first_ms": first_ms,
        "last_ms": last_ms,
        "close_reason": None,
        "gross_edge": gross,
        "fees": fees,
        "net_edge": net,
        "quantity": "2.00",
    }

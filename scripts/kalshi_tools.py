"""Offline Kalshi operator commands.

``doctor`` and ``discover`` do not open a socket unless the process environment
already has both authorization flags, a key id, and a private-key path. This
script never sets those flags. ``run`` refuses to start unless DATA_SOURCE is
already ``kalshi_authorized``. ``study-report`` reads stored detection rows
and does not include raw order books.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from consistency_connectors.kalshi.families import relationship_proposals
from consistency_connectors.kalshi.study import (
    build_report,
    operator_review_bundle,
    report_csv,
    report_json,
    report_markdown,
)
from consistency_connectors.settings import Settings
from consistency_connectors.sources.kalshi import KalshiAccessRefusedError, KalshiDataSource
from consistency_core.models.market import Catalog


def _flags_and_credentials() -> bool:
    return (
        os.environ.get("ENABLE_KALSHI_API", "").lower() == "true"
        and os.environ.get("KALSHI_AUTHORIZATION_CONFIRMED", "").lower() == "true"
        and bool(os.environ.get("KALSHI_API_KEY_ID", "").strip())
        and bool(os.environ.get("KALSHI_PRIVATE_KEY_PATH", "").strip())
    )


def doctor() -> int:
    print("Kalshi doctor")
    print(f"DATA_SOURCE={os.environ.get('DATA_SOURCE', 'synthetic')}")
    print(f"ENABLE_KALSHI_API={os.environ.get('ENABLE_KALSHI_API', 'false')}")
    confirmed = os.environ.get("KALSHI_AUTHORIZATION_CONFIRMED", "false")
    print(f"KALSHI_AUTHORIZATION_CONFIRMED={confirmed}")
    print(f"credentials_present={_flags_and_credentials()}")
    print("raw_order_books_are_not_printed=true")
    if not _flags_and_credentials():
        print("live_check=blocked")
        print(
            "Set ENABLE_KALSHI_API, KALSHI_AUTHORIZATION_CONFIRMED, "
            "KALSHI_API_KEY_ID, and KALSHI_PRIVATE_KEY_PATH in the environment. "
            "Do not paste the private key into the shell history."
        )
        return 2
    settings = Settings()
    source = KalshiDataSource(settings)

    async def _check() -> int:
        try:
            result = await source.connectivity_check()
        except KalshiAccessRefusedError as exc:
            print(f"live_check=refused {exc}")
            return 2
        print(f"live_check={json.dumps(result, sort_keys=True)}")
        return 0 if result.get("ok") is True else 2

    return asyncio.run(_check())


def discover() -> int:
    out = Path(os.environ.get("KALSHI_REVIEW_BUNDLE", "reports/kalshi-review-bundle.json"))
    if not _flags_and_credentials() or os.environ.get("DATA_SOURCE") != "kalshi_authorized":
        bundle = operator_review_bundle([])
        bundle["status"] = "NOT_STARTED"
        bundle["note"] = (
            "No live catalog was loaded. Authorization flags and credentials were not "
            "both present, or DATA_SOURCE was not already kalshi_authorized."
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote {out} status=NOT_STARTED")
        return 2
    settings = Settings()
    source = KalshiDataSource(settings)

    async def _load() -> int:
        markets = await source.get_markets()
        catalog = Catalog(
            series=tuple(await source.get_series()),
            events=tuple(await source.get_events()),
            markets=tuple(markets),
        )
        proposals = relationship_proposals(catalog, as_of=datetime.now(UTC))
        bundle = operator_review_bundle(proposals)
        bundle["operator_review_path"] = (
            "One batch for the selected event families. Fill settlement fields with "
            "apply_settlement_overlay, then pin rules hashes in the manual-review YAML "
            "consumed by apply_reviews. This file does not grant VERIFIED. Titles are not evidence."
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote {out} markets={len(markets)} verified=0")
        return 0

    return asyncio.run(_load())


def run() -> int:
    if os.environ.get("DATA_SOURCE") != "kalshi_authorized":
        print(
            "refusing to set DATA_SOURCE. Export DATA_SOURCE=kalshi_authorized first.",
            file=sys.stderr,
        )
        return 2
    if not _flags_and_credentials():
        print(
            "Kalshi run blocked: authorization flags and credentials are not both set.",
            file=sys.stderr,
        )
        return 2
    from consistency_worker.__main__ import main

    return main(["run"])


async def _report_from_database(url: str, study_id: str, now: datetime) -> dict[str, object]:
    from consistency_persistence.db import make_engine, make_sessionmaker
    from consistency_persistence.study_ledger import StudyLedger

    engine = make_engine(url)
    try:
        facts = await StudyLedger(make_sessionmaker(engine)).load(study_id)
    finally:
        await engine.dispose()
    exclusions = list(facts.exclusions)
    if not facts.found:
        exclusions.append("no kalshi_authorized study row matched STUDY_ID")
    return build_report(
        study_id=facts.study_id or study_id,
        study_start=facts.planned_start,
        study_end=facts.planned_end,
        now=now,
        commit=facts.commit_sha or "unknown",
        config_fingerprint=facts.config_fingerprint or "unknown",
        environment=facts.environment or os.environ.get("KALSHI_ENVIRONMENT", "production"),
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
        exclusions=exclusions,
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


def study_report() -> int:
    now = datetime.now(UTC)
    url = os.environ.get("DATABASE_URL", "").strip()
    study_id = os.environ.get("STUDY_ID", "").strip()
    if url and study_id:
        report = asyncio.run(_report_from_database(url, study_id, now))
    else:
        report = build_report(
            study_id=study_id,
            study_start=os.environ.get("STUDY_START", ""),
            study_end=os.environ.get("STUDY_END", ""),
            now=now,
            commit=os.environ.get("GIT_COMMIT", "unknown"),
            config_fingerprint=os.environ.get("CONFIG_FINGERPRINT", "unknown"),
            environment=os.environ.get("KALSHI_ENVIRONMENT", "production"),
            market_ids=[],
            relationship_ids=[],
            verified_relationship_ids=[],
            fee_schedule_ids=["kalshi-2026-07-07"],
            raw_retention_authorized=(
                os.environ.get("THIRD_PARTY_RAW_PERSISTENCE_AUTHORIZED", "").lower() == "true"
            ),
            replay_available=False,
            rows=[],
            analyzed_updates=None,
            synchronized_evaluations=None,
            coverage_established=False,
            eligible_sessions=0,
        )
    directory = Path(os.environ.get("KALSHI_REPORT_DIR", "reports"))
    directory.mkdir(parents=True, exist_ok=True)
    json_path = directory / "kalshi-study.json"
    md_path = directory / "kalshi-study.md"
    json_path.write_text(report_json(report), encoding="utf-8")
    md_path.write_text(report_markdown(report), encoding="utf-8")
    csv_path = directory / "kalshi-study.csv"
    csv_path.write_text(report_csv(report), encoding="utf-8")
    draft = directory / "kalshi-study-x-draft.txt"
    draft.write_text(str(report.get("unpublished_x_draft", "")) + "\n", encoding="utf-8")
    print(f"window={report['window_status']} json={json_path} markdown={md_path}")
    print("draft_post_not_published=true")
    return 0


def main(argv: list[str]) -> int:
    command = argv[1] if len(argv) > 1 else ""
    if command == "doctor":
        return doctor()
    if command == "discover":
        return discover()
    if command == "run":
        return run()
    if command == "study-report":
        return study_report()
    print("usage: kalshi_tools.py doctor|discover|run|study-report", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

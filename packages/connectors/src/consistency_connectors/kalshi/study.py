"""Observation episodes and the derived research report.

An opportunity episode is one detection lifecycle: the open, update, and close
of a single detection id. Book updates inside that lifecycle are the same
continuous discrepancy and are not extra opportunities. A later detection id is
a new episode, because the previous one closed or the books became untrusted.
Unsynchronized gaps are not bridged.

COMPLETE is refused unless persisted synchronized coverage reaches both ends of
the planned window. A manually entered end date is not that evidence.

Funnel counts name their population. A filter rejection share is not an
empirical false-positive rate: this repository has no labelled false positives.

Nothing here is an executed trade or a guaranteed fill.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from consistency_core.money import dec


@dataclass(frozen=True)
class DetectionEpisode:
    detection_id: str
    relationship_id: str
    strategy_id: str
    classification: str
    status: str
    first_ms: int
    last_ms: int
    close_reason: str | None
    gross_edge: Decimal | None
    fees: Decimal | None
    net_edge: Decimal | None
    quantity: Decimal | None
    fee_verified: bool


def _money(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool | float):
        return None
    try:
        return dec(value)
    except (ValueError, TypeError, ArithmeticError):
        return None


def _int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def episode_from_row(row: Mapping[str, object]) -> DetectionEpisode | None:
    detection_id = row.get("detection_id")
    relationship_id = row.get("relationship_id")
    if not isinstance(detection_id, str) or not isinstance(relationship_id, str):
        return None
    strategy = row.get("strategy_id")
    classification = row.get("classification")
    status = row.get("status")
    first_ms = _int(row.get("first_observed_ms"))
    last_ms = _int(row.get("last_observed_ms"))
    if (
        not isinstance(strategy, str)
        or not isinstance(classification, str)
        or not isinstance(status, str)
        or first_ms is None
        or last_ms is None
    ):
        return None
    close = row.get("close_reason")
    return DetectionEpisode(
        detection_id=detection_id,
        relationship_id=relationship_id,
        strategy_id=strategy,
        classification=classification,
        status=status,
        first_ms=first_ms,
        last_ms=last_ms,
        close_reason=close if isinstance(close, str) else None,
        gross_edge=_money(row.get("gross_edge")),
        fees=_money(row.get("fees")),
        net_edge=_money(row.get("net_edge")),
        quantity=_money(row.get("depth_supported_quantity")),
        fee_verified=classification == "FEE_ADJUSTED_CANDIDATE",
    )


# Non-overlapping partition of recorded evaluations. Every evaluation is in one bucket.
EVALUATION_BUCKETS: dict[str, str] = {
    "INVALID_RELATIONSHIP": "rule_verification_rejection",
    "UNSYNCHRONIZED_DATA": "market_data_quality_exclusion",
    "STALE_DATA": "market_data_quality_exclusion",
    "INSUFFICIENT_LIQUIDITY": "market_data_quality_exclusion",
    "NO_OPPORTUNITY": "no_theoretical_deviation",
    "THEORETICAL_ONLY": "theoretical_deviation",
    "DEPTH_SUPPORTED": "depth_supported",
    "FEE_UNVERIFIED": "fee_unverified",
    "FEE_ADJUSTED_CANDIDATE": "fee_verified",
}

THEORETICAL_CLASSES = frozenset(
    {"THEORETICAL_ONLY", "DEPTH_SUPPORTED", "FEE_UNVERIFIED", "FEE_ADJUSTED_CANDIDATE"}
)

FUNNEL_ORDER: tuple[str, ...] = (
    "rule_verification_rejection",
    "market_data_quality_exclusion",
    "no_theoretical_deviation",
    "theoretical_deviation",
    "depth_supported",
    "fee_unverified",
    "fee_verified",
    "unclassified",
)


def distinct_episodes(rows: Sequence[Mapping[str, object]]) -> list[DetectionEpisode]:
    """One opportunity per detection id. Updates of that id are one discrepancy."""
    chosen: dict[str, DetectionEpisode] = {}
    for row in rows:
        episode = episode_from_row(row)
        if episode is None:
            continue
        previous = chosen.get(episode.detection_id)
        if previous is None or episode.last_ms >= previous.last_ms:
            chosen[episode.detection_id] = episode
    return [chosen[key] for key in sorted(chosen)]


def _max_decimal(values: list[Decimal]) -> str | None:
    if not values:
        return None
    return format(max(values), "f")


def _duration(episode: DetectionEpisode) -> int:
    return max(0, episode.last_ms - episode.first_ms)


def evaluation_funnel(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    """Partition of the evaluation population. Shares name that population."""
    counts: dict[str, int] = dict.fromkeys(FUNNEL_ORDER, 0)
    synchronized = 0
    for row in rows:
        classification = row.get("classification")
        name = classification if isinstance(classification, str) else ""
        bucket = EVALUATION_BUCKETS.get(name, "unclassified")
        counts[bucket] = counts.get(bucket, 0) + 1
        if row.get("synchronized") is True:
            synchronized += 1
    total = len(rows)
    non_overlapping = {name: counts.get(name, 0) for name in FUNNEL_ORDER if counts.get(name, 0)}
    if counts.get("unclassified", 0):
        non_overlapping["unclassified"] = counts["unclassified"]
    return {
        "population": "all recorded evaluations, including negative results",
        "population_count": total,
        "synchronized_evaluations": synchronized,
        "non_overlapping": non_overlapping,
        "non_overlapping_sums_to_population": sum(non_overlapping.values()) == total,
        "not_an_empirical_false_positive_rate": True,
    }


def determine_window(
    *,
    study_start: str,
    study_end: str,
    now: datetime,
    observed_ms: int | None,
    episodes: int,
    eligible_sessions: int,
    coverage_established: bool,
) -> str:
    """A past end date is INCOMPLETE until coverage evidence says the window finished."""
    finished = bool(study_end) and study_end <= now.astimezone(UTC).isoformat()
    has_window = bool(study_start) or episodes > 0 or (observed_ms is not None and observed_ms > 0)
    if eligible_sessions <= 0 and episodes <= 0 and (observed_ms is None or observed_ms == 0):
        if finished and study_start:
            return "INCOMPLETE"
        return "NOT_STARTED" if not has_window else "IN_PROGRESS"
    if finished and coverage_established and eligible_sessions > 0:
        return "COMPLETE"
    if finished:
        return "INCOMPLETE"
    if has_window or eligible_sessions > 0:
        return "IN_PROGRESS"
    return "NOT_STARTED"


def build_report(
    *,
    study_id: str,
    study_start: str,
    study_end: str,
    now: datetime,
    commit: str,
    config_fingerprint: str,
    environment: str,
    market_ids: Sequence[str],
    relationship_ids: Sequence[str],
    verified_relationship_ids: Sequence[str],
    fee_schedule_ids: Sequence[str],
    raw_retention_authorized: bool,
    replay_available: bool,
    rows: Sequence[Mapping[str, object]],
    analyzed_updates: int | None,
    synchronized_evaluations: int | None,
    feed: Mapping[str, object] | None = None,
    observed_ms: int | None = None,
    synchronized_ms: int | None = None,
    exclusions: Sequence[str] = (),
    coverage_established: bool = False,
    eligible_sessions: int = 0,
    evaluation_rows: Sequence[Mapping[str, object]] = (),
    ledger: Mapping[str, object] | None = None,
    membership: Sequence[Mapping[str, object]] = (),
) -> dict[str, object]:
    episodes = distinct_episodes(rows)
    by_class = Counter(item.classification for item in episodes)
    window = determine_window(
        study_start=study_start,
        study_end=study_end,
        now=now,
        observed_ms=observed_ms,
        episodes=len(episodes),
        eligible_sessions=eligible_sessions,
        coverage_established=coverage_established,
    )
    warnings: list[str] = []
    if window == "INCOMPLETE":
        warnings.append(
            "incomplete_coverage: persisted synchronized intervals do not cover the planned window"
        )
    if study_end and window != "COMPLETE":
        warnings.append("a planned end date is not evidence that monitoring finished")
    if replay_available and not raw_retention_authorized:
        replay_available = False
        warnings.append("replay was claimed without raw retention and has been refused")
    if evaluation_rows:
        funnel: dict[str, object] = evaluation_funnel(evaluation_rows)
        synchronized_evaluations = int(cast(int, funnel["synchronized_evaluations"]))
    else:
        funnel = {
            "population": "all recorded evaluations, including negative results",
            "population_count": None,
            "note": "no evaluation ledger rows were supplied",
            "not_an_empirical_false_positive_rate": True,
        }
    fee_positive = [item for item in episodes if item.classification == "FEE_ADJUSTED_CANDIDATE"]
    depth = [item for item in episodes if item.classification == "DEPTH_SUPPORTED"]
    unverified_fee = [item for item in episodes if item.classification == "FEE_UNVERIFIED"]
    theoretical = [
        item
        for item in episodes
        if item.classification
        in ("THEORETICAL_ONLY", "DEPTH_SUPPORTED", "FEE_UNVERIFIED", "FEE_ADJUSTED_CANDIDATE")
    ]
    rejected = [
        item
        for item in episodes
        if item.classification
        not in ("FEE_ADJUSTED_CANDIDATE", "DEPTH_SUPPORTED", "NO_OPPORTUNITY")
    ]

    def edges(items: Sequence[DetectionEpisode], attr: str) -> dict[str, str | None]:
        values = [value for item in items if isinstance(value := getattr(item, attr), Decimal)]
        return {"max": _max_decimal(values), "count_with_value": str(len(values))}

    top = sorted(
        fee_positive,
        key=lambda item: item.net_edge or Decimal("-1"),
        reverse=True,
    )[:10]
    document = {
        "study_id": study_id or "unconfigured",
        "window_status": window,
        "generated_at": now.astimezone(UTC).isoformat(),
        "study_start": study_start or None,
        "study_end": study_end or None,
        "observed_duration_ms": observed_ms,
        "synchronized_monitoring_ms": synchronized_ms,
        "calendar_time_is_not_synchronized_time": True,
        "environment": environment,
        "commit": commit,
        "config_fingerprint": config_fingerprint,
        "markets": list(market_ids),
        "relationships": list(relationship_ids),
        "verified_relationships": list(verified_relationship_ids),
        "fee_schedule_ids": list(fee_schedule_ids),
        "raw_retention_authorized": raw_retention_authorized,
        "replay_available": replay_available,
        "analyzed_updates": analyzed_updates,
        "synchronized_evaluations": synchronized_evaluations,
        "distinct_episodes": len(episodes),
        "classification_counts": dict(sorted(by_class.items())),
        "theoretical_inconsistency_episodes": len(theoretical),
        "rejected_episodes": len(rejected),
        "depth_supported_episodes": len(depth),
        "fee_verified_positive_candidates": len(fee_positive),
        "unverified_fee_candidates": len(unverified_fee),
        "episode_duration_ms": {
            "max": max((_duration(item) for item in episodes), default=0),
            "count": len(episodes),
        },
        "quantity": {
            "max_depth_supported": _max_decimal(
                [item.quantity for item in episodes if item.quantity is not None]
            ),
        },
        "gross_edge": edges(theoretical, "gross_edge"),
        "modeled_fees": edges(theoretical, "fees"),
        "net_theoretical_edge": edges(theoretical, "net_edge"),
        "fee_verified_edge_is_reported": bool(fee_positive),
        "top_fee_verified_candidates": [
            {
                "detection_id": item.detection_id,
                "relationship_id": item.relationship_id,
                "strategy_id": item.strategy_id,
                "net_edge": None if item.net_edge is None else format(item.net_edge, "f"),
                "quantity": None if item.quantity is None else format(item.quantity, "f"),
                "first_ms": item.first_ms,
                "last_ms": item.last_ms,
            }
            for item in top
        ],
        "feed": None if feed is None else {key: feed[key] for key in sorted(feed)},
        "eligible_sessions": eligible_sessions,
        "coverage_established": coverage_established,
        "coverage_warnings": warnings,
        "membership_intervals": [dict(item) for item in membership],
        "evaluation_funnel": funnel,
        "overlapping_views": {
            "theoretical_inconsistency_episodes": {
                "count": len(theoretical),
                "population": (
                    "distinct opportunity episodes whose classification passed "
                    "the top-of-book edge gate"
                ),
                "includes": sorted(THEORETICAL_CLASSES),
                "note": (
                    "This view overlaps theoretical, depth-supported, fee-unverified, "
                    "and fee-verified episodes. It is not an empirical false-positive rate."
                ),
            }
        },
        "ledger": None if ledger is None else {key: ledger[key] for key in sorted(ledger)},
        "manual_exclusions": list(exclusions),
        "limitations": [
            "A candidate is not an executed trade and not a guaranteed fill.",
            "Independently observed books are not an atomic multi-market execution.",
            "Fee-adjusted candidates require a verified fee resolution. Zero is a valid result.",
            "Unsynchronized or stale books are not counted as verified positive opportunities.",
            "Raw provider order books are omitted from this report.",
            "Replay of raw Kalshi books is available only when raw retention was authorized.",
            "One opportunity episode is one detection id, not one book update.",
            "Filter shares use the named evaluation population and are not false-positive rates.",
        ],
    }
    document["unpublished_x_draft"] = _draft(document)
    return cast(dict[str, object], document)


def _count(value: object) -> int:
    return len(value) if isinstance(value, list) else 0


def report_markdown(document: Mapping[str, object]) -> str:
    status = document.get("window_status")
    lines = [
        "# Kalshi observation report",
        "",
        f"Window status: **{status}**.",
        "",
        "This report does not describe an executed arbitrage trade.",
        "",
        f"- Study: {document.get('study_id')}",
        f"- Start: {document.get('study_start')}",
        f"- End: {document.get('study_end')}",
        f"- Observed duration (ms): {document.get('observed_duration_ms')}",
        f"- Synchronized monitoring (ms): {document.get('synchronized_monitoring_ms')}",
        f"- Software commit: {document.get('commit')}",
        f"- Config fingerprint: {document.get('config_fingerprint')}",
        f"- Environment: {document.get('environment')}",
        f"- Markets in scope: {_count(document.get('markets'))}",
        f"- Relationships in scope: {_count(document.get('relationships'))}",
        f"- Verified relationships: {_count(document.get('verified_relationships'))}",
        f"- Fee schedules: {document.get('fee_schedule_ids')}",
        f"- Raw retention authorized: {document.get('raw_retention_authorized')}",
        f"- Raw replay available: {document.get('replay_available')}",
        f"- Analyzed updates: {document.get('analyzed_updates')}",
        f"- Synchronized evaluations: {document.get('synchronized_evaluations')}",
        f"- Coverage established: {document.get('coverage_established')}",
        f"- Coverage warnings: {document.get('coverage_warnings')}",
        f"- Eligible real sessions: {document.get('eligible_sessions')}",
        f"- Evaluation funnel: {document.get('evaluation_funnel')}",
        f"- Distinct episodes: {document.get('distinct_episodes')}",
        f"- Classification counts: {document.get('classification_counts')}",
        (
            f"- Theoretical inconsistency episodes: "
            f"{document.get('theoretical_inconsistency_episodes')}"
        ),
        f"- Depth-supported episodes: {document.get('depth_supported_episodes')}",
        f"- Fee-verified positive candidates: {document.get('fee_verified_positive_candidates')}",
        f"- Unverified-fee candidates: {document.get('unverified_fee_candidates')}",
        "",
        "## Limitations",
        "",
    ]
    limitations = document.get("limitations")
    if isinstance(limitations, list):
        lines.extend(f"- {item}" for item in limitations)
    lines.extend(
        [
            "",
            "## Draft post (not published)",
            "",
            _draft(document),
            "",
        ]
    )
    return "\n".join(lines)


def report_json(document: Mapping[str, object]) -> str:
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


def report_csv(document: Mapping[str, object]) -> str:
    """One summary row plus one row per non-overlapping funnel bucket."""
    headers = (
        "study_id",
        "window_status",
        "eligible_sessions",
        "observed_duration_ms",
        "synchronized_monitoring_ms",
        "analyzed_updates",
        "synchronized_evaluations",
        "distinct_episodes",
        "fee_verified_positive_candidates",
        "population",
        "bucket",
        "bucket_count",
    )
    funnel = document.get("evaluation_funnel")
    population = ""
    buckets: dict[str, object] = {}
    synchronized = document.get("synchronized_evaluations")
    if isinstance(funnel, Mapping):
        pop = funnel.get("population_count")
        population = "" if pop is None else str(pop)
        raw_buckets = funnel.get("non_overlapping")
        if isinstance(raw_buckets, Mapping):
            buckets = dict(raw_buckets)
        synchronized = funnel.get("synchronized_evaluations")
    base = [
        _cell(document.get("study_id")),
        _cell(document.get("window_status")),
        _cell(document.get("eligible_sessions")),
        _cell(document.get("observed_duration_ms")),
        _cell(document.get("synchronized_monitoring_ms")),
        _cell(document.get("analyzed_updates")),
        _cell(synchronized),
        _cell(document.get("distinct_episodes")),
        _cell(document.get("fee_verified_positive_candidates")),
        population,
    ]
    lines = [",".join(headers)]
    if not buckets:
        lines.append(",".join([*base, "", ""]))
    for name in sorted(buckets):
        lines.append(",".join([*base, name, _cell(buckets[name])]))
    return "\n".join(lines) + "\n"


def _cell(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    if any(ch in text for ch in ',"\n'):
        return '"' + text.replace('"', '""') + '"'
    return text


def _draft(document: Mapping[str, object]) -> str:
    status = document.get("window_status")
    positives = document.get("fee_verified_positive_candidates")
    if status == "INCOMPLETE":
        return (
            "INCOMPLETE. A planned end date is not enough. Persisted coverage does not "
            "establish the monitoring window. No fee-adjusted result is being reported "
            "as a finding."
        )
    if status != "COMPLETE":
        return (
            "IN PROGRESS. The Consistency Engine has not finished a measured Kalshi "
            "observation window. No fee-adjusted result is being reported as a finding."
        )
    return (
        f"Measured Kalshi window {document.get('study_id')}: "
        f"{positives} fee-verified positive candidate episode(s). "
        "These are theoretical candidates, not executed trades or guaranteed fills."
    )


def operator_review_bundle(
    proposals: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Batch of unverified proposals. This file does not grant VERIFIED."""
    return {
        "schema_version": 1,
        "instructions": (
            "Each item is a candidate. Do not treat title similarity as verification. "
            "A review can be applied through the existing manual-review file only after "
            "the rules hashes are checked. This bundle is not itself a verification."
        ),
        "proposals": list(proposals),
    }

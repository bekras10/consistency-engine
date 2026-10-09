"""Retention selection is pure; the database job is covered by the integration suite."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from consistency_persistence.retention import (
    RetentionPolicy,
    SessionRetentionView,
    sessions_losing_raw_data,
)

NOW = datetime(2026, 10, 9, tzinfo=UTC)
POLICY = RetentionPolicy(max_age_hours=168, max_sessions=2, third_party_hours=0)


def _view(
    session_id: str,
    *,
    hours_ago: int,
    kind: str = "synthetic",
    pinned: bool = False,
    raw: bool = True,
) -> SessionRetentionView:
    return SessionRetentionView(
        session_id=session_id,
        started_at=NOW - timedelta(hours=hours_ago),
        source_kind=kind,
        pinned=pinned,
        raw_persisted=raw,
    )


def test_pinned_demo_sessions_are_kept() -> None:
    drop = sessions_losing_raw_data(
        [_view("demo", hours_ago=10_000, pinned=True), _view("fresh", hours_ago=1)],
        now=NOW,
        policy=POLICY,
    )
    assert drop == []


def test_third_party_raw_is_removed_immediately_by_default() -> None:
    drop = sessions_losing_raw_data(
        [_view("kalshi", hours_ago=0, kind="kalshi_authorized")],
        now=NOW,
        policy=POLICY,
    )
    assert drop == ["kalshi"]


def test_oldest_unpinned_sessions_fall_off_the_cap() -> None:
    drop = sessions_losing_raw_data(
        [
            _view("a", hours_ago=10),
            _view("b", hours_ago=5),
            _view("c", hours_ago=1),
        ],
        now=NOW,
        policy=POLICY,
    )
    assert drop == ["a"]


def test_aged_out_sessions_lose_raw_data_before_the_cap() -> None:
    drop = sessions_losing_raw_data(
        [_view("old", hours_ago=200), _view("new", hours_ago=1)],
        now=NOW,
        policy=POLICY,
    )
    assert drop == ["old"]

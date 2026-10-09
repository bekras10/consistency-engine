"""Dashboard figures stay on fixed-point strings and keep current edge separate from the max."""

from __future__ import annotations

from decimal import Decimal

import pytest

from consistency_persistence.dashboard import (
    activity_buckets,
    at_least,
    current_figures,
    explain_detection,
    latency_ns_from_record,
    money,
    percentile_nearest,
)


def test_money_is_fixed_point_text() -> None:
    assert money(Decimal("0.10")) == "0.10"
    assert money(Decimal("0")) == "0"
    assert money(None) is None


def test_current_net_edge_is_not_the_historical_maximum() -> None:
    record = {
        "metrics": {
            "net_edge": "0.04",
            "gross_edge": "0.10",
            "total_fees": "0.06",
            "theoretical_deviation": "0.35",
            "depth_supported_quantity": "3.00",
        },
        "max_net_edge": "0.10",
        "max_deviation": "0.35",
        "max_capacity": "100",
    }
    figures = current_figures(record, Decimal("0.10"))
    assert figures["net_edge"] == "0.04"
    assert figures["max_net_edge"] == "0.10"
    assert figures["gross_edge"] == "0.10"
    assert at_least(figures["net_edge"], Decimal("0.05")) is False
    assert at_least(figures["net_edge"], Decimal("0.04")) is True


def test_latency_uses_stored_nanoseconds_only() -> None:
    assert (
        latency_ns_from_record(
            {"timing": {"processing_started_ns": 100, "detection_completed_ns": 250}}
        )
        == 150
    )
    assert latency_ns_from_record({"timing": {"processing_started_ns": True}}) is None
    assert latency_ns_from_record({}) is None
    assert percentile_nearest([], 50) is None
    assert percentile_nearest([10, 20, 30, 40], 50) == 20
    assert percentile_nearest([10, 20, 30, 40], 95) == 40


def test_percentile_rejects_out_of_range() -> None:
    with pytest.raises(ValueError, match="percent must be"):
        percentile_nearest([1], 0)


def test_activity_buckets_count_stored_timestamps() -> None:
    buckets = activity_buckets([1_000, 1_100, 2_500])
    assert sum(int(bucket["count"]) for bucket in buckets) == 3


def test_explanation_names_current_and_maximum() -> None:
    text = explain_detection(
        {
            "relationship_id": "rel-1",
            "relationship_type": "IMPLICATION",
            "classification": "FEE_ADJUSTED_CANDIDATE",
            "status": "OPEN",
            "net_edge": "0.04",
            "max_net_edge": "0.10",
        }
    )
    assert "0.04" in text
    assert "0.10" in text
    assert "not a realized trade" in text

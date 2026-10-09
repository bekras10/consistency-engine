"""Multi-level depth chart cumulative quantities (fixtures/golden/depth-multilevel.yaml).

YES bids accumulate from the highest price to the lowest:
  0.60 → 10.00
  0.55 → 10.00 + 20.50 = 30.50
  0.40 → 30.50 + 5.00 = 35.50
  0.3333 → 35.50 + 1.25 = 36.75

YES asks accumulate from the lowest price to the highest:
  0.62 → 8.00
  0.70 → 8.00 + 12.25 = 20.25
  0.80 → 20.25 + 4.00 = 24.25

Prices in the result are the input strings (``0.40`` stays ``0.40``).
"""

from __future__ import annotations

from typing import Any

import pytest

from consistency_persistence.depth_chart import depth_chart_points
from tests.golden.support import load

pytestmark = pytest.mark.golden


def _by_price(points: list[dict[str, str]]) -> dict[str, str]:
    return {point["price"]: point["cumulative"] for point in points}


def test_multilevel_depth_chart_cumulative_quantities() -> None:
    fixture: dict[str, Any] = load("depth-multilevel")
    points = depth_chart_points(fixture["bids"], fixture["asks"])
    assert _by_price(points["bids"]) == fixture["bids_cumulative"]
    assert _by_price(points["asks"]) == fixture["asks_cumulative"]
    assert [point["price"] for point in points["bids"]] == ["0.60", "0.55", "0.40", "0.3333"]
    assert [point["price"] for point in points["asks"]] == ["0.62", "0.70", "0.80"]

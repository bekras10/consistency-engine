"""Cumulative depth for the market chart.

Bids accumulate from the highest price to the lowest. Asks accumulate from the
lowest price to the highest. Prices and quantities stay fixed-point strings.
"""

from __future__ import annotations

from decimal import Decimal


def _accumulate(levels: list[dict[str, str]], *, high_to_low: bool) -> list[dict[str, str]]:
    ordered = sorted(levels, key=lambda level: Decimal(level["price"]), reverse=high_to_low)
    running = Decimal("0")
    points: list[dict[str, str]] = []
    for level in ordered:
        running += Decimal(level["quantity"])
        points.append({"price": level["price"], "cumulative": format(running, "f")})
    return points


def depth_chart_points(
    bids: list[dict[str, str]], asks: list[dict[str, str]]
) -> dict[str, list[dict[str, str]]]:
    return {
        "bids": _accumulate(bids, high_to_low=True),
        "asks": _accumulate(asks, high_to_low=False),
    }

"""Read-only Kalshi adapter. No order, portfolio, or balance calls."""

from consistency_connectors.kalshi.endpoints import ReadOnlyViolationError

__all__ = ["ReadOnlyViolationError"]

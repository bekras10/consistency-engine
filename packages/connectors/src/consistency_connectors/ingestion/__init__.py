"""Ingestion core: local book manager, bounded queues, async runner."""

from consistency_connectors.ingestion.book_manager import (
    BookManager,
    BookUpdate,
    DesyncReason,
    Timing,
)
from consistency_connectors.ingestion.queues import CoalescingQueue, merge_book_updates
from consistency_connectors.ingestion.runner import (
    Backoff,
    IngestionRunner,
    RunnerListener,
    RunnerLoss,
)

__all__ = [
    "Backoff",
    "BookManager",
    "BookUpdate",
    "CoalescingQueue",
    "DesyncReason",
    "IngestionRunner",
    "RunnerListener",
    "RunnerLoss",
    "Timing",
    "merge_book_updates",
]

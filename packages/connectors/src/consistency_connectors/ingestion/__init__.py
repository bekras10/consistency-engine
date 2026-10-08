"""Ingestion core: local book manager, bounded queues, async runner."""

from consistency_connectors.ingestion.book_manager import (
    BookManager,
    BookUpdate,
    DesyncReason,
    Timing,
)
from consistency_connectors.ingestion.queues import CoalescingQueue
from consistency_connectors.ingestion.runner import Backoff, IngestionRunner

__all__ = [
    "Backoff",
    "BookManager",
    "BookUpdate",
    "CoalescingQueue",
    "DesyncReason",
    "IngestionRunner",
    "Timing",
]

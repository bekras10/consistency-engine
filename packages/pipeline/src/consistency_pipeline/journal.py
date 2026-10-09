"""The session journal: everything needed to reproduce a session exactly (spec 12.1).

A journal is the ordered sequence of :class:`JournalEntry` items a pipeline processed: every
stream message as delivered (snapshots, deltas, heartbeats, connection/status/lifecycle events,
raw wire payloads) plus *control* entries for state changes that no market-data message carried
(runner-originated connection losses and recovery failures, relationship-set changes, live
clock ticks, end of session).

**Deterministic replay order** is ``ordinal``: the 0-based position at which the live pipeline
processed the entry. It equals the order in which ``BookManager`` applied messages, so
re-applying entries by ascending ordinal reproduces book states, evaluations, certificates and
lifecycle events exactly. ``StreamMessage.position`` (the source's own numbering) and the
exchange/receipt timestamps are preserved as recorded but are not used for ordering.
"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import model_validator

from consistency_connectors.base import RecoveryRequest
from consistency_core.events import StreamMessage
from consistency_core.models.common import FrozenModel
from consistency_core.models.relationship import Relationship

ControlAction = Literal[
    "connection_lost", "recovery_failed", "relationships_changed", "tick", "session_end"
]


class ControlEvent(FrozenModel):
    type: Literal["control"] = "control"
    action: ControlAction
    now_ms: int
    """Pipeline clock used when the entry was processed."""
    reason: str | None = None
    connection_ids: tuple[str, ...] = ()
    recovery_request: RecoveryRequest | None = None
    relationships: tuple[Relationship, ...] | None = None
    relationships_version: str | None = None


class JournalEntry(FrozenModel):
    ordinal: int
    message: StreamMessage | None = None
    control: ControlEvent | None = None

    @model_validator(mode="after")
    def _one(self) -> Self:
        if (self.message is None) == (self.control is None):
            raise ValueError("a journal entry holds exactly one of message / control")
        return self

    @property
    def now_ms(self) -> int:
        if self.message is not None:
            return self.message.received_ts_ms
        assert self.control is not None
        return self.control.now_ms

    @property
    def kind(self) -> str:
        if self.message is not None:
            return self.message.event.type
        assert self.control is not None
        return f"control:{self.control.action}"

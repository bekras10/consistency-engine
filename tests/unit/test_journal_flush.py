"""A failed journal commit must not drop the entries waiting to be written."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from consistency_persistence.recording import BatchJournal, VersionStamp
from consistency_pipeline.journal import ControlEvent, JournalEntry


class _Session:
    fail_commit: ClassVar[bool] = True
    committed: ClassVar[list[list[int]]] = []

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_exc: object) -> bool:
        return False

    def begin(self) -> _Begin:
        return _Begin(self)

    async def execute(self, _statement: object, params: object = None) -> None:
        if isinstance(params, list):
            self.rows.extend(row for row in params if isinstance(row, dict))


class _Begin:
    def __init__(self, session: _Session) -> None:
        self.session = session

    async def __aenter__(self) -> _Session:
        return self.session

    async def __aexit__(self, exc_type: object, _exc: object, _tb: object) -> bool:
        if exc_type is not None:
            self.session.rows.clear()
            return False
        if _Session.fail_commit:
            _Session.fail_commit = False
            self.session.rows.clear()
            raise RuntimeError("commit failed")
        _Session.committed.append([int(row["ordinal"]) for row in self.session.rows])
        self.session.rows.clear()
        return False


def _factory() -> _Session:
    return _Session()


def _entries() -> list[JournalEntry]:
    return [
        JournalEntry(ordinal=0, control=ControlEvent(action="tick", now_ms=1_000)),
        JournalEntry(ordinal=1, control=ControlEvent(action="tick", now_ms=2_000)),
    ]


async def test_failed_commit_preserves_buffer_and_next_flush_writes_once() -> None:
    _Session.fail_commit = True
    _Session.committed = []
    journal = BatchJournal(
        _factory,  # type: ignore[arg-type]
        "ses-flush",
        VersionStamp(
            fee_schedule_version="fees-v1",
            relationship_version="rel-v1",
            rules_hashes={},
        ),
        batch_size=10,
        enabled=True,
    )
    pending = _entries()
    for entry in pending:
        journal.append(entry)
    with pytest.raises(RuntimeError, match="commit failed"):
        await journal.flush()
    assert [entry.ordinal for entry in journal._buf] == [0, 1]
    await journal.flush()
    assert journal._buf == []
    assert _Session.committed == [[0, 1]]

"""Journal flush durability and concurrency.

A failed commit must not drop the entries waiting to be written. Two flushes in flight at
once must persist each ordinal once, in order, and must not hit the ordinal unique key.
"""

from __future__ import annotations

import asyncio
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


class _Overlap:
    """One shared table: an ordinal inserted by any in-flight flush cannot be inserted again."""

    def __init__(self) -> None:
        self.first_inside = asyncio.Event()
        self.release_first = asyncio.Event()
        self.second_inside = asyncio.Event()
        self.committed: list[list[int]] = []
        self.persisted: set[int] = set()
        self.entrants = 0


class _OverlapSession:
    def __init__(self, state: _Overlap) -> None:
        self.state = state
        self.rows: list[dict[str, Any]] = []

    async def __aenter__(self) -> _OverlapSession:
        return self

    async def __aexit__(self, *_exc: object) -> bool:
        return False

    def begin(self) -> _OverlapBegin:
        return _OverlapBegin(self)

    async def execute(self, _statement: object, params: object = None) -> None:
        if not isinstance(params, list):
            return
        incoming = [row for row in params if isinstance(row, dict)]
        ordinals = [int(row["ordinal"]) for row in incoming]
        for ordinal in ordinals:
            if ordinal in self.state.persisted:
                raise RuntimeError(f"unique constraint violation on ordinal {ordinal}")
        self.state.persisted.update(ordinals)
        self.rows.extend(incoming)


class _OverlapBegin:
    def __init__(self, session: _OverlapSession) -> None:
        self.session = session

    async def __aenter__(self) -> _OverlapSession:
        state = self.session.state
        state.entrants += 1
        if state.entrants == 1:
            state.first_inside.set()
            await state.release_first.wait()
        else:
            state.second_inside.set()
        return self.session

    async def __aexit__(self, exc_type: object, _exc: object, _tb: object) -> bool:
        if exc_type is None:
            self.session.state.committed.append([int(row["ordinal"]) for row in self.session.rows])
        else:
            for row in self.session.rows:
                self.session.state.persisted.discard(int(row["ordinal"]))
        self.session.rows.clear()
        return False


def _stamp() -> VersionStamp:
    return VersionStamp(
        fee_schedule_version="fees-v1",
        relationship_version="rel-v1",
        rules_hashes={},
    )


async def test_overlapping_flushes_persist_each_ordinal_once_in_order() -> None:
    """Two flush calls overlap inside the transaction.

    The first transaction pauses before its insert returns to the caller. The test appends
    one more entry and starts a second flush. Without a lock the second flush inserts the
    same ordinals and the stand-in unique key fails (or the same ordinal is committed
    twice). With a lock the second flush waits, then writes only the entry appended during
    the first transaction.
    """
    state = _Overlap()
    journal = BatchJournal(
        lambda: _OverlapSession(state),  # type: ignore[arg-type]
        "ses-overlap",
        _stamp(),
        batch_size=10,
        enabled=True,
    )
    for entry in _entries():
        journal.append(entry)
    first = asyncio.create_task(journal.flush())
    await asyncio.wait_for(state.first_inside.wait(), 2)
    journal.append(JournalEntry(ordinal=2, control=ControlEvent(action="tick", now_ms=3_000)))
    second = asyncio.create_task(journal.flush())
    for _ in range(20):
        if state.second_inside.is_set():
            break
        await asyncio.sleep(0)
    state.release_first.set()
    await asyncio.wait_for(asyncio.gather(first, second), 2)
    assert state.committed == [[0, 1], [2]]
    assert sorted(state.persisted) == [0, 1, 2]
    assert journal._buf == []

"""Contiguous outbox cursor. Assignment order is not commit order."""

from __future__ import annotations

from consistency_persistence.outbox import select_contiguous


def test_contiguous_prefix_while_a_gap_might_still_commit() -> None:
    assert select_contiguous([1, 2, 4], 0, writers_in_progress=True) == [1, 2]


def test_higher_committed_id_is_held_when_the_lower_id_is_missing() -> None:
    assert select_contiguous([2], 0, writers_in_progress=True) == []


def test_aborted_hole_is_skipped_when_no_transaction_is_open() -> None:
    assert select_contiguous([2, 3], 0, writers_in_progress=False) == [2, 3]


def test_cursor_does_not_move_past_a_held_gap() -> None:
    assert select_contiguous([11], 9, writers_in_progress=True) == []
    assert select_contiguous([10, 11], 9, writers_in_progress=True) == [10, 11]

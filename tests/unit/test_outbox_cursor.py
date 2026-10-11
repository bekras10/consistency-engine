"""Contiguous outbox cursor. Assignment order is not commit order."""

from __future__ import annotations

from consistency_persistence.outbox import (
    CLAIM_PAGE_SIZE,
    holes_for_claims,
    publication_prefix,
)


def test_contiguous_prefix_while_a_gap_might_still_commit() -> None:
    blocked, chosen = publication_prefix([1, 2, 4], 0, {3: "in progress"})
    assert blocked is True
    assert chosen == [1, 2]


def test_higher_committed_id_is_held_when_the_lower_id_is_missing() -> None:
    blocked, chosen = publication_prefix([2], 0, {1: "in progress"})
    assert blocked is True
    assert chosen == []


def test_aborted_hole_is_skipped_when_no_transaction_is_open() -> None:
    blocked, chosen = publication_prefix([2, 3], 0, {1: "aborted"})
    assert blocked is False
    assert chosen == [2, 3]


def test_cursor_does_not_move_past_a_held_gap() -> None:
    blocked, waiting = publication_prefix([11], 9, {10: "in progress"})
    assert blocked is True
    assert waiting == []
    blocked, delivered = publication_prefix([10, 11], 9, {})
    assert blocked is False
    assert delivered == [10, 11]


def test_unscanned_gap_is_not_treated_as_aborted() -> None:
    holes = holes_for_claims([7], 0, {5: "xid"}, {5: "committed"}, scanned_through=5)
    assert holes[6] == "unknown"
    blocked, chosen = publication_prefix([7], 0, holes)
    assert blocked is True
    assert chosen == []


def test_complete_scan_still_skips_an_aborted_hole() -> None:
    holes = holes_for_claims([7], 0, {7: "xid"}, {7: "committed"}, scanned_through=None)
    assert holes[1] == "aborted"
    blocked, chosen = publication_prefix([7], 0, holes)
    assert blocked is False
    assert chosen == [7]


def test_claim_page_size_is_a_bound() -> None:
    assert CLAIM_PAGE_SIZE >= 1
    from consistency_persistence import outbox

    source = outbox.__file__
    text = __import__("pathlib").Path(source).read_text()
    assert "LIMIT :limit" in text
    assert 'FROM outbox_claims WHERE id > :after_id ORDER BY id"' not in text


def test_committed_id_missing_from_the_snapshot_is_not_skipped() -> None:
    """Status says committed, but this snapshot has no row. Do not skip it."""
    blocked, chosen = publication_prefix([2], 0, {1: "committed"})
    assert blocked is True
    assert chosen == []

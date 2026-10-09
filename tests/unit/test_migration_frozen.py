"""The initial revision is frozen DDL, not a live copy of the ORM metadata."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REVISION = ROOT / "migrations" / "versions" / "0001_initial.py"


def test_initial_revision_does_not_follow_orm_metadata() -> None:
    text = REVISION.read_text()
    assert "create_all" not in text
    assert "drop_all" not in text
    assert "consistency_persistence" not in text
    assert "op.create_table(" in text
    assert "def downgrade" in text
    assert "op.drop_table(" in text

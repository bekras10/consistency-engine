"""Initial schema (spec section 11 plus session checkpoints).

Revision ID: 0001_initial
Revises:
Create Date: 2026-10-09

The table definitions live in ``consistency_persistence.schema`` so the migration and the
repository cannot drift. Later changes belong in new revisions, not edits to this one.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

from consistency_persistence.schema import CHECKPOINT_TABLE, SPEC_TABLES, Base

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES = (*SPEC_TABLES, CHECKPOINT_TABLE)


def upgrade() -> None:
    bind = op.get_bind()
    Base.metadata.create_all(bind)
    present = set(sa_tables(bind))
    missing = [name for name in _TABLES if name not in present]
    if missing:
        raise RuntimeError(f"migration did not create {missing}")


def downgrade() -> None:
    bind = op.get_bind()
    Base.metadata.drop_all(bind)


def sa_tables(bind: object) -> list[str]:
    from sqlalchemy import inspect

    return list(inspect(bind).get_table_names())

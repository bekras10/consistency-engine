"""Retention floor for consumed outbox ids.

Revision ID: 0004_outbox_retention
Revises: 0003_outbox_claims
Create Date: 2026-10-10

Explicit DDL. This revision does not import the ORM.

``pruned_through`` is the highest id whose outbox row and claim have been
deleted. Readers below that floor resync. They do not treat the deleted
prefix as aborted holes. Cleanup refuses an id the commit-safe watermark
has not reached, and it never deletes an id above the floor.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0004_outbox_retention"
down_revision: str | None = "0003_outbox_claims"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE outbox_retention (
            singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
            pruned_through BIGINT NOT NULL
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS outbox_retention")

"""Append-only notification outbox (Phase 11).

Revision ID: 0002_notification_outbox
Revises: 0001_initial
Create Date: 2026-10-09

Explicit DDL. This revision does not import the ORM and does not call create_all.
``id`` is BIGSERIAL: nextval runs at INSERT, before COMMIT.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0002_notification_outbox"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE notification_outbox (
            id BIGSERIAL PRIMARY KEY,
            topic TEXT NOT NULL,
            session_id TEXT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS notification_outbox")

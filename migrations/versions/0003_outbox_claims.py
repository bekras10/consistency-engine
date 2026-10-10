"""Outbox publication claims (commit-order cursor).

Revision ID: 0003_outbox_claims
Revises: 0002_notification_outbox
Create Date: 2026-10-10

Explicit DDL. This revision does not import the ORM.

``notification_outbox.id`` is still assigned before the detection transaction
commits. ``outbox_claims`` records that id and the assigning transaction's
``xid8`` in a transaction that commits first. Readers use ``pg_xact_status``
of the stored xid. They do not query ``pg_locks``.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0003_outbox_claims"
down_revision: str | None = "0002_notification_outbox"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE outbox_claims (
            id BIGINT PRIMARY KEY,
            xid TEXT NOT NULL
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS outbox_claims")

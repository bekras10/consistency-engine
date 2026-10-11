"""Store the closing evaluation apart from the qualifying certificate.

Revision ID: 0005_closing_certificate
Revises: 0004_outbox_retention
Create Date: 2026-10-11

Explicit DDL. This revision does not import the ORM and does not call create_all.

``certificate_json`` and ``certificate_hash`` stay the last qualifying proof.
The closing evaluation is written to the new columns and does not replace that proof.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0005_closing_certificate"
down_revision: str | None = "0004_outbox_retention"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE detections ADD COLUMN closing_classification TEXT")
    op.execute("ALTER TABLE detections ADD COLUMN closing_certificate_hash TEXT")
    op.execute("ALTER TABLE detections ADD COLUMN closing_certificate_json TEXT")


def downgrade() -> None:
    op.execute("ALTER TABLE detections DROP COLUMN IF EXISTS closing_certificate_json")
    op.execute("ALTER TABLE detections DROP COLUMN IF EXISTS closing_certificate_hash")
    op.execute("ALTER TABLE detections DROP COLUMN IF EXISTS closing_classification")

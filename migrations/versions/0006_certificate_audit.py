"""Audit historical certificate rows and record proof_status.

Revision ID: 0006_certificate_audit
Revises: 0005_closing_certificate
Create Date: 2026-10-11

Explicit DDL. This revision does not call create_all.

Rows whose certificate JSON already hashes to the stored hash are unchanged.
A mismatched qualifying document is replaced only by a proof reconstructed from
a checkpoint whose state digest verifies, and the hash is then the hash of those
bytes. Anything that cannot be reconstructed is marked uncertified and kept.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0006_certificate_audit"
down_revision: str | None = "0005_closing_certificate"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE detections ADD COLUMN proof_status TEXT NOT NULL DEFAULT 'certified'")
    from consistency_persistence.certificate_audit import audit_stored_proofs

    audit_stored_proofs(op.get_bind())


def downgrade() -> None:
    op.execute("ALTER TABLE detections DROP COLUMN IF EXISTS proof_status")

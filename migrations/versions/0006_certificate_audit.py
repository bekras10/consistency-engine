"""Audit historical certificate rows and record proof_status.

Revision ID: 0006_certificate_audit
Revises: 0005_closing_certificate
Create Date: 2026-10-11

Explicit DDL. This revision does not call create_all.

Rows whose certificate JSON already hashes to the stored hash are unchanged.
A mismatched qualifying document is replaced only when a journal replay or a
verified checkpoint reproduces that same stored hash and agrees with the row's
classification and any stored metrics, legs, and scenarios. The original hash
is not rewritten. Anything that cannot be linked is marked uncertified and kept.

Each affected detection is one savepoint. A repair failure quarantines that
row and does not roll back detections already certified in earlier savepoints.
If the quarantine write also fails, this upgrade raises and Alembic aborts
the revision. Checkpoints are paged per affected session. A displaced document
is stored as the closing proof only when it is a well-formed certificate
whose hash is the SHA-256 of those bytes.
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

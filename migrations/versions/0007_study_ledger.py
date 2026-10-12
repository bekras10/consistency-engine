"""Observation ledger for a study-scoped Kalshi window.

Revision ID: 0007_study_ledger
Revises: 0006_certificate_audit
Create Date: 2026-10-11

Explicit DDL. This revision does not import the ORM and does not call create_all.

Counters are not stored as incrementing totals. Events are inserted once under an
idempotency key, and aggregates are queries. Replaying the same key after a restart
does not add another observation. Opportunity episodes are one row per detection id.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0007_study_ledger"
down_revision: str | None = "0006_certificate_audit"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE observation_studies (
            study_id TEXT PRIMARY KEY,
            source_kind TEXT NOT NULL,
            environment TEXT NOT NULL,
            commit_sha TEXT NOT NULL,
            config_fingerprint TEXT NOT NULL,
            planned_start TEXT NOT NULL,
            planned_end TEXT NOT NULL,
            raw_retention_authorized BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    op.execute(
        """
        CREATE TABLE study_sessions (
            study_id TEXT NOT NULL REFERENCES observation_studies (study_id) ON DELETE CASCADE,
            session_id TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            eligible BOOLEAN NOT NULL,
            started_ms BIGINT NOT NULL,
            ended_ms BIGINT,
            PRIMARY KEY (study_id, session_id)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_study_sessions_study
            ON study_sessions (study_id, eligible, source_kind)
        """
    )
    op.execute(
        """
        CREATE TABLE study_membership_intervals (
            study_id TEXT NOT NULL REFERENCES observation_studies (study_id) ON DELETE CASCADE,
            session_id TEXT NOT NULL,
            subject_kind TEXT NOT NULL,
            subject_id TEXT NOT NULL,
            entered_ms BIGINT NOT NULL,
            exited_ms BIGINT,
            fingerprint TEXT NOT NULL,
            verification_status TEXT NOT NULL,
            PRIMARY KEY (study_id, session_id, subject_kind, subject_id, entered_ms)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_study_membership_subject
            ON study_membership_intervals (study_id, subject_kind, subject_id)
        """
    )
    op.execute(
        """
        CREATE TABLE study_events (
            idempotency_key TEXT PRIMARY KEY,
            study_id TEXT NOT NULL REFERENCES observation_studies (study_id) ON DELETE CASCADE,
            session_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            occurred_ms BIGINT NOT NULL,
            subject_id TEXT NOT NULL,
            classification TEXT NOT NULL,
            synchronized BOOLEAN NOT NULL,
            payload JSONB NOT NULL
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_study_events_kind ON study_events (study_id, session_id, kind)
        """
    )
    op.execute(
        """
        CREATE TABLE opportunity_episodes (
            episode_id TEXT PRIMARY KEY,
            study_id TEXT NOT NULL REFERENCES observation_studies (study_id) ON DELETE CASCADE,
            session_id TEXT NOT NULL,
            detection_id TEXT NOT NULL,
            relationship_id TEXT NOT NULL,
            strategy_id TEXT NOT NULL,
            classification TEXT NOT NULL,
            status TEXT NOT NULL,
            first_ms BIGINT NOT NULL,
            last_ms BIGINT NOT NULL,
            close_reason TEXT,
            gross_edge NUMERIC,
            fees NUMERIC,
            net_edge NUMERIC,
            quantity NUMERIC
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_opportunity_episodes_study
            ON opportunity_episodes (study_id, session_id)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS opportunity_episodes")
    op.execute("DROP TABLE IF EXISTS study_events")
    op.execute("DROP TABLE IF EXISTS study_membership_intervals")
    op.execute("DROP TABLE IF EXISTS study_sessions")
    op.execute("DROP TABLE IF EXISTS observation_studies")

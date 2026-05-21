"""processing_stages

Replace extraction_results + ai_usage_logs with a single processing_stages
table. One row per pipeline stage — status (SUCCESS/FAILED), result JSONB,
error text, conversation trail, and usage metrics all in one place.

Also removes error_message from processing_jobs (it belongs on the stage row).

Revision ID: 004
Revises: 003
Create Date: 2025-01-01 00:00:00.000000
"""

from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP TABLE IF EXISTS extraction_results")
    op.execute("DROP TABLE IF EXISTS ai_usage_logs")

    op.execute(
        "ALTER TABLE processing_jobs DROP COLUMN IF EXISTS error_message"
    )

    op.execute("""
        CREATE TABLE processing_stages (
            id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id       UUID NOT NULL REFERENCES processing_jobs(id) ON DELETE CASCADE,
            stage        TEXT NOT NULL,
            status       TEXT NOT NULL,
            result       JSONB,
            error        TEXT,
            conversation JSONB,
            usage        JSONB,
            started_at   TIMESTAMPTZ,
            completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE INDEX idx_processing_stages_job_id
            ON processing_stages (job_id)
    """)

    op.execute("""
        CREATE INDEX idx_processing_stages_job_stage
            ON processing_stages (job_id, stage)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS processing_stages")
    op.execute(
        "ALTER TABLE processing_jobs ADD COLUMN IF NOT EXISTS error_message TEXT"
    )
    # ai_usage_logs and extraction_results are not restored — data is gone

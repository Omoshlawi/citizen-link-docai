"""stage_conversations_table

Replace the conversation JSONB column on processing_stages with a proper
stage_conversations table. Each correction round gets its own row with
stable queryable columns (round, page, success) and a metadata JSONB for
variable fields (prompt, raw_response, errors).

Revision ID: 005
Revises: 004
Create Date: 2025-01-01 00:00:00.000000
"""

from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE processing_stages DROP COLUMN IF EXISTS conversation"
    )

    op.execute("""
        CREATE TABLE stage_conversations (
            id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            stage_id   UUID NOT NULL REFERENCES processing_stages(id) ON DELETE CASCADE,
            job_id     UUID NOT NULL REFERENCES processing_jobs(id) ON DELETE CASCADE,
            round      INT NOT NULL,
            page       INT,
            success    BOOLEAN NOT NULL,
            metadata   JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE INDEX idx_stage_conversations_stage_id
            ON stage_conversations (stage_id)
    """)

    op.execute("""
        CREATE INDEX idx_stage_conversations_job_id
            ON stage_conversations (job_id)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS stage_conversations")
    op.execute(
        "ALTER TABLE processing_stages ADD COLUMN IF NOT EXISTS conversation JSONB"
    )

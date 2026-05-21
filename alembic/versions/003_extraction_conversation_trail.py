"""extraction_conversation_trail

Add a conversation JSONB column to extraction_results to store each
correction round from the agentic loop — prompt sent, raw LLM response,
validation errors, and whether that round succeeded.

Revision ID: 003
Revises: 002
Create Date: 2025-01-01 00:00:00.000000
"""

from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Array of round objects: [{round, prompt, raw_response, errors, success}]
    op.execute(
        "ALTER TABLE extraction_results ADD COLUMN IF NOT EXISTS conversation JSONB"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE extraction_results DROP COLUMN IF EXISTS conversation"
    )

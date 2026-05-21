"""slim_processing_jobs

Drop external_case_id, external_document_id, external_extraction_id,
external_user_id, and case_type from processing_jobs — the caller no
longer sends these. The job is identified by its own UUID; NestJS stores
the returned job_id on AIExtraction.docaiJobId.

Revision ID: 002
Revises: 001
Create Date: 2025-01-01 00:00:00.000000
"""

from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE processing_jobs DROP COLUMN IF EXISTS external_case_id")
    op.execute("ALTER TABLE processing_jobs DROP COLUMN IF EXISTS external_document_id")
    op.execute("ALTER TABLE processing_jobs DROP COLUMN IF EXISTS external_extraction_id")
    op.execute("ALTER TABLE processing_jobs DROP COLUMN IF EXISTS external_user_id")
    op.execute("ALTER TABLE processing_jobs DROP COLUMN IF EXISTS case_type")


def downgrade() -> None:
    op.execute("ALTER TABLE processing_jobs ADD COLUMN IF NOT EXISTS case_type TEXT")
    op.execute("ALTER TABLE processing_jobs ADD COLUMN IF NOT EXISTS external_user_id TEXT")
    op.execute("ALTER TABLE processing_jobs ADD COLUMN IF NOT EXISTS external_extraction_id TEXT")
    op.execute("ALTER TABLE processing_jobs ADD COLUMN IF NOT EXISTS external_document_id TEXT")
    op.execute("ALTER TABLE processing_jobs ADD COLUMN IF NOT EXISTS external_case_id TEXT")

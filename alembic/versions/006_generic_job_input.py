"""generic_job_input

Replace extraction-specific columns on processing_jobs with a generic input model:
  - ADD job_type TEXT NOT NULL DEFAULT 'EXTRACTION'
  - ADD input JSONB NOT NULL DEFAULT '{}'
  - ADD priority INT NOT NULL DEFAULT 5
  - Migrate existing rows: pack case_number + image_urls into input JSONB
  - DROP COLUMN case_number
  - DROP COLUMN image_urls

This makes processing_jobs type-agnostic — fraud detection, match verification,
conflict resolution, etc. all slot in without further schema changes.

Revision ID: 006
Revises: 005
Create Date: 2025-01-01 00:00:00.000000
"""

from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Add new generic columns (nullable first so we can backfill)
    op.execute("""
        ALTER TABLE processing_jobs
            ADD COLUMN IF NOT EXISTS job_type  TEXT,
            ADD COLUMN IF NOT EXISTS input     JSONB,
            ADD COLUMN IF NOT EXISTS priority  INT NOT NULL DEFAULT 5
    """)

    # 2. Backfill existing rows — pack case_number + image_urls into input
    op.execute("""
        UPDATE processing_jobs
        SET
            job_type = 'EXTRACTION',
            input    = jsonb_build_object(
                           'case_number', case_number,
                           'image_urls',  to_jsonb(image_urls)
                       )
        WHERE job_type IS NULL
    """)

    # 3. Now enforce NOT NULL + DEFAULT on job_type and input
    op.execute("""
        ALTER TABLE processing_jobs
            ALTER COLUMN job_type SET NOT NULL,
            ALTER COLUMN job_type SET DEFAULT 'EXTRACTION',
            ALTER COLUMN input    SET NOT NULL,
            ALTER COLUMN input    SET DEFAULT '{}'
    """)

    # 4. Drop the extraction-specific columns
    op.execute("""
        ALTER TABLE processing_jobs
            DROP COLUMN IF EXISTS case_number,
            DROP COLUMN IF EXISTS image_urls
    """)

    # 5. Index on job_type for future pipeline-type queries
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_processing_jobs_job_type
            ON processing_jobs (job_type)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_processing_jobs_job_type")

    op.execute("""
        ALTER TABLE processing_jobs
            ADD COLUMN IF NOT EXISTS case_number TEXT,
            ADD COLUMN IF NOT EXISTS image_urls  TEXT[]
    """)

    # Restore extraction rows from input JSONB
    op.execute("""
        UPDATE processing_jobs
        SET
            case_number = input->>'case_number',
            image_urls  = ARRAY(SELECT jsonb_array_elements_text(input->'image_urls'))
        WHERE job_type = 'EXTRACTION'
    """)

    op.execute("""
        ALTER TABLE processing_jobs
            DROP COLUMN IF EXISTS job_type,
            DROP COLUMN IF EXISTS input,
            DROP COLUMN IF EXISTS priority
    """)

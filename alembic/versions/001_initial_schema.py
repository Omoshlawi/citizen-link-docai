"""Initial schema — 4 tables for citizen-link-docai.

Revision ID: 001
Revises: —
Create Date: 2025-01-01 00:00:00.000000

Tables:
  processing_jobs      — job tracking (opaque references to NestJS entities)
  extraction_results   — per-stage AI output (VISION / TEXT / EMBEDDING)
  ai_usage_logs        — every model call logged with tokens, cost, latency
  webhook_deliveries   — audit trail for every callback sent to NestJS
"""

from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS processing_jobs (
            id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            external_case_id      TEXT NOT NULL,
            external_document_id  TEXT NOT NULL,
            external_extraction_id TEXT NOT NULL,
            external_user_id      TEXT NOT NULL,
            case_type             TEXT NOT NULL,
            case_number           TEXT NOT NULL,
            image_urls            TEXT[] NOT NULL DEFAULT '{}',
            webhook_url           TEXT NOT NULL,
            status                TEXT NOT NULL DEFAULT 'PENDING',
            current_stage         TEXT,
            error_message         TEXT,
            created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_processing_jobs_status
            ON processing_jobs (status)
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_processing_jobs_external_case_id
            ON processing_jobs (external_case_id)
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS extraction_results (
            id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id     UUID NOT NULL REFERENCES processing_jobs(id) ON DELETE CASCADE,
            stage      TEXT NOT NULL,
            result     JSONB NOT NULL,
            confidence FLOAT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_extraction_results_job_id
            ON extraction_results (job_id)
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS ai_usage_logs (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id              UUID REFERENCES processing_jobs(id) ON DELETE SET NULL,
            stage               TEXT,
            model               TEXT NOT NULL,
            provider            TEXT NOT NULL,
            input_tokens        INT,
            output_tokens       INT,
            estimated_cost_usd  FLOAT,
            latency_ms          INT NOT NULL,
            success             BOOLEAN NOT NULL DEFAULT TRUE,
            error_message       TEXT,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_ai_usage_logs_job_id
            ON ai_usage_logs (job_id)
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS webhook_deliveries (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id          UUID NOT NULL REFERENCES processing_jobs(id) ON DELETE CASCADE,
            stage           TEXT NOT NULL,
            payload         JSONB NOT NULL,
            nestjs_url      TEXT NOT NULL,
            response_status INT,
            response_body   TEXT,
            attempt_count   INT NOT NULL DEFAULT 1,
            delivered       BOOLEAN NOT NULL DEFAULT FALSE,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_job_id
            ON webhook_deliveries (job_id)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS webhook_deliveries")
    op.execute("DROP TABLE IF EXISTS ai_usage_logs")
    op.execute("DROP TABLE IF EXISTS extraction_results")
    op.execute("DROP TABLE IF EXISTS processing_jobs")

"""Initial schema — citizen-link-docai.

Four tables:
  processing_jobs        — one row per pipeline job submitted by NestJS
  processing_stages      — one row per pipeline stage (VISION, STRUCTURE, …)
  stage_conversations    — one row per LLM message turn within a stage
  webhook_deliveries     — audit trail for every callback sent to NestJS

Revision ID: 001
Revises: —
Create Date: 2025-01-01 00:00:00.000000
"""

from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── processing_jobs ────────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE processing_jobs (
            id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            job_type      TEXT        NOT NULL DEFAULT 'EXTRACTION',
            input         JSONB       NOT NULL DEFAULT '{}',
            webhook_url   TEXT        NOT NULL,
            priority      INT         NOT NULL DEFAULT 5,
            status        TEXT        NOT NULL DEFAULT 'PENDING',
            current_stage TEXT,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE INDEX idx_processing_jobs_status
            ON processing_jobs (status)
    """)

    op.execute("""
        CREATE INDEX idx_processing_jobs_job_type
            ON processing_jobs (job_type)
    """)

    # ── processing_stages ──────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE processing_stages (
            id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id       UUID        NOT NULL REFERENCES processing_jobs(id) ON DELETE CASCADE,
            stage        TEXT        NOT NULL,
            status       TEXT        NOT NULL,
            result       JSONB,
            error        TEXT,
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

    # ── stage_conversations ────────────────────────────────────────────────────
    # One row per LLM message turn.
    # Round 1: system + user + assistant turns.
    # Rounds 2+: user(correction) + assistant turns only.
    # Concatenating all turns in round/created_at order reconstructs the full
    # conversation thread with zero duplication.
    #
    # role     : system | user | assistant
    # content  : prompt text or LLM response
    # success  : NULL for system/user rows; TRUE/FALSE on assistant rows
    # metadata :
    #   Vision user rows      — { url, mime_type }  (signed URL, no base64)
    #   Failed assistant rows — { errors: [...] }
    #   All other rows        — NULL
    op.execute("""
        CREATE TABLE stage_conversations (
            id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            stage_id   UUID        NOT NULL REFERENCES processing_stages(id) ON DELETE CASCADE,
            job_id     UUID        NOT NULL REFERENCES processing_jobs(id)   ON DELETE CASCADE,
            round      INT         NOT NULL,
            page       INT,
            role       TEXT        NOT NULL,
            content    TEXT        NOT NULL,
            success    BOOLEAN,
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

    op.execute("""
        CREATE INDEX idx_stage_conversations_role
            ON stage_conversations (role)
    """)

    # ── webhook_deliveries ─────────────────────────────────────────────────────
    # stage column stores the dot-notation event string
    # (e.g. extraction.vision.success, extraction.failed).
    op.execute("""
        CREATE TABLE webhook_deliveries (
            id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id          UUID        NOT NULL REFERENCES processing_jobs(id) ON DELETE CASCADE,
            stage           TEXT        NOT NULL,
            payload         JSONB       NOT NULL,
            callback_url    TEXT        NOT NULL,
            response_status INT,
            response_body   TEXT,
            attempt_count   INT         NOT NULL DEFAULT 1,
            delivered       BOOLEAN     NOT NULL DEFAULT FALSE,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE INDEX idx_webhook_deliveries_job_id
            ON webhook_deliveries (job_id)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS stage_conversations")
    op.execute("DROP TABLE IF EXISTS webhook_deliveries")
    op.execute("DROP TABLE IF EXISTS processing_stages")
    op.execute("DROP TABLE IF EXISTS processing_jobs")

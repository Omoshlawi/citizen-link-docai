"""stage_conversations_turns

Normalise stage_conversations so each row represents one message turn
in the LLM conversation rather than one correction round.

New columns:
  role    TEXT NOT NULL  — system | user | assistant
  content TEXT NOT NULL  — the message text (prompt or response)

Changed columns:
  success  BOOLEAN       — was NOT NULL; now nullable (NULL for system/user rows,
                           TRUE/FALSE on assistant rows only)

metadata shape changes (no migration needed — existing rows have old shape):
  user rows  (vision): { url, mime_type }
  assistant rows (failed): { errors: [...] }
  all others: NULL

Revision ID: 007
Revises: 006
Create Date: 2025-01-01 00:00:00.000000
"""

from alembic import op

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add role + content with temporary defaults so NOT NULL is satisfiable on existing rows
    op.execute("""
        ALTER TABLE stage_conversations
            ADD COLUMN IF NOT EXISTS role    TEXT NOT NULL DEFAULT '',
            ADD COLUMN IF NOT EXISTS content TEXT NOT NULL DEFAULT ''
    """)

    # Drop defaults — new rows must supply values explicitly
    op.execute("""
        ALTER TABLE stage_conversations
            ALTER COLUMN role    DROP DEFAULT,
            ALTER COLUMN content DROP DEFAULT
    """)

    # success is now nullable — only assistant rows carry a meaningful value
    op.execute("""
        ALTER TABLE stage_conversations
            ALTER COLUMN success DROP NOT NULL
    """)

    # Index so callers can efficiently filter by role (e.g. all failed assistant rows)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_stage_conversations_role
            ON stage_conversations (role)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_stage_conversations_role")

    op.execute("""
        ALTER TABLE stage_conversations
            DROP COLUMN IF EXISTS role,
            DROP COLUMN IF EXISTS content
    """)

    # Restore NOT NULL on success (backfill NULLs with false before enforcing)
    op.execute("UPDATE stage_conversations SET success = false WHERE success IS NULL")
    op.execute("ALTER TABLE stage_conversations ALTER COLUMN success SET NOT NULL")

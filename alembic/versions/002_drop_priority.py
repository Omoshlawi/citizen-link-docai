"""Drop priority column from processing_jobs.

ARQ does not support native priority queuing, so the field served no purpose
beyond storing metadata that was never acted upon.

Revision ID: 002
Revises: 001
Create Date: 2025-05-23 00:00:00.000000
"""

from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("processing_jobs", "priority")


def downgrade() -> None:
    import sqlalchemy as sa
    op.add_column(
        "processing_jobs",
        sa.Column("priority", sa.Integer(), nullable=False, server_default="5"),
    )

"""
ProcessingRepository — raw SQL operations on the processing_jobs table.

All queries use asyncpg directly — no ORM.
"""

from typing import Optional
from uuid import UUID

import asyncpg
import structlog

log = structlog.get_logger(__name__)


class ProcessingRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create_job(
        self,
        external_case_id: str,
        external_document_id: str,
        external_extraction_id: str,
        external_user_id: str,
        case_type: str,
        case_number: str,
        image_urls: list[str],
        webhook_url: str,
    ) -> str:
        """Insert a new PENDING job and return its UUID."""
        row = await self._pool.fetchrow(
            """
            INSERT INTO processing_jobs (
                external_case_id, external_document_id, external_extraction_id,
                external_user_id, case_type, case_number, image_urls, webhook_url,
                status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'PENDING')
            RETURNING id::text
            """,
            external_case_id,
            external_document_id,
            external_extraction_id,
            external_user_id,
            case_type,
            case_number,
            image_urls,
            webhook_url,
        )
        return row["id"]

    async def get_job(self, job_id: str) -> Optional[asyncpg.Record]:
        """Fetch a job by UUID. Returns None if not found."""
        return await self._pool.fetchrow(
            "SELECT * FROM processing_jobs WHERE id = $1::uuid",
            job_id,
        )

    async def update_status(
        self,
        job_id: str,
        status: str,
        current_stage: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Update a job's status, optionally setting current_stage and error_message."""
        await self._pool.execute(
            """
            UPDATE processing_jobs
            SET status        = $2,
                current_stage = COALESCE($3, current_stage),
                error_message = $4,
                updated_at    = NOW()
            WHERE id = $1::uuid
            """,
            job_id,
            status,
            current_stage,
            error_message,
        )

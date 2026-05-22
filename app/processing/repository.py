"""
ProcessingRepository — raw SQL operations on the processing_jobs table.

All queries use asyncpg directly — no ORM.
"""

import json
from typing import List, Optional, Tuple

import asyncpg
import structlog

log = structlog.get_logger(__name__)


class ProcessingRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create_job(
        self,
        job_type: str,
        input: dict,
        webhook_url: str,
        priority: int = 5,
    ) -> str:
        """Insert a new PENDING job and return its UUID."""
        row = await self._pool.fetchrow(
            """
            INSERT INTO processing_jobs (job_type, input, webhook_url, priority, status)
            VALUES ($1, $2::jsonb, $3, $4, 'PENDING')
            RETURNING id::text
            """,
            job_type,
            json.dumps(input),
            webhook_url,
            priority,
        )
        return row["id"]

    async def get_job(self, job_id: str) -> Optional[asyncpg.Record]:
        """Fetch a job by UUID. Returns None if not found."""
        return await self._pool.fetchrow(
            "SELECT * FROM processing_jobs WHERE id = $1::uuid",
            job_id,
        )

    async def list_jobs(
        self,
        job_type: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Tuple[List[asyncpg.Record], int]:
        """
        Return a paginated list of jobs and the total count, newest first.

        Both job_type and status filters are optional and combinable.
        """
        offset = (page - 1) * page_size
        conditions: list[str] = []
        params: list = []

        if job_type:
            params.append(job_type)
            conditions.append(f"job_type = ${len(params)}")

        if status:
            params.append(status)
            conditions.append(f"status = ${len(params)}")

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        rows = await self._pool.fetch(
            f"""
            SELECT * FROM processing_jobs
            {where}
            ORDER BY created_at DESC
            LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
            """,
            *params,
            page_size,
            offset,
        )

        total: int = await self._pool.fetchval(
            f"SELECT COUNT(*) FROM processing_jobs {where}",
            *params,
        )

        return rows, total

    async def update_status(
        self,
        job_id: str,
        status: str,
        current_stage: Optional[str] = None,
    ) -> None:
        """Update a job's status, optionally setting current_stage.

        Accepts plain strings or JobStatus enum values (which are str subclasses).
        """
        await self._pool.execute(
            """
            UPDATE processing_jobs
            SET status        = $2,
                current_stage = COALESCE($3, current_stage),
                updated_at    = NOW()
            WHERE id = $1::uuid
            """,
            job_id,
            status,
            current_stage,
        )

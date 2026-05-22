"""
InspectionRepository — read-only queries across processing_stages,
stage_conversations, and webhook_deliveries, with joins to processing_jobs.

All queries use asyncpg directly.  No ORM, no magic.

Condition builder pattern
--------------------------
Each query builds a WHERE clause dynamically from the filters provided.
Params are positional ($1, $2, …) and appended to a `params` list in the
same order the condition references them — so the $N numbers always match.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

import asyncpg


def _loads(v: Any) -> Optional[Dict]:
    """Safely decode a JSONB value (asyncpg returns it as a string)."""
    if v is None:
        return None
    if isinstance(v, str):
        return json.loads(v)
    return v  # already a dict in some asyncpg versions


class InspectionRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ── Processing stages ──────────────────────────────────────────────────────

    async def list_stages(
        self,
        *,
        job_id: Optional[str] = None,
        job_type: Optional[str] = None,
        stage: Optional[str] = None,
        status: Optional[str] = None,
        include_result: bool = False,
        page: int = 1,
        page_size: int = 20,
    ) -> Tuple[List[asyncpg.Record], int]:
        """
        Paginated stage list joined with processing_jobs.

        Filters are AND-combined; any combination is valid.
        result JSONB is excluded by default — opt in via include_result=True.
        """
        result_col = "ps.result" if include_result else "NULL::jsonb AS result"
        conditions: list[str] = []
        params: list = []

        if job_id:
            params.append(job_id)
            conditions.append(f"ps.job_id = ${len(params)}::uuid")

        if job_type:
            params.append(job_type)
            conditions.append(f"pj.job_type = ${len(params)}")

        if stage:
            params.append(stage)
            conditions.append(f"ps.stage = ${len(params)}")

        if status:
            params.append(status)
            conditions.append(f"ps.status = ${len(params)}")

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        offset = (page - 1) * page_size

        rows = await self._pool.fetch(
            f"""
            SELECT
                ps.id::text          AS stage_id,
                ps.job_id::text,
                ps.stage,
                ps.status,
                ps.error,
                ps.usage,
                ps.started_at,
                ps.completed_at,
                ps.created_at,
                {result_col},
                pj.job_type,
                pj.status            AS job_status,
                pj.priority          AS job_priority
            FROM processing_stages ps
            JOIN processing_jobs pj ON pj.id = ps.job_id
            {where}
            ORDER BY ps.created_at DESC
            LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
            """,
            *params, page_size, offset,
        )

        total: int = await self._pool.fetchval(
            f"""
            SELECT COUNT(*)
            FROM processing_stages ps
            JOIN processing_jobs pj ON pj.id = ps.job_id
            {where}
            """,
            *params,
        )

        return rows, total

    async def get_stage(
        self,
        stage_id: str,
        *,
        include_result: bool = False,
    ) -> Optional[asyncpg.Record]:
        """Single stage row joined with its parent job."""
        result_col = "ps.result" if include_result else "NULL::jsonb AS result"
        return await self._pool.fetchrow(
            f"""
            SELECT
                ps.id::text          AS stage_id,
                ps.job_id::text,
                ps.stage,
                ps.status,
                ps.error,
                ps.usage,
                ps.started_at,
                ps.completed_at,
                ps.created_at,
                {result_col},
                pj.job_type,
                pj.status            AS job_status,
                pj.priority          AS job_priority
            FROM processing_stages ps
            JOIN processing_jobs pj ON pj.id = ps.job_id
            WHERE ps.id = $1::uuid
            """,
            stage_id,
        )

    async def list_stages_for_job(
        self,
        job_id: str,
        *,
        include_result: bool = False,
    ) -> List[asyncpg.Record]:
        """All stages for one job, oldest first (pipeline order)."""
        result_col = "ps.result" if include_result else "NULL::jsonb AS result"
        return await self._pool.fetch(
            f"""
            SELECT
                ps.id::text          AS stage_id,
                ps.job_id::text,
                ps.stage,
                ps.status,
                ps.error,
                ps.usage,
                ps.started_at,
                ps.completed_at,
                ps.created_at,
                {result_col},
                pj.job_type,
                pj.status            AS job_status,
                pj.priority          AS job_priority
            FROM processing_stages ps
            JOIN processing_jobs pj ON pj.id = ps.job_id
            WHERE ps.job_id = $1::uuid
            ORDER BY ps.created_at ASC
            """,
            job_id,
        )

    # ── Stage conversations ────────────────────────────────────────────────────

    async def list_conversations(
        self,
        *,
        job_id: Optional[str] = None,
        stage_id: Optional[str] = None,
        stage_name: Optional[str] = None,
        success: Optional[bool] = None,
        page_num: Optional[int] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Tuple[List[asyncpg.Record], int]:
        """
        Paginated conversation list joined with processing_stages and processing_jobs.

        page_num filters by the image page number (VisionAgent only; null for structure).
        """
        conditions: list[str] = []
        params: list = []

        if job_id:
            params.append(job_id)
            conditions.append(f"sc.job_id = ${len(params)}::uuid")

        if stage_id:
            params.append(stage_id)
            conditions.append(f"sc.stage_id = ${len(params)}::uuid")

        if stage_name:
            params.append(stage_name)
            conditions.append(f"ps.stage = ${len(params)}")

        if success is not None:
            params.append(success)
            conditions.append(f"sc.success = ${len(params)}")

        if page_num is not None:
            params.append(page_num)
            conditions.append(f"sc.page = ${len(params)}")

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        offset = (page - 1) * page_size

        rows = await self._pool.fetch(
            f"""
            SELECT
                sc.id::text          AS conversation_id,
                sc.stage_id::text,
                sc.job_id::text,
                sc.round,
                sc.page,
                sc.role,
                sc.content,
                sc.success,
                sc.metadata,
                sc.created_at,
                ps.stage             AS stage_name,
                ps.status            AS stage_status,
                pj.job_type
            FROM stage_conversations sc
            JOIN processing_stages ps ON ps.id = sc.stage_id
            JOIN processing_jobs   pj ON pj.id = sc.job_id
            {where}
            ORDER BY sc.job_id, sc.stage_id, sc.page NULLS LAST, sc.round, sc.created_at
            LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
            """,
            *params, page_size, offset,
        )

        total: int = await self._pool.fetchval(
            f"""
            SELECT COUNT(*)
            FROM stage_conversations sc
            JOIN processing_stages ps ON ps.id = sc.stage_id
            JOIN processing_jobs   pj ON pj.id = sc.job_id
            {where}
            """,
            *params,
        )

        return rows, total

    async def list_conversations_for_stage(
        self,
        stage_id: str,
    ) -> List[asyncpg.Record]:
        """All message turns for one stage, ordered by round then insertion order."""
        return await self._pool.fetch(
            """
            SELECT
                sc.id::text          AS conversation_id,
                sc.stage_id::text,
                sc.job_id::text,
                sc.round,
                sc.page,
                sc.role,
                sc.content,
                sc.success,
                sc.metadata,
                sc.created_at,
                ps.stage             AS stage_name,
                ps.status            AS stage_status,
                pj.job_type
            FROM stage_conversations sc
            JOIN processing_stages ps ON ps.id = sc.stage_id
            JOIN processing_jobs   pj ON pj.id = sc.job_id
            WHERE sc.stage_id = $1::uuid
            ORDER BY sc.page NULLS LAST, sc.round, sc.created_at
            """,
            stage_id,
        )

    async def list_conversations_for_job(
        self,
        job_id: str,
    ) -> List[asyncpg.Record]:
        """All message turns across all stages for one job."""
        return await self._pool.fetch(
            """
            SELECT
                sc.id::text          AS conversation_id,
                sc.stage_id::text,
                sc.job_id::text,
                sc.round,
                sc.page,
                sc.role,
                sc.content,
                sc.success,
                sc.metadata,
                sc.created_at,
                ps.stage             AS stage_name,
                ps.status            AS stage_status,
                pj.job_type
            FROM stage_conversations sc
            JOIN processing_stages ps ON ps.id = sc.stage_id
            JOIN processing_jobs   pj ON pj.id = sc.job_id
            WHERE sc.job_id = $1::uuid
            ORDER BY ps.created_at ASC, sc.page NULLS LAST, sc.round, sc.created_at
            """,
            job_id,
        )

    # ── Webhook deliveries ─────────────────────────────────────────────────────

    async def list_webhooks(
        self,
        *,
        job_id: Optional[str] = None,
        event: Optional[str] = None,
        delivered: Optional[bool] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Tuple[List[asyncpg.Record], int]:
        """
        Paginated webhook delivery list joined with processing_jobs.

        event supports prefix matching (e.g. "extraction." matches all extraction events).
        payload JSONB is excluded from list results — use GET /v1/webhooks/{id} for the full payload.
        """
        conditions: list[str] = []
        params: list = []

        if job_id:
            params.append(job_id)
            conditions.append(f"wd.job_id = ${len(params)}::uuid")

        if event:
            params.append(event if "%" in event else f"{event}%")
            conditions.append(f"wd.stage ILIKE ${len(params)}")

        if delivered is not None:
            params.append(delivered)
            conditions.append(f"wd.delivered = ${len(params)}")

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        offset = (page - 1) * page_size

        rows = await self._pool.fetch(
            f"""
            SELECT
                wd.id::text          AS delivery_id,
                wd.job_id::text,
                wd.stage             AS event,
                wd.callback_url,
                wd.response_status,
                wd.response_body,
                wd.attempt_count,
                wd.delivered,
                wd.created_at,
                NULL::jsonb          AS payload,
                pj.job_type,
                pj.status            AS job_status
            FROM webhook_deliveries wd
            JOIN processing_jobs pj ON pj.id = wd.job_id
            {where}
            ORDER BY wd.created_at DESC
            LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
            """,
            *params, page_size, offset,
        )

        total: int = await self._pool.fetchval(
            f"""
            SELECT COUNT(*)
            FROM webhook_deliveries wd
            JOIN processing_jobs pj ON pj.id = wd.job_id
            {where}
            """,
            *params,
        )

        return rows, total

    async def get_webhook(self, delivery_id: str) -> Optional[asyncpg.Record]:
        """Single delivery record with full payload, joined with its parent job."""
        return await self._pool.fetchrow(
            """
            SELECT
                wd.id::text          AS delivery_id,
                wd.job_id::text,
                wd.stage             AS event,
                wd.callback_url,
                wd.response_status,
                wd.response_body,
                wd.attempt_count,
                wd.delivered,
                wd.created_at,
                wd.payload,
                pj.job_type,
                pj.status            AS job_status
            FROM webhook_deliveries wd
            JOIN processing_jobs pj ON pj.id = wd.job_id
            WHERE wd.id = $1::uuid
            """,
            delivery_id,
        )

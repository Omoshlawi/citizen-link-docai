"""
UsageRepository — writes ai_usage_logs and reads them for auditing.

Every model call (vision, structure, embedding) logs a row here.
This gives a full audit trail of cost, latency, and success per job.
"""

from typing import Optional

import asyncpg
import structlog

log = structlog.get_logger(__name__)


class UsageRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def log_usage(
        self,
        job_id: Optional[str],
        stage: Optional[str],
        model: str,
        provider: str,
        input_tokens: Optional[int],
        output_tokens: Optional[int],
        estimated_cost_usd: Optional[float],
        latency_ms: int,
        success: bool = True,
        error_message: Optional[str] = None,
    ) -> None:
        """Insert one ai_usage_logs row."""
        await self._pool.execute(
            """
            INSERT INTO ai_usage_logs (
                job_id, stage, model, provider,
                input_tokens, output_tokens, estimated_cost_usd,
                latency_ms, success, error_message
            )
            VALUES (
                $1::uuid, $2, $3, $4,
                $5, $6, $7,
                $8, $9, $10
            )
            """,
            job_id,
            stage,
            model,
            provider,
            input_tokens,
            output_tokens,
            estimated_cost_usd,
            latency_ms,
            success,
            error_message,
        )

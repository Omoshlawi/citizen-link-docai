"""
ARQ pipeline tasks — generic stage runner + webhook delivery.

Pipeline flow (EXTRACTION):
  run_stage("VISION") → run_stage("STRUCTURE")

Caller webhook contract:
  VISION    — OCR complete, extraction in progress (progress signal, no result data)
  COMPLETED — all extraction done, full fields delivered (terminal signal)
  FAILED    — a stage failed, includes which stage and the reason

New pipelines (fraud detection, match verification, etc.) register in
app/pipeline/registry.py and slot in automatically — no changes needed here.

Embedding is NOT part of this pipeline. The caller calls POST /v1/embed
with confirmed, human-verified data after the user reviews extracted fields.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

import asyncpg
import structlog

from app.agents.exceptions import AgentExhaustedError
from app.config import Settings
from app.pipeline.enums import WebhookStatus
from app.pipeline.webhook import deliver_webhook
from app.processing.repository import ProcessingRepository

log = structlog.get_logger(__name__)

# Approximate cost per 1M tokens (USD) — update as pricing changes
_COST_PER_1M: dict[str, dict[str, float]] = {
    "gpt-4o":                   {"input": 2.50,  "output": 10.00},
    "gpt-4o-mini":              {"input": 0.15,  "output": 0.60},
    "text-embedding-3-small":   {"input": 0.02,  "output": 0.00},
    "text-embedding-3-large":   {"input": 0.13,  "output": 0.00},
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _build_usage(usage_logs: list[dict]) -> dict:
    """
    Aggregate per-call usage entries into a single JSONB-ready dict.

    usage_logs is the list returned by agents — one entry per LLM call
    (including each correction round). New metrics (e.g. cached tokens)
    are added as new keys inside each call dict without needing a migration.
    """
    if not usage_logs:
        return {}

    model = usage_logs[0].get("model", "unknown")
    provider = usage_logs[0].get("provider", "unknown")

    calls = []
    total_input = 0
    total_output = 0
    total_latency = 0.0

    for i, entry in enumerate(usage_logs, start=1):
        input_t = entry.get("input_tokens") or 0
        output_t = entry.get("output_tokens") or 0
        latency = entry.get("latency_ms") or 0.0
        total_input += input_t
        total_output += output_t
        total_latency += latency
        calls.append({
            "call": i,
            "input_tokens": input_t,
            "output_tokens": output_t,
            "latency_ms": latency,
        })

    pricing = _COST_PER_1M.get(model)
    if pricing:
        cost: Optional[float] = round(
            total_input / 1_000_000 * pricing["input"]
            + total_output / 1_000_000 * pricing["output"],
            8,
        )
    else:
        cost = None  # local/unknown model — no cost

    return {
        "model": model,
        "provider": provider,
        "calls": calls,
        "total_input_tokens": total_input,
        "total_output_tokens": total_output,
        "total_latency_ms": round(total_latency, 2),
        "estimated_cost_usd": cost,
    }


async def _store_stage(
    pool: asyncpg.Pool,
    job_id: str,
    stage: str,
    *,
    status: str,
    result: Optional[dict] = None,
    error: Optional[str] = None,
    usage: Optional[dict] = None,
    started_at: Optional[datetime] = None,
) -> str:
    """Insert one processing_stages row and return its UUID."""
    row = await pool.fetchrow(
        """
        INSERT INTO processing_stages
            (job_id, stage, status, result, error, usage, started_at)
        VALUES
            ($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb, $7)
        RETURNING id::text
        """,
        job_id,
        stage,
        status,
        json.dumps(result) if result is not None else None,
        error,
        json.dumps(usage) if usage is not None else None,
        started_at,
    )
    return row["id"]


async def _store_conversation(
    pool: asyncpg.Pool,
    stage_id: str,
    job_id: str,
    conversation: list[dict],
) -> None:
    """Bulk-insert one stage_conversations row per correction round."""
    if not conversation:
        return
    rows = [
        (
            stage_id,
            job_id,
            entry.get("round"),
            entry.get("page"),
            entry.get("success"),
            json.dumps({
                "correction_sent": entry.get("correction_sent"),
                "raw_response": entry.get("raw_response"),
                "errors": entry.get("errors", []),
            }),
        )
        for entry in conversation
    ]
    await pool.executemany(
        """
        INSERT INTO stage_conversations
            (stage_id, job_id, round, page, success, metadata)
        VALUES
            ($1::uuid, $2::uuid, $3, $4, $5, $6::jsonb)
        """,
        rows,
    )


async def _get_stage_result(
    pool: asyncpg.Pool, job_id: str, stage: str
) -> Optional[dict]:
    """Return the result JSONB from the most recent SUCCESS row for a stage."""
    row = await pool.fetchrow(
        """
        SELECT result FROM processing_stages
        WHERE job_id = $1::uuid AND stage = $2 AND status = 'SUCCESS'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        job_id,
        stage,
    )
    return json.loads(row["result"]) if row else None


async def _notify_failure(
    pool: asyncpg.Pool,
    settings: Settings,
    job_id: str,
    failed_at: str,
    reason: str,
    job_row: asyncpg.Record,
) -> None:
    """Mark the job FAILED and fire the failure webhook. Stage row must already exist."""
    await ProcessingRepository(pool).update_status(job_id, "FAILED")
    await _enqueue_webhook(
        pool=pool,
        settings=settings,
        job_id=job_id,
        stage="FAILED",
        status=WebhookStatus.FAILED,
        callback_url=job_row["webhook_url"],
        result={"failedAt": failed_at, "reason": reason},
    )


async def _enqueue_webhook(
    pool: asyncpg.Pool,
    settings: Settings,
    job_id: str,
    stage: str,
    status: WebhookStatus,
    callback_url: str,
    result: Optional[dict] = None,
) -> None:
    from arq import create_pool as arq_create_pool
    from arq.connections import RedisSettings

    arq_pool = await arq_create_pool(RedisSettings.from_dsn(settings.redis_url))
    await arq_pool.enqueue_job(
        "task_deliver_webhook",
        job_id,
        stage,
        status,
        callback_url,
        settings.callback_secret,
        result,
    )
    await arq_pool.close()


async def _enqueue_next(settings: Settings, task_name: str, *args) -> None:
    """Enqueue an ARQ task with arbitrary positional args."""
    from arq import create_pool as arq_create_pool
    from arq.connections import RedisSettings

    arq_pool = await arq_create_pool(RedisSettings.from_dsn(settings.redis_url))
    await arq_pool.enqueue_job(task_name, *args)
    await arq_pool.close()


# ── Generic stage runner ───────────────────────────────────────────────────────

async def run_stage(ctx: dict, job_id: str, stage: str) -> None:
    """
    Generic ARQ task — runs one pipeline stage for any job type.

    Dispatches to the correct agent via the pipeline registry, stores the
    result, fires a progress webhook if this is a mid-pipeline stage, then
    either enqueues the next stage or fires the terminal COMPLETED webhook.

    Adding a new pipeline (fraud detection, match verification, etc.) requires
    only a registry entry — this task never needs to change.
    """
    from app.pipeline.registry import get_agent, get_pipeline

    pool: asyncpg.Pool = ctx["pool"]
    settings: Settings = ctx["settings"]

    structlog.contextvars.bind_contextvars(job_id=job_id, stage=stage)
    log.info("stage_started")

    repo = ProcessingRepository(pool)
    job = await repo.get_job(job_id)
    if not job:
        log.error("job_not_found")
        return

    await repo.update_status(job_id, "IN_PROGRESS", current_stage=stage)
    started_at = datetime.now(timezone.utc)

    job_type: str = job["job_type"]
    # asyncpg returns JSONB as a Python dict; guard for str fallback just in case
    raw_input = job["input"]
    job_input: dict = (
        json.loads(raw_input) if isinstance(raw_input, str) else dict(raw_input)
    )

    try:
        pipeline = get_pipeline(job_type)
        stage_index = pipeline.stages.index(stage)

        # Collect results from all stages that ran before this one
        previous_results: dict[str, dict] = {}
        for prev_stage in pipeline.stages[:stage_index]:
            prev_result = await _get_stage_result(pool, job_id, prev_stage)
            if prev_result is not None:
                previous_results[prev_stage] = prev_result

        # Run the agent
        agent = get_agent(job_type, stage, settings)
        result, usage_logs, conversation = await agent.run(job_input, previous_results)

        stage_id = await _store_stage(
            pool, job_id, stage,
            status="SUCCESS",
            result=result,
            usage=_build_usage(usage_logs),
            started_at=started_at,
        )
        await _store_conversation(pool, stage_id, job_id, conversation)
        log.info("stage_completed")

        next_index = stage_index + 1
        has_next = next_index < len(pipeline.stages)

        # Fire a progress webhook only for mid-pipeline stages — i.e. stages
        # that are marked as progress stages AND have a successor.  For a
        # single-stage pipeline (or the final stage of any pipeline) the
        # COMPLETED webhook below is sufficient; a redundant IN_PROGRESS
        # webhook immediately before it would be confusing to the caller.
        if stage in pipeline.progress_stages and has_next:
            await _enqueue_webhook(
                pool=pool, settings=settings, job_id=job_id,
                stage=stage, status=WebhookStatus.IN_PROGRESS,
                callback_url=job["webhook_url"],
            )

        if has_next:
            # More stages to run — enqueue the next one
            next_stage = pipeline.stages[next_index]
            log.info("enqueueing_next_stage", next_stage=next_stage)
            await _enqueue_next(settings, "run_stage", job_id, next_stage)
        else:
            # Final stage — build the terminal result and fire COMPLETED
            all_results: dict[str, dict] = {**previous_results, stage: result}
            final_result = pipeline.build_result(all_results)

            await repo.update_status(job_id, "COMPLETED", current_stage=None)
            await _enqueue_webhook(
                pool=pool, settings=settings, job_id=job_id,
                stage="COMPLETED", status=WebhookStatus.COMPLETED,
                callback_url=job["webhook_url"],
                result=final_result,
            )
            log.info("pipeline_completed", job_type=job_type)

    except AgentExhaustedError as exc:
        log.error("stage_failed", error=str(exc))
        stage_id = await _store_stage(
            pool, job_id, stage,
            status="FAILED", error=str(exc), started_at=started_at,
        )
        await _store_conversation(pool, stage_id, job_id, exc.conversation)
        await _notify_failure(pool, settings, job_id, stage, str(exc), job)
        raise

    except Exception as exc:
        log.error("stage_failed", error=str(exc))
        await _store_stage(
            pool, job_id, stage,
            status="FAILED", error=str(exc), started_at=started_at,
        )
        await _notify_failure(pool, settings, job_id, stage, str(exc), job)
        raise


# ── Webhook delivery task ──────────────────────────────────────────────────────

async def task_deliver_webhook(
    ctx: dict,
    job_id: str,
    stage: str,
    status: WebhookStatus,
    callback_url: str,
    callback_secret: str,
    result: Optional[dict] = None,
) -> None:
    """
    ARQ task — delivers one stage callback to the caller.

    ARQ retries automatically on failure (up to WEBHOOK_MAX_RETRIES).
    Every attempt is logged to webhook_deliveries regardless of outcome.
    """
    pool: asyncpg.Pool = ctx["pool"]
    attempt = ctx.get("job_try", 1)

    structlog.contextvars.bind_contextvars(job_id=job_id, stage=stage)
    log.info("webhook_task_started", attempt=attempt)

    await deliver_webhook(
        pool=pool,
        job_id=job_id,
        stage=stage,
        status=status,
        callback_url=callback_url,
        callback_secret=callback_secret,
        result=result,
        attempt_count=attempt,
    )

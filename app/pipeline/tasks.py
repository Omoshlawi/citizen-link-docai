"""
ARQ pipeline tasks — 2 extraction stages + post-processing + webhook delivery.

Pipeline flow:
  run_vision → run_structure → run_post_processing

Embedding is NOT part of this pipeline. The caller requests embeddings
separately via POST /v1/embed after receiving the COMPLETED webhook.
This keeps extraction and indexing as independent concerns.

Error handling:
  Any unhandled exception in a stage marks the job FAILED and enqueues
  a task_deliver_webhook so the caller is notified immediately.
"""

from __future__ import annotations

import json
from typing import Optional

import asyncpg
import structlog

from app.agents.structure_agent import StructureAgent
from app.agents.vision_agent import VisionAgent
from app.config import Settings
from app.pipeline.webhook import deliver_webhook
from app.processing.repository import ProcessingRepository
from app.usage.service import UsageService

log = structlog.get_logger(__name__)


# ── Helpers ────────────────────────────────────────────────────────────────────

async def _fail_job(
    pool: asyncpg.Pool,
    settings: Settings,
    job_id: str,
    failed_at: str,
    reason: str,
    job_row: asyncpg.Record,
) -> None:
    """Mark a job FAILED and enqueue a failure callback to the caller."""
    await ProcessingRepository(pool).update_status(
        job_id, "FAILED", error_message=reason
    )
    await _enqueue_webhook(
        pool=pool,
        settings=settings,
        job_id=job_id,
        external_case_id=job_row["external_case_id"],
        external_extraction_id=job_row["external_extraction_id"],
        stage="FAILED",
        status="failed",
        callback_url=job_row["webhook_url"],
        result={"failedAt": failed_at, "reason": reason},
    )


async def _store_extraction_result(
    pool: asyncpg.Pool,
    job_id: str,
    stage: str,
    result: dict,
    confidence: Optional[float] = None,
) -> None:
    await pool.execute(
        """
        INSERT INTO extraction_results (job_id, stage, result, confidence)
        VALUES ($1::uuid, $2, $3::jsonb, $4)
        """,
        job_id,
        stage,
        json.dumps(result),
        confidence,
    )


async def _get_extraction_result(
    pool: asyncpg.Pool, job_id: str, stage: str
) -> Optional[dict]:
    row = await pool.fetchrow(
        "SELECT result FROM extraction_results WHERE job_id = $1::uuid AND stage = $2",
        job_id,
        stage,
    )
    return json.loads(row["result"]) if row else None


async def _enqueue_webhook(
    pool: asyncpg.Pool,
    settings: Settings,
    job_id: str,
    external_case_id: str,
    external_extraction_id: str,
    stage: str,
    status: str,
    callback_url: str,
    result: Optional[dict] = None,
) -> None:
    from arq import create_pool as arq_create_pool
    from arq.connections import RedisSettings

    arq_pool = await arq_create_pool(RedisSettings.from_dsn(settings.redis_url))
    await arq_pool.enqueue_job(
        "task_deliver_webhook",
        job_id,
        external_case_id,
        external_extraction_id,
        stage,
        status,
        callback_url,
        settings.callback_secret,
        result,
    )
    await arq_pool.close()


async def _enqueue_next(settings: Settings, task_name: str, job_id: str) -> None:
    from arq import create_pool as arq_create_pool
    from arq.connections import RedisSettings

    arq_pool = await arq_create_pool(RedisSettings.from_dsn(settings.redis_url))
    await arq_pool.enqueue_job(task_name, job_id)
    await arq_pool.close()


# ── Stage 1: Vision ────────────────────────────────────────────────────────────

async def run_vision(ctx: dict, job_id: str) -> None:
    """
    Stage 1 — OCR extraction from document images.

    Downloads images via pre-signed URLs, runs VisionAgent (call → validate →
    auto-correct up to MAX_AGENT_ITERATIONS rounds), stores the structured
    OCR output, then kicks off run_structure.
    """
    pool: asyncpg.Pool = ctx["pool"]
    settings: Settings = ctx["settings"]

    structlog.contextvars.bind_contextvars(job_id=job_id, stage="VISION")
    log.info("stage_started")

    repo = ProcessingRepository(pool)
    job = await repo.get_job(job_id)
    if not job:
        log.error("job_not_found")
        return

    await repo.update_status(job_id, "IN_PROGRESS", current_stage="VISION")

    try:
        agent = VisionAgent(settings)
        result, usage_logs = await agent.extract(list(job["image_urls"]))

        await _store_extraction_result(
            pool, job_id, "VISION", result,
            confidence=result.get("averageConfidence"),
        )
        await UsageService(pool).log_batch(job_id, usage_logs)
        log.info("stage_completed", confidence=result.get("averageConfidence"))

        await _enqueue_webhook(
            pool=pool, settings=settings, job_id=job_id,
            external_case_id=job["external_case_id"],
            external_extraction_id=job["external_extraction_id"],
            stage="VISION", status="completed",
            callback_url=job["webhook_url"],
        )
        await _enqueue_next(settings, "run_structure", job_id)

    except Exception as exc:
        log.error("stage_failed", error=str(exc))
        await _fail_job(pool, settings, job_id, "VISION", str(exc), job)
        raise


# ── Stage 2: Structure ─────────────────────────────────────────────────────────

async def run_structure(ctx: dict, job_id: str) -> None:
    """
    Stage 2 — Structured field extraction from OCR output.

    Runs StructureAgent (call → validate → auto-correct up to
    MAX_AGENT_ITERATIONS rounds) to produce validated document fields,
    then kicks off run_post_processing.
    """
    pool: asyncpg.Pool = ctx["pool"]
    settings: Settings = ctx["settings"]

    structlog.contextvars.bind_contextvars(job_id=job_id, stage="STRUCTURE")
    log.info("stage_started")

    repo = ProcessingRepository(pool)
    job = await repo.get_job(job_id)
    if not job:
        log.error("job_not_found")
        return

    await repo.update_status(job_id, "IN_PROGRESS", current_stage="TEXT")

    try:
        vision_result = await _get_extraction_result(pool, job_id, "VISION")
        if not vision_result:
            raise RuntimeError("Vision result not found — cannot run structure stage")

        agent = StructureAgent(settings)
        result, usage_logs = await agent.extract(vision_result)

        confidence = result.get("quality", {}).get("extractionConfidence")
        await _store_extraction_result(pool, job_id, "TEXT", result, confidence=confidence)
        await UsageService(pool).log_batch(job_id, usage_logs)
        log.info("stage_completed", confidence=confidence)

        await _enqueue_webhook(
            pool=pool, settings=settings, job_id=job_id,
            external_case_id=job["external_case_id"],
            external_extraction_id=job["external_extraction_id"],
            stage="TEXT", status="completed",
            callback_url=job["webhook_url"],
            result=result,
        )
        await _enqueue_next(settings, "run_post_processing", job_id)

    except Exception as exc:
        log.error("stage_failed", error=str(exc))
        await _fail_job(pool, settings, job_id, "TEXT", str(exc), job)
        raise


# ── Stage 3: Post-processing ───────────────────────────────────────────────────

async def run_post_processing(ctx: dict, job_id: str) -> None:
    """
    Stage 3 — Compile and deliver the final COMPLETED callback.

    Gathers vision and structure outputs, marks the job COMPLETED, and sends
    a single COMPLETED webhook with the full extraction result.

    The caller handles embedding separately — it calls POST /v1/embed with the
    extracted document text after receiving this webhook.
    """
    pool: asyncpg.Pool = ctx["pool"]
    settings: Settings = ctx["settings"]

    structlog.contextvars.bind_contextvars(job_id=job_id, stage="POST_PROCESSING")
    log.info("stage_started")

    repo = ProcessingRepository(pool)
    job = await repo.get_job(job_id)
    if not job:
        log.error("job_not_found")
        return

    await repo.update_status(job_id, "IN_PROGRESS", current_stage="POST_PROCESSING")

    try:
        vision_result = await _get_extraction_result(pool, job_id, "VISION")
        structure_result = await _get_extraction_result(pool, job_id, "TEXT")

        final_result = {
            "fields": structure_result,
            "ocrConfidence": vision_result.get("averageConfidence") if vision_result else None,
            "extractionConfidence": (
                structure_result.get("quality", {}).get("extractionConfidence")
                if structure_result else None
            ),
        }

        await repo.update_status(job_id, "COMPLETED", current_stage=None)
        log.info("stage_completed")

        await _enqueue_webhook(
            pool=pool, settings=settings, job_id=job_id,
            external_case_id=job["external_case_id"],
            external_extraction_id=job["external_extraction_id"],
            stage="COMPLETED", status="completed",
            callback_url=job["webhook_url"],
            result=final_result,
        )

    except Exception as exc:
        log.error("stage_failed", error=str(exc))
        await _fail_job(pool, settings, job_id, "POST_PROCESSING", str(exc), job)
        raise


# ── Webhook delivery task ──────────────────────────────────────────────────────

async def task_deliver_webhook(
    ctx: dict,
    job_id: str,
    external_case_id: str,
    external_extraction_id: str,
    stage: str,
    status: str,
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
        external_case_id=external_case_id,
        external_extraction_id=external_extraction_id,
        stage=stage,
        status=status,
        callback_url=callback_url,
        callback_secret=callback_secret,
        result=result,
        attempt_count=attempt,
    )

"""
ARQ pipeline tasks — 2 stages + webhook delivery.

Pipeline flow:
  run_vision → run_structure

Caller webhook contract:
  VISION    — OCR complete, extraction in progress (progress signal, no result data)
  COMPLETED — all extraction done, full fields delivered (terminal signal)
  FAILED    — a stage failed, includes which stage and the reason

Future stages (fraud detection, quality scoring, etc.) slot between
run_structure and the COMPLETED signal without any caller contract changes.

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
from app.agents.structure_agent import StructureAgent
from app.agents.vision_agent import VisionAgent
from app.config import Settings
from app.pipeline.enums import WebhookStage, WebhookStatus
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
        stage=WebhookStage.FAILED,
        status=WebhookStatus.FAILED,
        callback_url=job_row["webhook_url"],
        result={"failedAt": failed_at, "reason": reason},
    )


async def _enqueue_webhook(
    pool: asyncpg.Pool,
    settings: Settings,
    job_id: str,
    stage: WebhookStage,
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
    auto-correct up to MAX_AGENT_ITERATIONS rounds), stores the OCR output,
    sends a VISION progress webhook, then enqueues run_structure.
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
    started_at = datetime.now(timezone.utc)

    try:
        agent = VisionAgent(settings)
        result, usage_logs, conversation = await agent.extract(list(job["image_urls"]))

        stage_id = await _store_stage(
            pool, job_id, "VISION",
            status="SUCCESS",
            result=result,
            usage=_build_usage(usage_logs),
            started_at=started_at,
        )
        await _store_conversation(pool, stage_id, job_id, conversation)
        log.info("stage_completed", confidence=result.get("averageConfidence"))

        await _enqueue_webhook(
            pool=pool, settings=settings, job_id=job_id,
            stage=WebhookStage.VISION, status=WebhookStatus.IN_PROGRESS,
            callback_url=job["webhook_url"],
        )
        await _enqueue_next(settings, "run_structure", job_id)

    except AgentExhaustedError as exc:
        log.error("stage_failed", error=str(exc))
        stage_id = await _store_stage(
            pool, job_id, "VISION",
            status="FAILED", error=str(exc), started_at=started_at,
        )
        await _store_conversation(pool, stage_id, job_id, exc.conversation)
        await _notify_failure(pool, settings, job_id, "VISION", str(exc), job)
        raise

    except Exception as exc:
        log.error("stage_failed", error=str(exc))
        await _store_stage(
            pool, job_id, "VISION",
            status="FAILED", error=str(exc), started_at=started_at,
        )
        await _notify_failure(pool, settings, job_id, "VISION", str(exc), job)
        raise


# ── Stage 2: Structure ─────────────────────────────────────────────────────────

async def run_structure(ctx: dict, job_id: str) -> None:
    """
    Stage 2 — Structured field extraction from OCR output.

    Runs StructureAgent (call → validate → auto-correct up to
    MAX_AGENT_ITERATIONS rounds) to produce validated document fields.

    On success: marks the job COMPLETED and sends the terminal COMPLETED
    webhook with the full extraction result. The caller needs nothing else
    from docai — embedding is their responsibility after user confirmation.
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

    await repo.update_status(job_id, "IN_PROGRESS", current_stage="STRUCTURE")
    started_at = datetime.now(timezone.utc)

    try:
        vision_result = await _get_stage_result(pool, job_id, "VISION")
        if not vision_result:
            raise RuntimeError("Vision result not found — cannot run structure stage")

        agent = StructureAgent(settings)
        result, usage_logs, conversation = await agent.extract(vision_result)
        confidence = result.get("quality", {}).get("extractionConfidence")

        stage_id = await _store_stage(
            pool, job_id, "STRUCTURE",
            status="SUCCESS",
            result=result,
            usage=_build_usage(usage_logs),
            started_at=started_at,
        )
        await _store_conversation(pool, stage_id, job_id, conversation)
        log.info("stage_completed", confidence=confidence)

        await repo.update_status(job_id, "COMPLETED", current_stage=None)

        await _enqueue_webhook(
            pool=pool, settings=settings, job_id=job_id,
            stage=WebhookStage.COMPLETED, status=WebhookStatus.COMPLETED,
            callback_url=job["webhook_url"],
            result={
                "fields": result,
                "ocrConfidence": vision_result.get("averageConfidence"),
                "extractionConfidence": confidence,
            },
        )

    except AgentExhaustedError as exc:
        log.error("stage_failed", error=str(exc))
        stage_id = await _store_stage(
            pool, job_id, "STRUCTURE",
            status="FAILED", error=str(exc), started_at=started_at,
        )
        await _store_conversation(pool, stage_id, job_id, exc.conversation)
        await _notify_failure(pool, settings, job_id, "STRUCTURE", str(exc), job)
        raise

    except Exception as exc:
        log.error("stage_failed", error=str(exc))
        await _store_stage(
            pool, job_id, "STRUCTURE",
            status="FAILED", error=str(exc), started_at=started_at,
        )
        await _notify_failure(pool, settings, job_id, "STRUCTURE", str(exc), job)
        raise


# ── Webhook delivery task ──────────────────────────────────────────────────────

async def task_deliver_webhook(
    ctx: dict,
    job_id: str,
    stage: WebhookStage,
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

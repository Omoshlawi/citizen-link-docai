"""
ARQ pipeline tasks — generic stage runner + webhook delivery.

Pipeline flow (EXTRACTION):
  run_stage("VISION") → run_stage("STRUCTURE")

Caller webhook contract (event-based, dot-notation):
  {ns}.{stage}.success  — stage completed; payload is that stage's raw output
  {ns}.success          — all stages done; payload is nested { stage: result, ... }
  {ns}.{stage}.failed   — stage failed (terminal); payload is { reason }
  {ns}.failed           — flat rollup failure (fires alongside stage event);
                          payload is { failedAt, reason }

Example for EXTRACTION:
  extraction.vision.success      → { averageConfidence, fullText, … }
  extraction.structure.success   → { documentType, person, document, … }
  extraction.success             → { vision: {…}, structure: {…} }   ← terminal happy path
  extraction.vision.failed       → { reason }                         ← terminal, gate rejected
  extraction.structure.failed    → { reason }                         ← terminal, agent failed
  extraction.failed              → { failedAt, reason }               ← terminal rollup

New pipelines register in app/pipeline/registry.py — no changes needed here.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

import asyncpg
import structlog

from app.agents.exceptions import AgentExhaustedError
from app.config import Settings
from app.models.pipeline import ConversationEntry, JobRecord, UsageSummary
from app.pipeline.enums import DocaiEvent, JobStatus, StageStatus
from app.pipeline.webhook import deliver_webhook
from app.processing.repository import ProcessingRepository

log = structlog.get_logger(__name__)


# ── DB helpers ─────────────────────────────────────────────────────────────────

async def _store_stage(
    pool: asyncpg.Pool,
    job_id: str,
    stage: str,
    *,
    status: StageStatus,
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
        status.value,
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
    conversation: list[ConversationEntry],
) -> None:
    """Bulk-insert one stage_conversations row per message turn."""
    if not conversation:
        return
    rows = [
        (
            stage_id,
            job_id,
            entry.round,
            entry.page,
            entry.role,
            entry.content,
            entry.success,
            json.dumps(entry.metadata) if entry.metadata is not None else None,
        )
        for entry in conversation
    ]
    await pool.executemany(
        """
        INSERT INTO stage_conversations
            (stage_id, job_id, round, page, role, content, success, metadata)
        VALUES
            ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8::jsonb)
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


# ── Failure helper ─────────────────────────────────────────────────────────────

async def _notify_failure(
    pool: asyncpg.Pool,
    arq_redis,
    settings: Settings,
    job_id: str,
    namespace: str,
    failed_at: str,
    reason: str,
    job: JobRecord,
) -> None:
    """
    Mark the job FAILED and enqueue two failure webhooks:
      1. {namespace}.{stage}.failed  — stage-specific, payload { reason }
      2. {namespace}.failed          — flat rollup,      payload { failedAt, reason }

    Stage row must already exist before calling this.
    """
    stage_event  = DocaiEvent(f"{namespace}.{failed_at.lower()}.failed")
    rollup_event = DocaiEvent(f"{namespace}.failed")

    await ProcessingRepository(pool).update_status(job_id, JobStatus.FAILED)

    await _enqueue_webhook(
        pool=pool, arq_redis=arq_redis, settings=settings, job_id=job_id,
        event=stage_event, callback_url=job.webhook_url,
        result={"reason": reason},
    )
    await _enqueue_webhook(
        pool=pool, arq_redis=arq_redis, settings=settings, job_id=job_id,
        event=rollup_event, callback_url=job.webhook_url,
        result={"failedAt": failed_at.lower(), "reason": reason},
    )


# ── ARQ enqueue helpers ────────────────────────────────────────────────────────

async def _enqueue_webhook(
    pool: asyncpg.Pool,
    arq_redis,
    settings: Settings,
    job_id: str,
    event: DocaiEvent,
    callback_url: str,
    result: Optional[dict] = None,
) -> None:
    await arq_redis.enqueue_job(
        "task_deliver_webhook",
        job_id,
        event,
        callback_url,
        settings.callback_secret,
        result,
    )


async def _enqueue_next(arq_redis, task_name: str, *args) -> None:
    await arq_redis.enqueue_job(task_name, *args)


# ── Generic stage runner ───────────────────────────────────────────────────────

async def run_stage(ctx: dict, job_id: str, stage: str) -> None:
    """
    Generic ARQ task — runs one pipeline stage for any job type.

    Dispatches to the correct agent via the pipeline registry, stores the
    result, always fires a {namespace}.{stage}.success event with the raw
    stage output, then either enqueues the next stage or fires the terminal
    {namespace}.success event with the nested combined result.
    """
    from app.pipeline.registry import get_agent, get_pipeline

    pool: asyncpg.Pool = ctx["pool"]
    arq_redis = ctx["redis"]
    settings: Settings = ctx["settings"]

    structlog.contextvars.bind_contextvars(job_id=job_id, stage=stage)
    log.info("stage_started")

    repo = ProcessingRepository(pool)
    job = await repo.get_job(job_id)
    if not job:
        log.error("job_not_found")
        return

    await repo.update_status(job_id, JobStatus.IN_PROGRESS, current_stage=stage)
    started_at = datetime.now(timezone.utc)

    try:
        pipeline = get_pipeline(job.job_type)
        stage_index = pipeline.stages.index(stage)

        # Collect plain-dict results from all stages that ran before this one
        # (loaded from DB JSONB — no model overhead for stages we don't own)
        previous_results: dict[str, dict] = {}
        for prev_stage in pipeline.stages[:stage_index]:
            prev_result = await _get_stage_result(pool, job_id, prev_stage)
            if prev_result is not None:
                previous_results[prev_stage] = prev_result

        # Run the agent — returns typed (result_model, usage_entries, conversation_entries)
        agent = get_agent(job.job_type, stage, settings)
        result_model, usage_entries, conversation = await agent.run(
            job.input, previous_results
        )

        # Serialise for DB storage
        usage_summary = UsageSummary.from_entries(usage_entries)

        stage_id = await _store_stage(
            pool, job_id, stage,
            status=StageStatus.SUCCESS,
            result=result_model.to_dict(),
            usage=usage_summary.to_dict(),
            started_at=started_at,
        )
        await _store_conversation(pool, stage_id, job_id, conversation)
        log.info("stage_completed")

        next_index = stage_index + 1
        has_next = next_index < len(pipeline.stages)

        # Post-stage gate — rule-based fast-fail before spending tokens on next stage.
        # Only checked when a subsequent stage exists; the final stage is never gated.
        # Post-stage gate — fast-fail before spending tokens on next stage.
        # Runs before the success event so a gate rejection never fires *.success.
        if has_next:
            gate = pipeline.post_stage_gate.get(stage)
            if gate:
                gate_error = gate(result_model.to_dict())
                if gate_error:
                    log.warning("stage_gate_failed", stage=stage, reason=gate_error)
                    await _notify_failure(pool, arq_redis, settings, job_id, pipeline.namespace, stage, gate_error, job)
                    return  # expected business outcome — no retry, no exception

        # Always fire a per-stage success event with the raw stage output.
        stage_success_event = DocaiEvent(f"{pipeline.namespace}.{stage.lower()}.success")
        await _enqueue_webhook(
            pool=pool, arq_redis=arq_redis, settings=settings, job_id=job_id,
            event=stage_success_event,
            callback_url=job.webhook_url,
            result=result_model.to_dict(),
        )

        if has_next:
            next_stage = pipeline.stages[next_index]
            log.info("enqueueing_next_stage", next_stage=next_stage)
            await _enqueue_next(arq_redis, "run_stage", job_id, next_stage)
        else:
            # Final stage — fire the terminal {namespace}.success with the nested combined result.
            all_results = {**previous_results, stage: result_model.to_dict()}
            pipeline_success_event = DocaiEvent(f"{pipeline.namespace}.success")

            await repo.update_status(job_id, JobStatus.COMPLETED, current_stage=None)
            await _enqueue_webhook(
                pool=pool, arq_redis=arq_redis, settings=settings, job_id=job_id,
                event=pipeline_success_event,
                callback_url=job.webhook_url,
                result=pipeline.build_result(all_results),
            )
            log.info("pipeline_completed", job_type=job.job_type)

    except AgentExhaustedError as exc:
        log.error("stage_failed", error=str(exc))
        stage_id = await _store_stage(
            pool, job_id, stage,
            status=StageStatus.FAILED,
            error=str(exc),
            started_at=started_at,
        )
        await _store_conversation(pool, stage_id, job_id, exc.conversation)
        await _notify_failure(pool, arq_redis, settings, job_id, pipeline.namespace, stage, str(exc), job)
        raise

    except Exception as exc:
        log.error("stage_failed", error=str(exc))
        await _store_stage(
            pool, job_id, stage,
            status=StageStatus.FAILED,
            error=str(exc),
            started_at=started_at,
        )
        await _notify_failure(pool, arq_redis, settings, job_id, pipeline.namespace, stage, str(exc), job)
        raise


# ── Webhook delivery task ──────────────────────────────────────────────────────

async def task_deliver_webhook(
    ctx: dict,
    job_id: str,
    event: DocaiEvent,
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

    structlog.contextvars.bind_contextvars(job_id=job_id, event=event)
    log.info("webhook_task_started", attempt=attempt)

    await deliver_webhook(
        pool=pool,
        job_id=job_id,
        event=event,
        callback_url=callback_url,
        callback_secret=callback_secret,
        result=result,
        attempt_count=attempt,
    )

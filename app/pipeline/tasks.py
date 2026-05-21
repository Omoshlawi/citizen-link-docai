"""
ARQ pipeline tasks — 4 stages + webhook delivery.

Each stage is an async coroutine that ARQ calls as a job.
The ctx dict is populated by ARQ from WorkerSettings.ctx at startup.

Pipeline flow:
  run_vision → run_structure → run_embedding → run_post_processing
  (each stage enqueues the next + enqueues deliver_webhook for its own result)

Error handling:
  Any unhandled exception in a stage updates the job to FAILED and
  enqueues a deliver_webhook with stage=FAILED so NestJS is notified.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

import asyncpg
import structlog

from app.agents.structure_agent import StructureAgent
from app.agents.vision_agent import VisionAgent
from app.config import Settings
from app.embedding.service import EmbeddingService
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
    """Mark a job FAILED and send a failure webhook to NestJS."""
    repo = ProcessingRepository(pool)
    await repo.update_status(job_id, "FAILED", error_message=reason)

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
    """Persist a stage output to extraction_results."""
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
    """Fetch a previously stored stage result."""
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
    """Enqueue a deliver_webhook ARQ task."""
    from arq import create_pool as arq_create_pool
    from arq.connections import RedisSettings

    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    arq_pool = await arq_create_pool(redis_settings)
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
    """Enqueue the next pipeline stage."""
    from arq import create_pool as arq_create_pool
    from arq.connections import RedisSettings

    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    arq_pool = await arq_create_pool(redis_settings)
    await arq_pool.enqueue_job(task_name, job_id)
    await arq_pool.close()


# ── Stage 1: Vision ────────────────────────────────────────────────────────────

async def run_vision(ctx: dict, job_id: str) -> None:
    """
    Stage 1 — Vision extraction.

    Downloads images from pre-signed S3 URLs and runs VisionAgent.
    Stores output to extraction_results (VISION) and enqueues run_structure.
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
            pool, job_id, "VISION", result, confidence=result.get("averageConfidence")
        )
        await UsageService(pool).log_batch(job_id, usage_logs)

        log.info("stage_completed", confidence=result.get("averageConfidence"))

        # Notify NestJS that vision is done
        await _enqueue_webhook(
            pool=pool,
            settings=settings,
            job_id=job_id,
            external_case_id=job["external_case_id"],
            external_extraction_id=job["external_extraction_id"],
            stage="VISION",
            status="completed",
            callback_url=job["webhook_url"],
        )

        # Kick off next stage
        await _enqueue_next(settings, "run_structure", job_id)

    except Exception as exc:
        log.error("stage_failed", error=str(exc))
        await _fail_job(pool, settings, job_id, "VISION", str(exc), job)
        raise


# ── Stage 2: Structure ─────────────────────────────────────────────────────────

async def run_structure(ctx: dict, job_id: str) -> None:
    """
    Stage 2 — Structure extraction.

    Takes vision output and runs StructureAgent to produce validated
    document fields. Stores output to extraction_results (TEXT).
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
            pool=pool,
            settings=settings,
            job_id=job_id,
            external_case_id=job["external_case_id"],
            external_extraction_id=job["external_extraction_id"],
            stage="TEXT",
            status="completed",
            callback_url=job["webhook_url"],
            result=result,
        )

        await _enqueue_next(settings, "run_embedding", job_id)

    except Exception as exc:
        log.error("stage_failed", error=str(exc))
        await _fail_job(pool, settings, job_id, "TEXT", str(exc), job)
        raise


# ── Stage 3: Embedding ─────────────────────────────────────────────────────────

async def run_embedding(ctx: dict, job_id: str) -> None:
    """
    Stage 3 — Embedding generation.

    Converts structured text into a vector and stores it in extraction_results
    (EMBEDDING). NestJS post-processing webhook includes the vector so NestJS
    can store it in documents.embedding_<dims>.
    """
    pool: asyncpg.Pool = ctx["pool"]
    settings: Settings = ctx["settings"]

    structlog.contextvars.bind_contextvars(job_id=job_id, stage="EMBEDDING")
    log.info("stage_started")

    repo = ProcessingRepository(pool)
    job = await repo.get_job(job_id)
    if not job:
        log.error("job_not_found")
        return

    await repo.update_status(job_id, "IN_PROGRESS", current_stage="EMBEDDING")

    try:
        structure_result = await _get_extraction_result(pool, job_id, "TEXT")
        if not structure_result:
            raise RuntimeError("Structure result not found — cannot run embedding stage")

        # Build the embedding text from the structured fields
        embedding_text = _build_embedding_text(structure_result)

        import time
        svc = EmbeddingService(settings)
        start = time.perf_counter()
        vector = await svc.embed(embedding_text, use_case="document")
        latency_ms = round((time.perf_counter() - start) * 1000, 2)

        embedding_result = {
            "vector": vector,
            "dims": len(vector),
            "model": svc.model,
            "text": embedding_text,
        }

        await _store_extraction_result(pool, job_id, "EMBEDDING", embedding_result)
        await UsageService(pool).log_batch(
            job_id,
            [
                {
                    "stage": "EMBEDDING",
                    "model": svc.model,
                    "provider": "ollama" if settings.embedding_is_openai is False else "openai",
                    "input_tokens": None,
                    "output_tokens": None,
                    "latency_ms": latency_ms,
                    "success": True,
                }
            ],
        )

        log.info("stage_completed", dims=len(vector))

        await _enqueue_webhook(
            pool=pool,
            settings=settings,
            job_id=job_id,
            external_case_id=job["external_case_id"],
            external_extraction_id=job["external_extraction_id"],
            stage="EMBEDDING",
            status="completed",
            callback_url=job["webhook_url"],
        )

        await _enqueue_next(settings, "run_post_processing", job_id)

    except Exception as exc:
        log.error("stage_failed", error=str(exc))
        await _fail_job(pool, settings, job_id, "EMBEDDING", str(exc), job)
        raise


# ── Stage 4: Post-processing ───────────────────────────────────────────────────

async def run_post_processing(ctx: dict, job_id: str) -> None:
    """
    Stage 4 — Compile final result and send COMPLETED webhook to NestJS.

    Gathers outputs from all prior stages and sends a single COMPLETED
    webhook with the full extraction result + embedding vector.
    NestJS uses this payload to update AIExtraction and store the vector.
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
        embedding_result = await _get_extraction_result(pool, job_id, "EMBEDDING")

        final_result = {
            "fields": structure_result,
            "embedding": embedding_result,
            "ocrConfidence": vision_result.get("averageConfidence") if vision_result else None,
            "extractionConfidence": (
                structure_result.get("quality", {}).get("extractionConfidence")
                if structure_result
                else None
            ),
        }

        await repo.update_status(job_id, "COMPLETED", current_stage=None)
        log.info("stage_completed")

        await _enqueue_webhook(
            pool=pool,
            settings=settings,
            job_id=job_id,
            external_case_id=job["external_case_id"],
            external_extraction_id=job["external_extraction_id"],
            stage="COMPLETED",
            status="completed",
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
    ARQ task — delivers one webhook callback to NestJS.

    ARQ retries this task automatically on failure (up to webhook_max_retries).
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


# ── Embedding text builder ─────────────────────────────────────────────────────

def _build_embedding_text(structure: dict) -> str:
    """
    Build a rich text string from structured document fields for embedding.
    Mirrors the createDocumentText logic in NestJS EmbeddingService.
    """
    parts: list[str] = []

    person = structure.get("person", {})
    doc_type = structure.get("documentType", {})
    document = structure.get("document", {})
    address = structure.get("address", {})
    additional = structure.get("additionalFields", [])

    full_name = person.get("fullName")
    type_code = doc_type.get("code", "")

    if full_name and type_code:
        parts.append(f"This is a {type_code} document belonging to {full_name}")

    if full_name:
        parts.append(f"Full name: {full_name}")
    if person.get("surname"):
        parts.append(f"Surname: {person['surname']}")

    given_names = person.get("givenNames", [])
    if given_names:
        parts.append(f"Given names: {' '.join(given_names)}")
    if person.get("dateOfBirth"):
        parts.append(f"Date of birth: {person['dateOfBirth']}")
    if person.get("gender"):
        parts.append(f"Gender: {person['gender']}")
    if person.get("placeOfBirth"):
        parts.append(f"Place of birth: {person['placeOfBirth']}")

    if type_code:
        parts.append(f"Document type: {type_code}")
    if document.get("number"):
        parts.append(f"Document number: {document['number']}")
    if document.get("serialNumber"):
        parts.append(f"Serial number: {document['serialNumber']}")
    if document.get("issuer"):
        parts.append(f"Issued by: {document['issuer']}")
    if document.get("placeOfIssue"):
        parts.append(f"Place of issue: {document['placeOfIssue']}")

    if address.get("raw"):
        parts.append(f"Address: {address['raw']}")

    components = address.get("components", [])
    if components:
        comp_str = ", ".join(f"{c['type']}: {c['value']}" for c in components)
        parts.append(f"Address details: {comp_str}")

    for field in additional:
        parts.append(f"{field.get('fieldName', '')}: {field.get('fieldValue', '')}")

    return ". ".join(filter(None, parts)) + "."

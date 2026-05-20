"""
WebhookDelivery — sends stage callbacks to NestJS and logs every attempt.

This module is called from within ARQ tasks (deliver_webhook task).
It does NOT make direct HTTP calls from pipeline stages — the stages enqueue
a deliver_webhook ARQ task, which calls this module. This enables:
  - Automatic retry on failure (ARQ retries the task)
  - Manual re-enqueue for debugging
  - Full audit trail in webhook_deliveries table
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

import asyncpg
import httpx
import structlog

log = structlog.get_logger(__name__)


async def _log_delivery(
    pool: asyncpg.Pool,
    job_id: str,
    stage: str,
    payload: dict,
    nestjs_url: str,
    response_status: Optional[int],
    response_body: Optional[str],
    attempt_count: int,
    delivered: bool,
) -> None:
    """Insert one webhook_deliveries row."""
    await pool.execute(
        """
        INSERT INTO webhook_deliveries (
            job_id, stage, payload, nestjs_url,
            response_status, response_body,
            attempt_count, delivered
        )
        VALUES ($1::uuid, $2, $3::jsonb, $4, $5, $6, $7, $8)
        """,
        job_id,
        stage,
        json.dumps(payload),
        nestjs_url,
        response_status,
        response_body,
        attempt_count,
        delivered,
    )


async def deliver_webhook(
    pool: asyncpg.Pool,
    job_id: str,
    external_case_id: str,
    external_extraction_id: str,
    stage: str,
    status: str,
    nestjs_url: str,
    nestjs_secret: str,
    result: Optional[dict] = None,
    attempt_count: int = 1,
) -> None:
    """
    POST a stage callback to NestJS.

    Raises on HTTP error so ARQ can retry the task automatically.
    Every attempt (success or failure) is logged to webhook_deliveries.
    """
    payload = {
        "jobId": job_id,
        "externalCaseId": external_case_id,
        "externalExtractionId": external_extraction_id,
        "stage": stage,
        "status": status,
        "result": result or {},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    response_status: Optional[int] = None
    response_body: Optional[str] = None
    delivered = False

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                nestjs_url,
                json=payload,
                headers={
                    "X-Internal-Secret": nestjs_secret,
                    "Content-Type": "application/json",
                },
            )
            response_status = response.status_code
            response_body = response.text[:2000]  # cap to avoid huge DB values
            response.raise_for_status()
            delivered = True

        log.info(
            "webhook_delivered",
            job_id=job_id,
            stage=stage,
            status_code=response_status,
        )

    except httpx.HTTPStatusError as exc:
        log.error(
            "webhook_http_error",
            job_id=job_id,
            stage=stage,
            status_code=exc.response.status_code,
            attempt=attempt_count,
        )
        raise  # Let ARQ retry

    except Exception as exc:
        log.error(
            "webhook_delivery_failed",
            job_id=job_id,
            stage=stage,
            error=str(exc),
            attempt=attempt_count,
        )
        raise  # Let ARQ retry

    finally:
        # Always log the attempt — even if it failed
        try:
            await _log_delivery(
                pool=pool,
                job_id=job_id,
                stage=stage,
                payload=payload,
                nestjs_url=nestjs_url,
                response_status=response_status,
                response_body=response_body,
                attempt_count=attempt_count,
                delivered=delivered,
            )
        except Exception as log_exc:
            log.error("webhook_delivery_log_failed", error=str(log_exc))

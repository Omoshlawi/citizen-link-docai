"""
Processing router — job submission and status inspection.

Submission endpoints (one per pipeline type):
  POST /v1/jobs/extraction       — OCR + structured field extraction

Status / inspection endpoints (shared across all pipelines):
  GET  /v1/jobs                  — list all jobs with optional filters
  GET  /v1/jobs/{job_id}         — poll a single job's status

Authentication: X-Internal-Secret + X-User-Id (require_internal_auth).

Adding a new pipeline:
  1. Add a typed request schema in schemas.py
  2. Add a submit_* method in service.py
  3. Add a POST route here — the worker, registry, and DB stay unchanged
"""

from typing import Optional

import structlog
from fastapi import APIRouter, Depends, Query, Request

from app.config import Settings, get_settings
from app.dependencies import get_pool, require_internal_auth
from app.exceptions import NotFoundError
from app.processing.repository import ProcessingRepository
from app.processing.schemas import (
    ExtractionRequest,
    JobListResponse,
    JobStatusResponse,
    ProcessResponse,
)
from app.processing.service import ProcessingService

log = structlog.get_logger(__name__)
router = APIRouter(tags=["jobs"])


# ── Dependency ─────────────────────────────────────────────────────────────────

def _get_service(
    request: Request,
    settings: Settings = Depends(get_settings),
) -> ProcessingService:
    return ProcessingService(get_pool(request), settings)


def _row_to_job(row) -> JobStatusResponse:
    return JobStatusResponse(
        job_id=str(row["id"]),
        job_type=row["job_type"],
        status=row["status"],
        current_stage=row["current_stage"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ── Submission endpoints ───────────────────────────────────────────────────────

@router.post(
    "/jobs/extraction",
    status_code=202,
    response_model=ProcessResponse,
    summary="Submit an extraction job",
    description=(
        "Enqueue an OCR + structured field extraction pipeline for one or more "
        "document images. Returns 202 immediately — the caller receives event-based "
        "webhook callbacks as each stage completes.\n\n"
        "**Webhook events (dot-notation):**\n"
        "- `extraction.vision.success` — OCR complete, raw vision output in result\n"
        "- `extraction.structure.success` — field extraction complete, raw structure output\n"
        "- `extraction.success` — terminal; nested `{ vision, structure }` combined result\n"
        "- `extraction.vision.failed` / `extraction.structure.failed` — stage-specific failure\n"
        "- `extraction.failed` — flat rollup failure; `{ failedAt, reason }`"
    ),
)
async def submit_extraction(
    body: ExtractionRequest,
    user_id: str = Depends(require_internal_auth),
    svc: ProcessingService = Depends(_get_service),
) -> ProcessResponse:
    job_id = await svc.submit_extraction(body)
    log.info("extraction_job_accepted", job_id=job_id, user_id=user_id, case_number=body.case_number)
    return ProcessResponse(job_id=job_id)


# Future pipeline endpoints slot in here:
#
# @router.post("/jobs/fraud-check", status_code=202, response_model=ProcessResponse)
# async def submit_fraud_check(body: FraudCheckRequest, ...) -> ProcessResponse:
#     ...
#
# @router.post("/jobs/match-verification", status_code=202, response_model=ProcessResponse)
# async def submit_match_verification(body: MatchVerificationRequest, ...) -> ProcessResponse:
#     ...


# ── Status / inspection endpoints ──────────────────────────────────────────────

@router.get(
    "/jobs",
    response_model=JobListResponse,
    summary="List all pipeline jobs",
    description=(
        "List all pipeline jobs, newest first. Useful for monitoring dashboards "
        "and debugging. NestJS uses webhooks for real-time updates."
    ),
)
async def list_jobs(
    request: Request,
    _: str = Depends(require_internal_auth),
    job_type: Optional[str] = Query(
        default=None,
        description="Filter by pipeline type: EXTRACTION, FRAUD_DETECTION, …",
    ),
    status: Optional[str] = Query(
        default=None,
        description="Filter by status: PENDING, IN_PROGRESS, COMPLETED, FAILED",
    ),
    page: int = Query(default=1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(default=20, ge=1, le=100, description="Results per page"),
) -> JobListResponse:
    pool = get_pool(request)
    repo = ProcessingRepository(pool)
    rows, total = await repo.list_jobs(
        job_type=job_type, status=status, page=page, page_size=page_size
    )
    return JobListResponse(
        jobs=[_row_to_job(row) for row in rows],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get(
    "/jobs/{job_id}",
    response_model=JobStatusResponse,
    summary="Get a single job's status",
    description=(
        "Fetch the current status of one pipeline job. "
        "NestJS primarily relies on webhooks — this endpoint is useful as a "
        "fallback or for debugging a specific job."
    ),
)
async def get_job_status(
    job_id: str,
    request: Request,
    _: str = Depends(require_internal_auth),
) -> JobStatusResponse:
    pool = get_pool(request)
    row = await ProcessingRepository(pool).get_job(job_id)
    if not row:
        raise NotFoundError(f"Job {job_id} not found")
    return _row_to_job(row)

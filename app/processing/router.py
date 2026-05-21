"""
Processing router.

POST /v1/process        — NestJS fires an extraction job (fire-and-forget, 202 Accepted)
GET  /v1/jobs           — list all jobs with optional status filter and pagination
GET  /v1/jobs/{job_id} — poll a single job's status

Authentication: X-Internal-Secret + X-User-Id (require_internal_auth).
"""

import structlog
from fastapi import APIRouter, Depends, Query, Request
from typing import Optional

from app.config import Settings, get_settings
from app.dependencies import get_pool, require_internal_auth
from app.exceptions import NotFoundError
from app.processing.repository import ProcessingRepository
from app.processing.schemas import JobListResponse, JobStatusResponse, ProcessRequest, ProcessResponse
from app.processing.service import ProcessingService

log = structlog.get_logger(__name__)
router = APIRouter(tags=["processing"])


def get_processing_service(
    request: Request,
    settings: Settings = Depends(get_settings),
) -> ProcessingService:
    pool = get_pool(request)
    return ProcessingService(pool, settings)


def _row_to_job(row) -> JobStatusResponse:
    """Convert an asyncpg Record to a JobStatusResponse."""
    return JobStatusResponse(
        job_id=str(row["id"]),
        external_case_id=row["external_case_id"],
        external_document_id=row["external_document_id"],
        external_extraction_id=row["external_extraction_id"],
        external_user_id=row["external_user_id"],
        case_type=row["case_type"],
        case_number=row["case_number"],
        status=row["status"],
        current_stage=row["current_stage"],
        error_message=row["error_message"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@router.post("/process", status_code=202, response_model=ProcessResponse)
async def submit_process(
    body: ProcessRequest,
    user_id: str = Depends(require_internal_auth),
    svc: ProcessingService = Depends(get_processing_service),
) -> ProcessResponse:
    """
    Accept an extraction job from the caller.

    Returns 202 immediately — processing happens asynchronously via ARQ.
    The caller receives progress via webhook callbacks as each stage completes.
    """
    job_id = await svc.submit_job(body)
    log.info("process_request_accepted", job_id=job_id, user_id=user_id)
    return ProcessResponse(job_id=job_id)


@router.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    request: Request,
    _: str = Depends(require_internal_auth),
    status: Optional[str] = Query(
        default=None,
        description="Filter by status: PENDING, IN_PROGRESS, COMPLETED, FAILED",
    ),
    page: int = Query(default=1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(default=20, ge=1, le=100, description="Results per page"),
) -> JobListResponse:
    """
    List all extraction jobs, newest first.

    Optional filters:
    - `status` — narrow to a specific pipeline status
    - `page` / `page_size` — pagination

    Useful for dashboards and debugging. NestJS uses webhooks for real-time
    updates; this endpoint is for inspection and monitoring.
    """
    pool = get_pool(request)
    repo = ProcessingRepository(pool)
    rows, total = await repo.list_jobs(status=status, page=page, page_size=page_size)

    return JobListResponse(
        jobs=[_row_to_job(row) for row in rows],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(
    job_id: str,
    request: Request,
    _: str = Depends(require_internal_auth),
) -> JobStatusResponse:
    """
    Fetch the current status of a single extraction job.

    NestJS primarily relies on webhooks, but this endpoint is useful for
    polling as a fallback or for debugging a specific job.
    """
    pool = get_pool(request)
    repo = ProcessingRepository(pool)
    row = await repo.get_job(job_id)

    if not row:
        raise NotFoundError(f"Job {job_id} not found")

    return _row_to_job(row)

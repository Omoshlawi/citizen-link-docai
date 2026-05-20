"""
Processing router.

POST /v1/process  — NestJS fires an extraction job (fire-and-forget, 202 Accepted)
GET  /v1/jobs/{job_id} — poll job status (optional — NestJS mainly uses webhooks)

Authentication: X-Internal-Secret + X-User-Id (require_internal_auth).
"""

import structlog
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.config import Settings, get_settings
from app.dependencies import get_pool, require_internal_auth
from app.exceptions import NotFoundError
from app.processing.repository import ProcessingRepository
from app.processing.schemas import JobStatusResponse, ProcessRequest
from app.processing.service import ProcessingService

log = structlog.get_logger(__name__)
router = APIRouter(tags=["processing"])


def get_processing_service(
    request: Request,
    settings: Settings = Depends(get_settings),
) -> ProcessingService:
    pool = get_pool(request)
    return ProcessingService(pool, settings)


@router.post("/process", status_code=202)
async def submit_process(
    body: ProcessRequest,
    user_id: str = Depends(require_internal_auth),
    svc: ProcessingService = Depends(get_processing_service),
) -> JSONResponse:
    """
    Accept an extraction job from NestJS.

    Returns 202 immediately — processing happens asynchronously via ARQ.
    NestJS receives progress via webhook callbacks as each stage completes.
    """
    job_id = await svc.submit_job(body)
    log.info("process_request_accepted", job_id=job_id, user_id=user_id)
    return JSONResponse(
        status_code=202,
        content={"jobId": job_id, "status": "PENDING"},
    )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(
    job_id: str,
    _: str = Depends(require_internal_auth),
    request: Request = None,
) -> JobStatusResponse:
    """
    Poll the status of an extraction job.

    NestJS primarily uses webhooks, but this endpoint allows polling as a fallback
    or for debugging purposes.
    """
    pool = get_pool(request)
    repo = ProcessingRepository(pool)
    row = await repo.get_job(job_id)

    if not row:
        raise NotFoundError(f"Job {job_id} not found")

    return JobStatusResponse(
        job_id=str(row["id"]),
        external_case_id=row["external_case_id"],
        external_extraction_id=row["external_extraction_id"],
        status=row["status"],
        current_stage=row["current_stage"],
        error_message=row["error_message"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )

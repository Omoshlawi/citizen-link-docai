"""
Inspection router — read-only query endpoints for pipeline observability.

All endpoints join related records so callers get full context in one request.
All endpoints require X-Internal-Secret authentication.

Stages:
  GET /v1/stages                          list with filters
  GET /v1/stages/{stage_id}               single stage + optional conversations
  GET /v1/jobs/{job_id}/stages            all stages for one job + optional conversations

Conversations:
  GET /v1/conversations                   list with filters
  GET /v1/stages/{stage_id}/conversations all rounds for one stage

Webhook deliveries:
  GET /v1/webhooks                        list with filters
  GET /v1/webhooks/{delivery_id}          single delivery with full payload
"""

from typing import Optional

from fastapi import APIRouter, Depends, Query, Request

from app.dependencies import get_pool, require_internal_auth
from app.exceptions import NotFoundError
from app.inspection.repository import InspectionRepository, _loads
from app.inspection.schemas import (
    ConversationListResponse,
    ConversationResponse,
    JobStagesResponse,
    StageDetail,
    StageListResponse,
    StageResponse,
    WebhookDeliveryListResponse,
    WebhookDeliveryResponse,
)

router = APIRouter(tags=["inspection"])


# ── Row → schema helpers ───────────────────────────────────────────────────────

def _to_stage(row, *, include_result: bool = False) -> StageResponse:
    return StageResponse(
        stage_id=row["stage_id"],
        job_id=row["job_id"],
        stage=row["stage"],
        status=row["status"],
        error=row["error"],
        usage=_loads(row["usage"]),
        started_at=row["started_at"],
        completed_at=row["completed_at"],
        created_at=row["created_at"],
        result=_loads(row["result"]) if include_result else None,
        job_type=row["job_type"],
        job_status=row["job_status"],
    )


def _to_conversation(row) -> ConversationResponse:
    return ConversationResponse(
        conversation_id=row["conversation_id"],
        stage_id=row["stage_id"],
        job_id=row["job_id"],
        round=row["round"],
        page=row["page"],
        role=row["role"],
        content=row["content"],
        success=row["success"],
        metadata=_loads(row["metadata"]),
        created_at=row["created_at"],
        stage_name=row["stage_name"],
        stage_status=row["stage_status"],
        job_type=row["job_type"],
    )


def _to_webhook(row) -> WebhookDeliveryResponse:
    return WebhookDeliveryResponse(
        delivery_id=row["delivery_id"],
        job_id=row["job_id"],
        event=row["event"],
        callback_url=row["callback_url"],
        response_status=row["response_status"],
        response_body=row["response_body"],
        attempt_count=row["attempt_count"],
        delivered=row["delivered"],
        created_at=row["created_at"],
        payload=_loads(row["payload"]),
        job_type=row["job_type"],
        job_status=row["job_status"],
    )


# ── Stages ─────────────────────────────────────────────────────────────────────

@router.get(
    "/stages",
    response_model=StageListResponse,
    summary="List pipeline stages",
    description=(
        "Paginated list of processing stages across all jobs, newest first.\n\n"
        "Each record includes the parent job context (type, status, priority).\n\n"
        "**Filters** — all optional, combinable:\n"
        "- `job_id` — stages for a specific job\n"
        "- `job_type` — stages from a specific pipeline (EXTRACTION, …)\n"
        "- `stage` — specific stage name (VISION, STRUCTURE, …)\n"
        "- `status` — SUCCESS or FAILED\n\n"
        "`include_result=true` adds the raw stage output JSONB — can be large for VISION stages."
    ),
)
async def list_stages(
    request: Request,
    _: str = Depends(require_internal_auth),
    job_id: Optional[str] = Query(None, description="Filter by job UUID"),
    job_type: Optional[str] = Query(None, description="Filter by pipeline type (e.g. EXTRACTION)"),
    stage: Optional[str] = Query(None, description="Filter by stage name (e.g. VISION, STRUCTURE)"),
    status: Optional[str] = Query(None, description="Filter by stage status: SUCCESS or FAILED"),
    include_result: bool = Query(False, description="Include the raw stage output JSONB in each record"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> StageListResponse:
    repo = InspectionRepository(get_pool(request))
    rows, total = await repo.list_stages(
        job_id=job_id, job_type=job_type, stage=stage, status=status,
        include_result=include_result, page=page, page_size=page_size,
    )
    return StageListResponse(
        stages=[_to_stage(r, include_result=include_result) for r in rows],
        total=total, page=page, page_size=page_size,
    )


@router.get(
    "/stages/{stage_id}",
    response_model=StageDetail,
    summary="Get a single stage",
    description=(
        "Fetch one processing stage by ID, joined with its parent job.\n\n"
        "- `include_result=true` — adds the raw stage output JSONB\n"
        "- `include_conversations=true` — nests all LLM correction rounds"
    ),
)
async def get_stage(
    stage_id: str,
    request: Request,
    _: str = Depends(require_internal_auth),
    include_result: bool = Query(True, description="Include raw stage output JSONB"),
    include_conversations: bool = Query(False, description="Nest all LLM correction rounds"),
) -> StageDetail:
    repo = InspectionRepository(get_pool(request))
    row = await repo.get_stage(stage_id, include_result=include_result)
    if not row:
        raise NotFoundError(f"Stage {stage_id} not found")

    conversations = []
    if include_conversations:
        conv_rows = await repo.list_conversations_for_stage(stage_id)
        conversations = [_to_conversation(r) for r in conv_rows]

    return StageDetail(**_to_stage(row, include_result=include_result).model_dump(), conversations=conversations)


@router.get(
    "/jobs/{job_id}/stages",
    response_model=JobStagesResponse,
    summary="Get all stages for a job",
    description=(
        "All pipeline stages for one job, in execution order.\n\n"
        "- `include_result=true` — adds raw output JSONB to each stage\n"
        "- `include_conversations=true` — nests LLM correction rounds under each stage\n\n"
        "This is the most complete single-request view of a job's execution history."
    ),
)
async def get_job_stages(
    job_id: str,
    request: Request,
    _: str = Depends(require_internal_auth),
    include_result: bool = Query(True, description="Include raw stage output JSONB"),
    include_conversations: bool = Query(False, description="Nest LLM correction rounds under each stage"),
) -> JobStagesResponse:
    repo = InspectionRepository(get_pool(request))

    stage_rows = await repo.list_stages_for_job(job_id, include_result=include_result)
    if not stage_rows:
        # Verify the job actually exists before returning an empty list
        from app.processing.repository import ProcessingRepository
        job = await ProcessingRepository(get_pool(request)).get_job(job_id)
        if not job:
            raise NotFoundError(f"Job {job_id} not found")
        return JobStagesResponse(
            job_id=job_id,
            job_type=job["job_type"],
            job_status=job["status"],
            stages=[],
        )

    # Build a {stage_id → [conversations]} map in one query if needed
    conv_by_stage: dict[str, list[ConversationResponse]] = {}
    if include_conversations:
        conv_rows = await repo.list_conversations_for_job(job_id)
        for r in conv_rows:
            sid = r["stage_id"]
            conv_by_stage.setdefault(sid, []).append(_to_conversation(r))

    first = stage_rows[0]
    stages = [
        StageDetail(
            **_to_stage(r, include_result=include_result).model_dump(),
            conversations=conv_by_stage.get(r["stage_id"], []),
        )
        for r in stage_rows
    ]

    return JobStagesResponse(
        job_id=job_id,
        job_type=first["job_type"],
        job_status=first["job_status"],
        stages=stages,
    )


# ── Conversations ──────────────────────────────────────────────────────────────

@router.get(
    "/conversations",
    response_model=ConversationListResponse,
    summary="List LLM correction rounds",
    description=(
        "Paginated list of stage_conversations rows — one row per LLM call.\n\n"
        "Records are ordered by job → stage → page → round so the correction history "
        "for any extraction reads naturally top-to-bottom.\n\n"
        "**Filters** — all optional, combinable:\n"
        "- `job_id` — all rounds for a specific job\n"
        "- `stage_id` — all rounds for a specific stage\n"
        "- `stage` — rounds from a specific stage type (VISION, STRUCTURE, …)\n"
        "- `success` — true = rounds that produced valid output; false = failed rounds\n"
        "- `page_num` — image page number (vision only)"
    ),
)
async def list_conversations(
    request: Request,
    _: str = Depends(require_internal_auth),
    job_id: Optional[str] = Query(None, description="Filter by job UUID"),
    stage_id: Optional[str] = Query(None, description="Filter by stage UUID"),
    stage: Optional[str] = Query(None, description="Filter by stage name (VISION, STRUCTURE, …)"),
    success: Optional[bool] = Query(None, description="true = successful rounds only; false = failed rounds only"),
    page_num: Optional[int] = Query(None, description="Filter by image page number (vision only)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> ConversationListResponse:
    repo = InspectionRepository(get_pool(request))
    rows, total = await repo.list_conversations(
        job_id=job_id, stage_id=stage_id, stage_name=stage,
        success=success, page_num=page_num, page=page, page_size=page_size,
    )
    return ConversationListResponse(
        conversations=[_to_conversation(r) for r in rows],
        total=total, page=page, page_size=page_size,
    )


@router.get(
    "/stages/{stage_id}/conversations",
    response_model=ConversationListResponse,
    summary="Get all LLM rounds for a stage",
    description=(
        "All correction rounds for one stage, ordered by page then round number.\n\n"
        "Useful for post-mortem analysis of why a stage needed multiple attempts, "
        "or for auditing the model's reasoning on a specific document."
    ),
)
async def list_stage_conversations(
    stage_id: str,
    request: Request,
    _: str = Depends(require_internal_auth),
) -> ConversationListResponse:
    repo = InspectionRepository(get_pool(request))
    rows = await repo.list_conversations_for_stage(stage_id)
    return ConversationListResponse(
        conversations=[_to_conversation(r) for r in rows],
        total=len(rows), page=1, page_size=len(rows) or 1,
    )


# ── Webhook deliveries ─────────────────────────────────────────────────────────

@router.get(
    "/webhooks",
    response_model=WebhookDeliveryListResponse,
    summary="List webhook delivery attempts",
    description=(
        "Paginated list of all webhook delivery attempts, newest first.\n\n"
        "Each record includes the parent job context (type, status).\n\n"
        "**Filters** — all optional, combinable:\n"
        "- `job_id` — deliveries for a specific job\n"
        "- `event` — filter by event string; supports prefix matching "
        "(e.g. `extraction.` matches all extraction events)\n"
        "- `delivered` — true = successfully delivered; false = failed attempts\n\n"
        "Payload JSONB is excluded from list results — fetch a single record to see the full payload."
    ),
)
async def list_webhooks(
    request: Request,
    _: str = Depends(require_internal_auth),
    job_id: Optional[str] = Query(None, description="Filter by job UUID"),
    event: Optional[str] = Query(
        None,
        description="Filter by event string or prefix (e.g. 'extraction.vision.success' or 'extraction.')",
    ),
    delivered: Optional[bool] = Query(None, description="true = delivered; false = failed attempts"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> WebhookDeliveryListResponse:
    repo = InspectionRepository(get_pool(request))
    rows, total = await repo.list_webhooks(
        job_id=job_id, event=event, delivered=delivered, page=page, page_size=page_size,
    )
    return WebhookDeliveryListResponse(
        deliveries=[_to_webhook(r) for r in rows],
        total=total, page=page, page_size=page_size,
    )


@router.get(
    "/webhooks/{delivery_id}",
    response_model=WebhookDeliveryResponse,
    summary="Get a single webhook delivery",
    description=(
        "Fetch one delivery record by ID — includes the full payload JSONB "
        "that was (or was attempted to be) sent to the caller."
    ),
)
async def get_webhook(
    delivery_id: str,
    request: Request,
    _: str = Depends(require_internal_auth),
) -> WebhookDeliveryResponse:
    repo = InspectionRepository(get_pool(request))
    row = await repo.get_webhook(delivery_id)
    if not row:
        raise NotFoundError(f"Webhook delivery {delivery_id} not found")
    return _to_webhook(row)

"""
Inspection endpoint response schemas.

These schemas represent denormalised views — each record carries joined context
from related tables so the caller never has to make multiple requests to
understand a stage, conversation, or webhook delivery.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ── Shared pagination wrapper ─────────────────────────────────────────────────

class Page(BaseModel):
    total: int
    page: int
    page_size: int


# ── Processing stages ──────────────────────────────────────────────────────────

class StageResponse(BaseModel):
    """One processing_stages row, joined with its parent processing_jobs row."""

    stage_id: str
    job_id: str
    stage: str = Field(description="Stage name: VISION, STRUCTURE, …")
    status: str = Field(description="SUCCESS or FAILED")
    error: Optional[str] = Field(None, description="Failure reason (null on success)")
    usage: Optional[Dict[str, Any]] = Field(
        None,
        description="Token counts, latency, cost — null when the stage failed before any LLM call",
    )
    started_at: Optional[datetime]
    completed_at: datetime
    created_at: datetime
    # result is omitted by default — opt in via include_result=true
    result: Optional[Dict[str, Any]] = Field(
        None,
        description="Raw stage output JSONB — only populated when include_result=true",
    )
    # Joined from processing_jobs
    job_type: str
    job_status: str
    job_priority: int


class StageListResponse(Page):
    stages: List[StageResponse]


# ── Stage conversations ────────────────────────────────────────────────────────

class ConversationResponse(BaseModel):
    """
    One stage_conversations row, joined with its stage and job.

    Each row represents one LLM call (correction round).  round=1 is the first
    attempt; round>1 means the model needed correction.
    """

    conversation_id: str
    stage_id: str
    job_id: str
    round: int = Field(description="Correction round number (1 = first attempt)")
    page: Optional[int] = Field(None, description="Image page number (vision only; null for structure)")
    success: bool = Field(description="True if this round produced valid output")
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="{ correction_sent, raw_response, errors } — always included",
    )
    created_at: datetime
    # Joined from processing_stages
    stage_name: str = Field(description="Parent stage name: VISION, STRUCTURE, …")
    stage_status: str = Field(description="Parent stage status: SUCCESS or FAILED")
    # Joined from processing_jobs
    job_type: str


class ConversationListResponse(Page):
    conversations: List[ConversationResponse]


# ── Stage detail with nested conversations ─────────────────────────────────────

class StageDetail(StageResponse):
    """Stage response optionally extended with its conversation rows."""
    conversations: List[ConversationResponse] = Field(
        default_factory=list,
        description="All LLM correction rounds for this stage — populated when include_conversations=true",
    )


# ── Job stages overview ────────────────────────────────────────────────────────

class JobStagesResponse(BaseModel):
    """All stages for one job, newest first, optionally with conversations nested."""
    job_id: str
    job_type: str
    job_status: str
    stages: List[StageDetail]


# ── Webhook deliveries ─────────────────────────────────────────────────────────

class WebhookDeliveryResponse(BaseModel):
    """
    One webhook_deliveries row, joined with its parent job.

    The event field holds the full dot-notation event string (e.g. extraction.vision.success).
    """

    delivery_id: str
    job_id: str
    event: str = Field(description="Dot-notation event string, e.g. extraction.vision.success")
    callback_url: str
    response_status: Optional[int] = Field(None, description="HTTP status returned by the caller")
    response_body: Optional[str] = Field(None, description="First 2 000 chars of the response body")
    attempt_count: int
    delivered: bool
    created_at: datetime
    # payload is omitted from list responses — opt in via include_payload=true
    payload: Optional[Dict[str, Any]] = Field(
        None,
        description="Full JSON payload sent to the caller — only populated when include_payload=true or on single-record fetch",
    )
    # Joined from processing_jobs
    job_type: str
    job_status: str


class WebhookDeliveryListResponse(Page):
    deliveries: List[WebhookDeliveryResponse]

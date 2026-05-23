"""
Processing endpoint schemas.

One request schema per pipeline type — each endpoint is fully typed so bad
input is rejected at the HTTP boundary (422) rather than surfacing inside a
worker at runtime.

The internal job model (job_type + input JSONB) stays generic — adding a new
pipeline only requires a new request schema + a new route, no DB migrations.

JobStatusResponse — response from GET /v1/jobs/{job_id}
ProcessResponse   — 202 body returned by all submission endpoints
JobListResponse   — paginated response from GET /v1/jobs
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ── Submission request schemas (one per pipeline type) ─────────────────────────

class ExtractionRequest(BaseModel):
    """
    POST /v1/jobs/extraction — OCR + structured field extraction.

    The caller (NestJS) passes raw S3 object keys. Docai fetches the images
    via the NestJS internal stream endpoint (authenticated with X-Internal-Secret)
    rather than using presigned URLs that break across Docker host boundaries.
    """
    case_number: str = Field(
        ...,
        description="Human-readable case reference — used for logging and image organisation.",
    )
    image_keys: list[str] = Field(
        ...,
        min_length=1,
        description="S3 object keys for the document images, in page order.",
    )
    webhook_url: str = Field(
        ...,
        description="Caller's webhook URL — docai POSTs stage callbacks here.",
    )


# ── Future pipeline schemas slot in here:
#
# class FraudCheckRequest(BaseModel):
#     """POST /v1/jobs/fraud-check"""
#     document_id: str
#     extraction_job_id: str
#     webhook_url: str
#
# class MatchVerificationRequest(BaseModel):
#     """POST /v1/jobs/match-verification"""
#     claim_id: str
#     candidate_ids: list[str] = Field(..., min_length=1)
#     webhook_url: str


# ── Shared response schemas ────────────────────────────────────────────────────

class ProcessResponse(BaseModel):
    """202 response body — returned by all job submission endpoints."""
    job_id: str = Field(..., description="UUID of the queued pipeline job.")


class JobStatusResponse(BaseModel):
    """Single-job status — returned by GET /v1/jobs/{job_id}."""
    job_id: str
    job_type: str
    status: str
    current_stage: Optional[str]
    webhook_url: str
    input: Optional[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime


class JobListResponse(BaseModel):
    """Paginated job list — returned by GET /v1/jobs."""
    jobs: List[JobStatusResponse]
    total: int
    page: int
    page_size: int

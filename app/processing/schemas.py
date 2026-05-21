"""
Processing endpoint schemas.

ProcessRequest  — payload NestJS sends when submitting an extraction job.
JobStatusResponse — response from GET /v1/jobs/{job_id}.
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class ProcessRequest(BaseModel):
    case_number: str = Field(..., description="Human-readable case reference — used for logging and image organisation")
    image_urls: list[str] = Field(
        ...,
        min_length=1,
        description=(
            "Pre-signed MinIO URLs for the document images. "
            "The caller generates these before calling docai — the signature is "
            "embedded in the URL so docai downloads via plain HTTP with no credentials."
        ),
    )
    webhook_url: str = Field(
        ...,
        description="Caller's webhook URL — docai POSTs stage callbacks here.",
    )


class JobStatusResponse(BaseModel):
    job_id: str
    case_number: str
    status: str
    current_stage: Optional[str]
    created_at: datetime
    updated_at: datetime


class ProcessResponse(BaseModel):
    job_id: str = Field(..., description="UUID of the queued extraction job")


class JobListResponse(BaseModel):
    jobs: List[JobStatusResponse]
    total: int
    page: int
    page_size: int

"""
Processing endpoint schemas.

ProcessRequest  — payload NestJS sends when submitting an extraction job.
JobStatusResponse — response from GET /v1/jobs/{job_id}.
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class ProcessRequest(BaseModel):
    external_case_id: str = Field(..., description="NestJS DocumentCase.id")
    external_document_id: str = Field(..., description="NestJS Document.id")
    external_extraction_id: str = Field(..., description="NestJS AIExtraction.id")
    external_user_id: str = Field(..., description="NestJS User.id (case owner)")
    case_type: str = Field(..., description="'LOST' or 'FOUND'")
    case_number: str = Field(..., description="Human-readable case reference")
    image_urls: list[str] = Field(
        ...,
        min_length=1,
        description=(
            "Pre-signed MinIO URLs for the document images. "
            "NestJS generates these before calling docai — the signature is "
            "embedded in the URL so docai downloads via plain HTTP with no credentials."
        ),
    )
    webhook_url: str = Field(
        ...,
        description="NestJS URL to POST stage callbacks to (overrides env default for multi-tenant)",
    )


class JobStatusResponse(BaseModel):
    job_id: str
    external_case_id: str
    external_document_id: str
    external_extraction_id: str
    external_user_id: str
    case_type: str
    case_number: str
    status: str
    current_stage: Optional[str]
    error_message: Optional[str]
    created_at: datetime
    updated_at: datetime


class JobListResponse(BaseModel):
    jobs: List[JobStatusResponse]
    total: int
    page: int
    page_size: int

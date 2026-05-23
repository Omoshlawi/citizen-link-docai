"""
Typed domain models for inspection query results.

Each class is a typed view of a DB row (with joins) returned by InspectionRepository.
All JSONB columns are decoded at construction time in from_record() — callers always
receive plain Python dicts, never raw strings.

StageRecord       — one processing_stages row joined with processing_jobs
ConversationRecord — one stage_conversations row joined with stages + jobs
WebhookRecord     — one webhook_deliveries row joined with processing_jobs
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import asyncpg


def _decode_jsonb(v: Any) -> Optional[Dict]:
    """Safely decode a JSONB column value from asyncpg.

    asyncpg returns JSONB as a dict in most versions; as a string in some older
    configurations.  None is passed through as-is.
    """
    if v is None:
        return None
    if isinstance(v, str):
        return json.loads(v)
    return dict(v)


@dataclass
class StageRecord:
    """
    Typed view of a processing_stages row joined with processing_jobs.

    result is None when the repository was queried with include_result=False
    (the default for list endpoints to keep payloads small).
    """
    stage_id: str
    job_id: str
    stage: str
    status: str
    error: Optional[str]
    usage: Optional[Dict[str, Any]]
    started_at: Optional[datetime]
    completed_at: datetime
    created_at: datetime
    result: Optional[Dict[str, Any]]
    job_type: str
    job_status: str

    @classmethod
    def from_record(cls, row: asyncpg.Record) -> StageRecord:
        return cls(
            stage_id=str(row["stage_id"]),
            job_id=str(row["job_id"]),
            stage=row["stage"],
            status=row["status"],
            error=row["error"],
            usage=_decode_jsonb(row["usage"]),
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            created_at=row["created_at"],
            result=_decode_jsonb(row["result"]),
            job_type=row["job_type"],
            job_status=row["job_status"],
        )


@dataclass
class ConversationRecord:
    """
    Typed view of a stage_conversations row joined with processing_stages and processing_jobs.

    One row per LLM message turn.  Ordering by round then created_at reconstructs
    the full correction thread for a stage.
    """
    conversation_id: str
    stage_id: str
    job_id: str
    round: int
    page: Optional[int]
    role: str
    content: str
    success: Optional[bool]
    metadata: Optional[Dict[str, Any]]
    created_at: datetime
    stage_name: str
    stage_status: str
    job_type: str

    @classmethod
    def from_record(cls, row: asyncpg.Record) -> ConversationRecord:
        return cls(
            conversation_id=str(row["conversation_id"]),
            stage_id=str(row["stage_id"]),
            job_id=str(row["job_id"]),
            round=row["round"],
            page=row["page"],
            role=row["role"],
            content=row["content"],
            success=row["success"],
            metadata=_decode_jsonb(row["metadata"]),
            created_at=row["created_at"],
            stage_name=row["stage_name"],
            stage_status=row["stage_status"],
            job_type=row["job_type"],
        )


@dataclass
class WebhookRecord:
    """
    Typed view of a webhook_deliveries row joined with processing_jobs.

    payload is None in list responses (excluded for size); always populated
    on single-record fetch via get_webhook().
    """
    delivery_id: str
    job_id: str
    event: str
    callback_url: str
    response_status: Optional[int]
    response_body: Optional[str]
    attempt_count: int
    delivered: bool
    created_at: datetime
    payload: Optional[Dict[str, Any]]
    job_type: str
    job_status: str

    @classmethod
    def from_record(cls, row: asyncpg.Record) -> WebhookRecord:
        return cls(
            delivery_id=str(row["delivery_id"]),
            job_id=str(row["job_id"]),
            event=row["event"],
            callback_url=row["callback_url"],
            response_status=row["response_status"],
            response_body=row["response_body"],
            attempt_count=row["attempt_count"],
            delivered=row["delivered"],
            created_at=row["created_at"],
            payload=_decode_jsonb(row["payload"]),
            job_type=row["job_type"],
            job_status=row["job_status"],
        )

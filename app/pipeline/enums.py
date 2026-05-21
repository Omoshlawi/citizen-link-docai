"""
Shared enums for the docai pipeline.

Using Python Enum + Pydantic Literal types ensures that any typo in a stage
or status value is caught at the boundary — either when deserialising incoming
ARQ job arguments or when building outgoing webhook payloads.
"""

from enum import Enum


class WebhookStage(str, Enum):
    VISION = "VISION"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class WebhookStatus(str, Enum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class JobStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class PipelineStage(str, Enum):
    VISION = "VISION"
    STRUCTURE = "STRUCTURE"

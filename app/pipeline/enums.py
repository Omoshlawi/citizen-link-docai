"""
Shared enums for the docai pipeline.

Using Python Enum + Pydantic Literal types ensures that any typo in a stage
or status value is caught at the boundary — either when deserialising incoming
ARQ job arguments or when building outgoing webhook payloads.
"""

from enum import Enum


class DocaiEvent(str, Enum):
    """
    All webhook events emitted by docai — single source of truth.

    Format: {pipeline-namespace}.{stage}.{success|failed}
    Terminal success: {pipeline-namespace}.success  (combined nested payload)
    Terminal failure: {pipeline-namespace}.failed   (flat rollup, fires alongside stage event)

    Every event carries the actual output of that stage — consumers pick what
    they care about.  The terminal {namespace}.success nests all stage outputs.

    New pipelines add their own namespace block here AND in the NestJS mirror
    (src/docai/docai-webhook.schema.ts :: DocaiEvent).
    """
    # ── EXTRACTION pipeline ────────────────────────────────────────────────────
    EXTRACTION_VISION_SUCCESS    = "extraction.vision.success"     # raw VisionAgent output
    EXTRACTION_STRUCTURE_SUCCESS = "extraction.structure.success"  # raw StructureAgent output
    EXTRACTION_SUCCESS           = "extraction.success"            # nested { vision, structure } — terminal
    EXTRACTION_VISION_FAILED     = "extraction.vision.failed"      # terminal, stage-specific
    EXTRACTION_STRUCTURE_FAILED  = "extraction.structure.failed"   # terminal, stage-specific
    EXTRACTION_FAILED            = "extraction.failed"             # terminal, flat rollup
    # ── Future pipelines ───────────────────────────────────────────────────────
    # FRAUD_DETECTION_CHECK_SUCCESS = "fraud-detection.check.success"
    # FRAUD_DETECTION_SUCCESS       = "fraud-detection.success"
    # FRAUD_DETECTION_CHECK_FAILED  = "fraud-detection.check.failed"
    # FRAUD_DETECTION_FAILED        = "fraud-detection.failed"


class JobStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class JobType(str, Enum):
    EXTRACTION = "EXTRACTION"


class StageStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"

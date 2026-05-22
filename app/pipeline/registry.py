"""
Pipeline registry — maps job_type → ordered stages + agent factories.

Adding a new pipeline (e.g. fraud detection) means:
  1. Register agent(s) in _AGENT_FACTORIES
  2. Add a PipelineConfig entry in PIPELINES
  3. No database migrations needed

PipelineConfig fields
---------------------
stages           — ordered list of stage names (e.g. ["VISION", "STRUCTURE"])
progress_stages  — subset of stages that fire a mid-pipeline progress webhook
                   (non-terminal, no result payload).  Terminal COMPLETED is
                   always fired after the last stage by run_stage.
build_result     — callable: {stage_name: result_dict} → COMPLETED webhook payload.
                   Called only after the final stage succeeds.
post_stage_gate  — callable per stage: result_dict → error_message | None.
                   Runs after a stage stores its SUCCESS result but before the
                   next stage is enqueued.  Returning a non-None string fails the
                   job immediately with that message — no retry, no further LLM
                   spend.  Only checked when a next stage exists.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from app.config import Settings


# ── Agent protocol (structural) ────────────────────────────────────────────────

class AgentProtocol:
    """
    Interface every pipeline agent must satisfy.

    run(job_input, previous_results) -> (result_model, usage_entries, conversation_entries)
    """

    async def run(
        self,
        job_input: dict,
        previous_results: dict[str, dict],
    ) -> tuple[Any, list[Any], list[Any]]:
        raise NotImplementedError


# ── Pipeline config ────────────────────────────────────────────────────────────

@dataclass
class PipelineConfig:
    """Registry entry for one pipeline type."""

    stages: list[str]
    """Ordered stage names — run_stage dispatches them left-to-right."""

    progress_stages: set[str] = field(default_factory=set)
    """
    Stages that fire a progress webhook (mid-pipeline, no result payload).
    Terminal COMPLETED is always fired after the final stage by run_stage.
    """

    build_result: Callable[[dict[str, dict]], dict] = field(
        default_factory=lambda: (lambda _: {})
    )
    """
    Build the COMPLETED webhook result dict from all stage results.
    Called only after the final stage succeeds.
    """

    post_stage_gate: dict[str, Callable[[dict], Optional[str]]] = field(
        default_factory=dict
    )
    """
    Per-stage quality gates.  Each entry is:
        stage_name → callable(result_dict) → error_message | None

    The gate runs after the stage stores its SUCCESS result, before the next
    stage is enqueued.  A non-None return value fails the job immediately with
    that message — no retry, no further LLM spend.  Gates are only checked when
    a subsequent stage exists; the final stage is never gated (COMPLETED fires).
    """


# ── EXTRACTION pipeline — gate ─────────────────────────────────────────────────

# Tunable thresholds — adjust based on operational experience with your vision model
_MIN_OCR_CONFIDENCE = 0.40   # below this, image is too degraded to send to structure
_MIN_TEXT_LENGTH    = 25     # below this, not enough text for meaningful field extraction


def _gate_vision_quality(result: dict) -> Optional[str]:
    """
    Rule-based quality check on VisionAgent output.

    Two fast checks — no LLM call, no token cost:

    1. averageConfidence too low → image is physically unreadable (blur, damage,
       bad lighting).  The structure agent would be extracting from noise.

    2. fullText too short → the image contains almost no text.  This catches
       blank uploads, screenshots of solid colours, landscape photos, or anything
       that isn't a document.  It won't catch a well-photographed non-ID image
       that happens to have text (e.g. a receipt) — that's the structure agent's
       job to classify as UNKNOWN.

    Returns None (pass) or a user-friendly error string (fail).
    """
    avg_confidence = result.get("averageConfidence", 1.0)
    full_text      = result.get("fullText", "")

    if avg_confidence < _MIN_OCR_CONFIDENCE:
        return (
            f"Document image quality is too poor to process "
            f"(OCR confidence {avg_confidence:.0%} — minimum {_MIN_OCR_CONFIDENCE:.0%}). "
            "Please upload a clearer, well-lit image of the document."
        )

    if len(full_text.strip()) < _MIN_TEXT_LENGTH:
        return (
            "No meaningful text was detected in the uploaded image. "
            "Please ensure you have uploaded a valid identity document "
            "and that the image is not blurred or obscured."
        )

    return None


# ── EXTRACTION pipeline — result builder ──────────────────────────────────────

def _build_extraction_result(stage_results: dict[str, dict]) -> dict:
    """Assemble the COMPLETED webhook payload for EXTRACTION jobs."""
    vision    = stage_results.get("VISION",    {})
    structure = stage_results.get("STRUCTURE", {})
    return {
        "fields":              structure,
        "ocrConfidence":       vision.get("averageConfidence"),
        "extractionConfidence": structure.get("quality", {}).get("extractionConfidence"),
    }


# ── Pipeline registry ──────────────────────────────────────────────────────────

PIPELINES: dict[str, PipelineConfig] = {
    "EXTRACTION": PipelineConfig(
        stages          = ["VISION", "STRUCTURE"],
        progress_stages = {"VISION"},
        build_result    = _build_extraction_result,
        post_stage_gate = {"VISION": _gate_vision_quality},
    ),
    # Future pipelines slot in here without any schema or task changes:
    # "FRAUD_DETECTION":    PipelineConfig(stages=["FRAUD"],  build_result=...),
    # "MATCH_VERIFICATION": PipelineConfig(stages=["MATCH"],  build_result=...),
}


# ── Agent factories ────────────────────────────────────────────────────────────
# Lazy imports keep startup fast and avoid circular imports — the registry is
# imported by tasks.py, which is imported by the agents.

def _make_vision_agent(settings: Settings) -> Any:
    from app.agents.vision_agent import VisionAgent
    return VisionAgent(settings)


def _make_structure_agent(settings: Settings) -> Any:
    from app.agents.structure_agent import StructureAgent
    return StructureAgent(settings)


_AGENT_FACTORIES: dict[tuple[str, str], Callable[[Settings], Any]] = {
    ("EXTRACTION", "VISION"):    _make_vision_agent,
    ("EXTRACTION", "STRUCTURE"): _make_structure_agent,
}


# ── Public helpers ─────────────────────────────────────────────────────────────

def get_pipeline(job_type: str) -> PipelineConfig:
    """Return the PipelineConfig for a job type. Raises ValueError if unknown."""
    config = PIPELINES.get(job_type)
    if config is None:
        raise ValueError(
            f"Unknown pipeline job_type: {job_type!r}. "
            f"Registered types: {list(PIPELINES)}"
        )
    return config


def get_agent(job_type: str, stage: str, settings: Settings) -> Any:
    """
    Instantiate and return the agent for (job_type, stage).
    Raises ValueError if no agent is registered for this combination.
    """
    factory = _AGENT_FACTORIES.get((job_type, stage))
    if factory is None:
        raise ValueError(
            f"No agent registered for job_type={job_type!r}, stage={stage!r}. "
            f"Registered: {list(_AGENT_FACTORIES)}"
        )
    return factory(settings)

"""
Pipeline registry — maps job_type → ordered stages + agent factories.

Adding a new pipeline (e.g. fraud detection) means:
  1. Register agent(s) in _AGENT_FACTORIES
  2. Add a PipelineConfig entry in PIPELINES
  3. No database migrations needed

PipelineConfig
--------------
stages          — ordered list of stage names (e.g. ["VISION", "STRUCTURE"])
progress_stages — subset of stages that fire a mid-pipeline progress webhook
                  (non-terminal, no result payload).  Terminal COMPLETED is
                  always fired after the last stage by run_stage.
build_result    — callable that takes {stage_name: result_dict} for all completed
                  stages and returns the result dict for the COMPLETED webhook.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from app.config import Settings


# ── Agent protocol (structural) ────────────────────────────────────────────────

class AgentProtocol:
    """
    Interface every pipeline agent must satisfy.

    run(job_input, previous_results) -> (result, usage_logs, conversation)

      job_input        — the JSONB dict stored on processing_jobs.input
      previous_results — {stage_name: result_dict} for all stages that already
                         ran successfully before this one
    """

    async def run(
        self,
        job_input: dict,
        previous_results: dict[str, dict],
    ) -> tuple[dict, list[dict], list[dict]]:
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


# ── EXTRACTION pipeline helpers ────────────────────────────────────────────────

def _build_extraction_result(stage_results: dict[str, dict]) -> dict:
    """Assemble the COMPLETED webhook payload for EXTRACTION jobs."""
    vision = stage_results.get("VISION", {})
    structure = stage_results.get("STRUCTURE", {})
    return {
        "fields": structure,
        "ocrConfidence": vision.get("averageConfidence"),
        "extractionConfidence": structure.get("quality", {}).get("extractionConfidence"),
    }


# ── Pipeline registry ──────────────────────────────────────────────────────────

PIPELINES: dict[str, PipelineConfig] = {
    "EXTRACTION": PipelineConfig(
        stages=["VISION", "STRUCTURE"],
        progress_stages={"VISION"},   # fires a mid-pipeline webhook after OCR
        build_result=_build_extraction_result,
    ),
    # Future pipelines slot in here without any schema or task changes:
    # "FRAUD_DETECTION": PipelineConfig(stages=["FRAUD"], build_result=...),
    # "MATCH_VERIFICATION": PipelineConfig(stages=["MATCH"], build_result=...),
}


# ── Agent factories ────────────────────────────────────────────────────────────
# Lazy imports inside the factory functions keep startup fast and avoid
# circular imports — the registry is imported by tasks.py, which is imported
# by the agents.

def _make_vision_agent(settings: Settings) -> Any:
    from app.agents.vision_agent import VisionAgent
    return VisionAgent(settings)


def _make_structure_agent(settings: Settings) -> Any:
    from app.agents.structure_agent import StructureAgent
    return StructureAgent(settings)


_AGENT_FACTORIES: dict[tuple[str, str], Callable[[Settings], Any]] = {
    ("EXTRACTION", "VISION"):     _make_vision_agent,
    ("EXTRACTION", "STRUCTURE"):  _make_structure_agent,
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

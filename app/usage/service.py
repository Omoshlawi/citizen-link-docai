"""
UsageService — convenience wrapper for logging a batch of usage entries.

The pipeline stages return a list of usage dicts from the agents.
This service writes them all to ai_usage_logs in one go.

Cost estimation is intentionally rough — it uses public pricing as a guide.
Ollama local models cost $0 (no API call), but latency is still logged.
"""

from typing import Optional

import asyncpg
import structlog

from app.usage.repository import UsageRepository

log = structlog.get_logger(__name__)

# Approximate cost per 1M tokens (USD) — update as pricing changes
_COST_PER_1M = {
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "text-embedding-3-small": {"input": 0.02, "output": 0.00},
    "text-embedding-3-large": {"input": 0.13, "output": 0.00},
}


def _estimate_cost(
    model: str,
    input_tokens: Optional[int],
    output_tokens: Optional[int],
) -> Optional[float]:
    """Rough cost estimate in USD. Returns None for local models (Ollama)."""
    pricing = _COST_PER_1M.get(model)
    if not pricing:
        return None  # local model — no cost
    input_cost = (input_tokens or 0) / 1_000_000 * pricing["input"]
    output_cost = (output_tokens or 0) / 1_000_000 * pricing["output"]
    return round(input_cost + output_cost, 8)


class UsageService:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._repo = UsageRepository(pool)

    async def log_batch(self, job_id: str, usage_entries: list[dict]) -> None:
        """
        Write a list of usage entries to ai_usage_logs.

        Each entry is a dict with keys:
          stage, model, provider, input_tokens, output_tokens,
          latency_ms, success, error_message (optional)
        """
        for entry in usage_entries:
            try:
                cost = _estimate_cost(
                    entry.get("model", ""),
                    entry.get("input_tokens"),
                    entry.get("output_tokens"),
                )
                await self._repo.log_usage(
                    job_id=job_id,
                    stage=entry.get("stage"),
                    model=entry.get("model", "unknown"),
                    provider=entry.get("provider", "unknown"),
                    input_tokens=entry.get("input_tokens"),
                    output_tokens=entry.get("output_tokens"),
                    estimated_cost_usd=cost,
                    latency_ms=int(entry.get("latency_ms", 0)),
                    success=entry.get("success", True),
                    error_message=entry.get("error_message"),
                )
            except Exception as exc:
                # Never let usage logging break the pipeline
                log.error(
                    "usage_log_failed",
                    job_id=job_id,
                    error=str(exc),
                    entry=entry,
                )

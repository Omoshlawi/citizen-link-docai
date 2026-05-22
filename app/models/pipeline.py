"""
Pipeline internal models.

ConversationEntry — one LLM correction round (vision or structure stage).
UsageEntry        — one LLM call's token/latency metrics.
CallRecord        — per-call row inside a UsageSummary.
UsageSummary      — aggregated usage for one pipeline stage; replaces _build_usage().
JobRecord         — typed view of a processing_jobs DB row.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional

import asyncpg

# Approximate cost per 1M tokens (USD) — update as pricing changes
_COST_PER_1M: dict[str, dict[str, float]] = {
    "gpt-4o":                  {"input": 2.50,  "output": 10.00},
    "gpt-4o-mini":             {"input": 0.15,  "output": 0.60},
    "text-embedding-3-small":  {"input": 0.02,  "output": 0.00},
    "text-embedding-3-large":  {"input": 0.13,  "output": 0.00},
}


@dataclass
class ConversationEntry:
    """
    One message turn in an agent's LLM conversation.

    Each turn maps directly to one stage_conversations row:
      role    — system | user | assistant
      content — the message text (prompt text or LLM response)
      page    — Vision only; None for Structure
      success — set only on assistant rows (True = valid response)
      metadata:
        user rows (Vision)      : {"url": "<signed-url>", "mime_type": "image/jpeg"}
        assistant rows (failed) : {"errors": ["...", ...]}
        all other rows          : None

    Round 1 always contains: system + user + assistant turns.
    Rounds 2+ contain only:  user(correction) + assistant turns.
    Concatenating all turns in round/created_at order reconstructs the full
    conversation thread with zero duplication.
    """
    round: int
    role: str                           # system | user | assistant
    content: str
    page: Optional[int] = None          # Vision only
    success: Optional[bool] = None      # assistant rows only
    metadata: Optional[dict] = None     # see docstring above


@dataclass
class UsageEntry:
    """Raw per-call metrics emitted by an agent after each LLM call."""
    stage: str
    model: str
    provider: str
    input_tokens: Optional[int]
    output_tokens: Optional[int]
    latency_ms: float


@dataclass
class CallRecord:
    """One call's metrics inside a UsageSummary."""
    call: int
    input_tokens: int
    output_tokens: int
    latency_ms: float

    def to_dict(self) -> dict:
        return {
            "call": self.call,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "latency_ms": self.latency_ms,
        }


@dataclass
class UsageSummary:
    """
    Aggregated token usage + cost for one pipeline stage.

    Replaces the _build_usage() free function in tasks.py.
    """
    model: str
    provider: str
    calls: list[CallRecord]
    total_input_tokens: int
    total_output_tokens: int
    total_latency_ms: float
    estimated_cost_usd: Optional[float]

    @classmethod
    def from_entries(cls, entries: list[UsageEntry]) -> UsageSummary:
        """Aggregate a list of per-call UsageEntry objects into a single summary."""
        if not entries:
            return cls(
                model="unknown",
                provider="unknown",
                calls=[],
                total_input_tokens=0,
                total_output_tokens=0,
                total_latency_ms=0.0,
                estimated_cost_usd=None,
            )

        model = entries[0].model
        provider = entries[0].provider
        calls: list[CallRecord] = []
        total_input = 0
        total_output = 0
        total_latency = 0.0

        for i, entry in enumerate(entries, start=1):
            inp = entry.input_tokens or 0
            out = entry.output_tokens or 0
            lat = entry.latency_ms or 0.0
            total_input += inp
            total_output += out
            total_latency += lat
            calls.append(CallRecord(call=i, input_tokens=inp, output_tokens=out, latency_ms=lat))

        pricing = _COST_PER_1M.get(model)
        cost: Optional[float] = (
            round(
                total_input / 1_000_000 * pricing["input"]
                + total_output / 1_000_000 * pricing["output"],
                8,
            )
            if pricing
            else None
        )

        return cls(
            model=model,
            provider=provider,
            calls=calls,
            total_input_tokens=total_input,
            total_output_tokens=total_output,
            total_latency_ms=round(total_latency, 2),
            estimated_cost_usd=cost,
        )

    def to_dict(self) -> dict:
        """Serialise for JSONB storage in processing_stages.usage."""
        return {
            "model": self.model,
            "provider": self.provider,
            "calls": [c.to_dict() for c in self.calls],
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_latency_ms": self.total_latency_ms,
            "estimated_cost_usd": self.estimated_cost_usd,
        }


@dataclass
class JobRecord:
    """
    Typed view of a processing_jobs row.

    Eliminates string-key dict access across pipeline tasks.
    Use JobRecord.from_record(row) wherever you'd otherwise do row["job_type"] etc.
    """
    id: str
    job_type: str
    input: dict
    webhook_url: str
    priority: int
    status: str
    current_stage: Optional[str]

    @classmethod
    def from_record(cls, row: asyncpg.Record) -> JobRecord:
        """
        Construct a JobRecord from an asyncpg Record.

        Handles both str (rare) and dict (normal) for the JSONB input column.
        """
        raw_input = row["input"]
        job_input: dict = (
            json.loads(raw_input) if isinstance(raw_input, str) else dict(raw_input)
        )
        return cls(
            id=str(row["id"]),
            job_type=row["job_type"],
            input=job_input,
            webhook_url=row["webhook_url"],
            priority=row["priority"],
            status=row["status"],
            current_stage=row.get("current_stage"),
        )

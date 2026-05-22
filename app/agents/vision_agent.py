"""
VisionAgent — image → VisionOutput.

Two focused jobs per page:
  1. Transcribe ALL text exactly as it appears (verbatim, no interpretation)
  2. Describe meaningful non-text visual elements in plain prose
     (flags, photographs, fingerprints, signatures, stamps, emblems, etc.)

This deliberately unstructured output is designed to feed StructureAgent,
which handles all interpretation and field extraction.  Keeping vision
purely observational removes the main sources of LLM hallucination and
schema correction loops.
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
import time
from typing import Optional

import httpx
import structlog
from openai import AsyncOpenAI

from app.agents.exceptions import AgentExhaustedError
from app.config import Settings
from app.models.pipeline import ConversationEntry, UsageEntry
from app.models.vision import VisionMeta, VisionOutput, VisionPage

log = structlog.get_logger(__name__)


# ── Prompts ────────────────────────────────────────────────────────────────────

VISION_SYSTEM_PROMPT = (
    "You are a document reader. You observe and transcribe. You never interpret or infer. "
    "Output ONLY valid JSON — no markdown, no code fences, no text outside the JSON."
)

VISION_USER_PROMPT = """\
Read this document image and produce two things:

1. TEXT — transcribe every visible character exactly as it appears.
   - Preserve original casing, punctuation, spacing, and line breaks
   - Never correct spelling, expand abbreviations, or fill in obscured text
   - If text is partially illegible, transcribe what is visible and use [...] for the unreadable portion

2. VISUAL ELEMENTS — describe each meaningful non-text element you can see, in plain English.
   Focus only on elements that carry identity or document significance:
   - National symbols: flags, coats of arms, emblems (note what they suggest about the issuing country)
   - Biometric elements: photographs, fingerprints, signatures
   - Security features: stamps, seals, watermarks, holograms, embossed marks
   - Machine-readable features: barcodes, QR codes, MRZ strips (describe as visual, do not decode)
   Ignore purely decorative borders, background patterns, and print artifacts.

CONFIDENCE — your overall reading quality for this page:
  0.90–1.00  clear, fully legible
  0.70–0.89  minor blur, wear, or folds — most text readable
  0.50–0.69  partially obscured or degraded — some text may be missing
  below 0.50 heavily degraded — significant text loss likely

confidence MUST be a decimal in [0.0000, 1.0000] rounded to 4 decimal places.

OUTPUT SCHEMA (return exactly this, one object for the whole image):
{
  "meta": {
    "pageCount": 1,
    "engine": "vision-llm"
  },
  "pages": [
    {
      "pageNumber": 1,
      "confidence": <number>,
      "text": "<all visible text, line breaks as \\n>",
      "visualElements": [
        "<plain English description of element>",
        "<plain English description of element>"
      ]
    }
  ]
}
"""


# ── Validation (operates on raw dict before model construction) ────────────────

def _clean_json(text: str) -> str:
    """Strip markdown code fences if the model wraps output in them."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _validate_vision_output(data: dict) -> list[str]:
    """
    Validate the parsed LLM output before constructing a VisionOutput.
    Returns a list of human-readable error strings for the correction prompt.
    Empty list means valid.
    """
    errors: list[str] = []

    meta = data.get("meta")
    if not isinstance(meta, dict):
        errors.append("meta must be an object")
    elif meta.get("engine") != "vision-llm":
        errors.append('meta.engine must be "vision-llm"')

    pages = data.get("pages")
    if not isinstance(pages, list) or len(pages) == 0:
        errors.append("pages must be a non-empty array")
    else:
        for i, page in enumerate(pages):
            if not isinstance(page.get("text"), str):
                errors.append(f"pages[{i}].text must be a string")
            conf = page.get("confidence")
            if not isinstance(conf, (int, float)) or not (0 <= conf <= 1):
                errors.append(f"pages[{i}].confidence must be a decimal in [0, 1]")
            if not isinstance(page.get("visualElements"), list):
                errors.append(f"pages[{i}].visualElements must be an array")
            else:
                for j, el in enumerate(page["visualElements"]):
                    if not isinstance(el, str) or not el.strip():
                        errors.append(
                            f"pages[{i}].visualElements[{j}] must be a non-empty string"
                        )

    return errors


# ── Derived field computation (operates on VisionOutput in-place) ──────────────

def _compute_derived_fields(output: VisionOutput) -> VisionOutput:
    """
    Compute fullText and averageConfidence from the page models.

    Both are deterministic — never asked from the LLM.
    """
    for page in output.pages:
        page.text = page.text.strip()

    output.fullText = "\n".join(page.text for page in output.pages)

    confidences = [page.confidence for page in output.pages]
    output.averageConfidence = (
        round(sum(confidences) / len(confidences), 4) if confidences else 0.0
    )
    return output


# ── Agent ──────────────────────────────────────────────────────────────────────

class VisionAgent:
    """
    Agentic vision extraction — call → validate → auto-correct, max N rounds.

    Pages are processed in parallel (asyncio.gather) so total latency is
    bounded by the slowest page, not the sum of all pages.  Each page has its
    own isolated multi-turn conversation so corrections on page 2 never affect
    page 1's context.
    """

    def __init__(self, settings: Settings) -> None:
        self._model = settings.vision_ai_model
        self._max_iterations = settings.max_agent_iterations
        self._client = AsyncOpenAI(
            base_url=settings.vision_ai_base_url,
            api_key=settings.vision_ai_api_key,
        )

    async def _download_image(self, url: str) -> tuple[bytes, str]:
        """
        Download a document image from a pre-signed MinIO URL.

        NestJS generates the pre-signed URL before calling docai — the
        signature is embedded in the URL so no credentials are needed here.
        """
        async with httpx.AsyncClient(timeout=30) as http:
            response = await http.get(url)
            response.raise_for_status()
        content_type = response.headers.get("content-type", "image/jpeg").split(";")[0]
        return response.content, content_type

    def _build_initial_messages(self, image_bytes: bytes, mime_type: str) -> list[dict]:
        """Build the opening system + user messages for one page."""
        b64 = base64.b64encode(image_bytes).decode()
        return [
            {"role": "system", "content": VISION_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": VISION_USER_PROMPT},
                    {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{b64}"}},
                ],
            },
        ]

    async def _call_llm(self, messages: list[dict]) -> tuple[str, dict]:
        """
        Send the current conversation to the vision LLM.

        Returns (raw_text, usage_dict) — usage_dict is intentionally untyped
        here since UsageEntry is constructed by the caller, which knows the stage.
        """
        start = time.perf_counter()
        response = await self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            temperature=0.2,
            max_tokens=4096,
        )
        latency_ms = round((time.perf_counter() - start) * 1000, 2)

        raw_text = response.choices[0].message.content or ""
        usage = {
            "input_tokens": response.usage.prompt_tokens if response.usage else None,
            "output_tokens": response.usage.completion_tokens if response.usage else None,
            "latency_ms": latency_ms,
        }
        return raw_text, usage

    async def _extract_page(
        self,
        page_num: int,
        url: str,
        provider: str,
    ) -> tuple[VisionOutput, list[UsageEntry], list[ConversationEntry]]:
        """
        Run vision extraction for a single page.

        Returns (page_output, usage_entries, conversation_entries).
        Raises AgentExhaustedError — carrying this page's conversation trail —
        if all correction attempts are exhausted.
        """
        log.info("vision_agent_downloading_image", page=page_num, url=url[:80])
        image_bytes, mime_type = await self._download_image(url)

        messages = self._build_initial_messages(image_bytes, mime_type)
        correction_text: Optional[str] = None
        usage_entries: list[UsageEntry] = []
        conversation: list[ConversationEntry] = []

        for attempt in range(1, self._max_iterations + 1):
            log.info("vision_agent_calling_llm", page=page_num, attempt=attempt, model=self._model)
            raw_text, raw_usage = await self._call_llm(messages)

            usage_entries.append(UsageEntry(
                stage="VISION",
                model=self._model,
                provider=provider,
                input_tokens=raw_usage["input_tokens"],
                output_tokens=raw_usage["output_tokens"],
                latency_ms=raw_usage["latency_ms"],
            ))

            errors: list[str] = []
            parsed: Optional[dict] = None
            try:
                parsed = json.loads(_clean_json(raw_text))
                errors = _validate_vision_output(parsed)
            except json.JSONDecodeError as e:
                errors = [f"Response is not valid JSON: {e}"]

            conversation.append(ConversationEntry(
                round=attempt,
                page=page_num,
                correction_sent=correction_text,
                raw_response=raw_text,
                errors=errors,
                success=not bool(errors),
            ))

            if not errors and parsed is not None:
                log.info("vision_agent_success", page=page_num, attempt=attempt)
                return VisionOutput.model_validate(parsed), usage_entries, conversation

            if attempt < self._max_iterations:
                correction_text = (
                    "Your output had these validation errors:\n"
                    + "\n".join(f"  - {e}" for e in errors)
                    + "\n\nPlease correct the JSON and return the full valid response."
                )
                messages.append({"role": "assistant", "content": raw_text})
                messages.append({"role": "user", "content": correction_text})
                log.warning("vision_agent_correction", page=page_num, attempt=attempt, errors=errors)
            else:
                log.error("vision_agent_failed_all_attempts", page=page_num, errors=errors)
                raise AgentExhaustedError(
                    f"Vision agent failed after {self._max_iterations} attempts "
                    f"on page {page_num}: {errors}",
                    conversation,
                )

        raise AgentExhaustedError(  # pragma: no cover
            f"Vision agent loop exited without result on page {page_num}", conversation
        )

    async def extract(
        self,
        image_urls: list[str],
    ) -> tuple[VisionOutput, list[UsageEntry], list[ConversationEntry]]:
        """
        Run vision extraction on all provided images in parallel.

        Total latency = slowest page (not sum of pages).

        Returns:
            output       — merged VisionOutput with fullText + averageConfidence
            usage        — UsageEntry list, ordered page → round
            conversation — ConversationEntry list, ordered page → round
        """
        provider = (
            "ollama"
            if "ollama" in self._model.lower() or "11434" in str(self._client.base_url)
            else "openai"
        )

        tasks = [
            self._extract_page(page_num, url, provider)
            for page_num, url in enumerate(image_urls, start=1)
        ]

        try:
            page_outputs: list[tuple[VisionOutput, list[UsageEntry], list[ConversationEntry]]] = (
                await asyncio.gather(*tasks)
            )
        except AgentExhaustedError:
            raise

        page_results, all_usage, all_conversations = zip(*page_outputs)

        merged_usage = [e for page_usage in all_usage for e in page_usage]
        merged_conversations = [e for page_conv in all_conversations for e in page_conv]

        merged = self._merge_pages(list(page_results))
        final = _compute_derived_fields(merged)
        return final, merged_usage, merged_conversations

    def _merge_pages(self, page_results: list[VisionOutput]) -> VisionOutput:
        """
        Merge single-page VisionOutput objects into one multi-page VisionOutput.
        Page numbers are re-assigned sequentially.
        """
        if len(page_results) == 1:
            return page_results[0]

        all_pages: list[VisionPage] = []
        for result in page_results:
            all_pages.extend(result.pages)

        for i, page in enumerate(all_pages, start=1):
            page.pageNumber = i

        return VisionOutput(
            meta=VisionMeta(pageCount=len(all_pages), engine="vision-llm"),
            pages=all_pages,
        )

    async def run(
        self,
        job_input: dict,
        previous_results: dict[str, dict],
    ) -> tuple[VisionOutput, list[UsageEntry], list[ConversationEntry]]:
        """
        Unified agent interface called by the generic run_stage task.

        Extracts image_urls from job_input and delegates to extract().
        previous_results is unused — VISION is always the first stage.
        """
        image_urls: list[str] = job_input.get("image_urls", [])
        if not image_urls:
            raise ValueError(
                "job_input must contain a non-empty 'image_urls' list for VISION stage"
            )
        return await self.extract(image_urls)

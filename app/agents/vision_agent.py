"""
VisionAgent — image → structured OCR output.

Calls the vision LLM, validates the response against the expected schema,
and auto-corrects up to MAX_AGENT_ITERATIONS rounds if validation fails.

Output schema mirrors NestJS VisionExtractionOutputSchema (vision.dto.ts).
"""

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

log = structlog.get_logger(__name__)

# ── Vision output schema (mirrors NestJS VisionExtractionOutputSchema) ─────────

VISION_SYSTEM_PROMPT = (
    "You are a pure OCR engine. You only read. You never interpret. "
    "Output ONLY valid JSON — no markdown, no code fences, no text outside the JSON."
)

VISION_USER_PROMPT = """\
You are a document OCR engine. Extract all text and identify visual regions from the provided image.

RULES:
- Extract ALL visible text exactly as it appears (preserve casing, spacing, line breaks)
- Never correct spelling, infer missing text, or add explanation
- Output ONLY valid JSON, no markdown, no text outside the JSON

BLOCK TYPES:
"text" — readable text region
  text: exact content as seen | tags: []

"photo" — image, logo, stamp, seal, signature, flag, illustration
  text: "photo of [3-10 words describing what you see]" | tags: ["keyword1", "keyword2"]
  Examples:
    text: "photo of a Kenyan national flag"  tags: ["flag","kenyan"]
    text: "photo of a blue government stamp"  tags: ["stamp","government","blue"]
    text: "photo of a handwritten signature"  tags: ["signature","handwritten"]

CONFIDENCE: 0.9000–1.0000 clear | 0.7000–0.8999 minor blur | 0.5000–0.6999 partially obscured | <0.5000 heavily degraded
- confidence MUST be a decimal number in [0.0000, 1.0000] rounded to 4 decimal places (e.g. 0.9234, never 92)

OUTPUT SCHEMA:
{"meta":{"pageCount":number,"languageHints":["string"],"engine":"vision-llm"},"pages":[{"pageNumber":number,"width":number,"height":number,"blocks":[{"id":"b1","type":"text|photo","text":"string","tags":["string"],"confidence":number,"bbox":[x_min,y_min,x_max,y_max]}]}]}
"""


def _clean_json(text: str) -> str:
    """Strip markdown code fences if the model wraps output in them."""
    text = text.strip()
    # Remove ```json ... ``` or ``` ... ```
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _validate_vision_output(data: dict) -> list[str]:
    """
    Validate the parsed vision output and return a list of missing/invalid fields.
    Empty list means the output is valid.
    """
    errors = []

    if not isinstance(data.get("meta"), dict):
        errors.append("meta object is missing or invalid")
    else:
        meta = data["meta"]
        if not isinstance(meta.get("pageCount"), (int, float)):
            errors.append("meta.pageCount must be a number")
        if meta.get("engine") != "vision-llm":
            errors.append('meta.engine must be "vision-llm"')

    pages = data.get("pages")
    if not isinstance(pages, list) or len(pages) == 0:
        errors.append("pages must be a non-empty array")
    else:
        for i, page in enumerate(pages):
            if not isinstance(page.get("blocks"), list):
                errors.append(f"pages[{i}].blocks must be an array")
            else:
                for j, block in enumerate(page["blocks"]):
                    conf = block.get("confidence")
                    if not isinstance(conf, (int, float)) or not (0 <= conf <= 1):
                        errors.append(
                            f"pages[{i}].blocks[{j}].confidence must be a decimal in [0, 1]"
                        )

    return errors


def _compute_derived_fields(data: dict) -> dict:
    """
    Compute fullText and averageConfidence deterministically from block content.
    More reliable than asking the model to compute these.
    """
    pages = data.get("pages", [])

    all_blocks = [b for p in pages for b in p.get("blocks", [])]
    text_blocks = [
        b for b in all_blocks if b.get("type") == "text" and b.get("text", "").strip()
    ]

    avg_conf = (
        round(sum(b["confidence"] for b in text_blocks) / len(text_blocks), 4)
        if text_blocks
        else 0.0
    )

    full_text = "\n".join(
        b["text"].strip()
        for p in pages
        for b in p.get("blocks", [])
        if b.get("type") == "text"
    )

    # Trim block text in-place
    for p in pages:
        for b in p.get("blocks", []):
            if isinstance(b.get("text"), str):
                b["text"] = b["text"].strip()

    return {
        **data,
        "fullText": full_text,
        "averageConfidence": avg_conf,
        "pages": pages,
    }


class VisionAgent:
    """
    Agentic vision extraction — call → validate → auto-correct, max N rounds.

    The LLM receives a proper multi-turn conversation that grows with each
    failed round: its own bad output appears as an assistant message, and the
    validation errors arrive as the next user message. This mirrors how the
    model was trained for self-correction tasks.
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
        """Build the opening system + user messages for a page."""
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
        Send the current conversation to the vision LLM and return (raw_text, usage).
        `messages` grows across rounds: system → user → assistant → user → ...
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

    async def extract(
        self,
        image_urls: list[str],
    ) -> tuple[dict, list[dict], list[dict]]:
        """
        Run vision extraction on all provided images.

        Returns:
            (result, usage_logs, conversation) where:
              result       — validated vision output dict (with fullText + averageConfidence)
              usage_logs   — list of per-call usage dicts (aggregated by tasks into processing_stages.usage)
              conversation — list of correction-round records for audit/debugging
        """
        provider = (
            "ollama"
            if "ollama" in self._model.lower() or "11434" in str(self._client.base_url)
            else "openai"
        )
        usage_logs: list[dict] = []
        conversation: list[dict] = []
        page_results: list[dict] = []

        for page_num, url in enumerate(image_urls, start=1):
            log.info("vision_agent_downloading_image", page=page_num, url=url[:80])
            image_bytes, mime_type = await self._download_image(url)

            # Build the conversation history for this page.
            # Each failed round appends:  assistant (bad output) → user (correction).
            # The LLM therefore sees its own prior attempts in the assistant role.
            messages = self._build_initial_messages(image_bytes, mime_type)
            correction_text: Optional[str] = None  # recorded for the conversation trail

            for attempt in range(1, self._max_iterations + 1):
                log.info("vision_agent_calling_llm", page=page_num, attempt=attempt, model=self._model)
                raw_text, usage = await self._call_llm(messages)
                usage_logs.append({"stage": "VISION", "model": self._model, "provider": provider, **usage})

                errors: list[str] = []
                try:
                    parsed = json.loads(_clean_json(raw_text))
                    errors = _validate_vision_output(parsed)
                except json.JSONDecodeError as e:
                    errors = [f"Response is not valid JSON: {e}"]

                conversation.append({
                    "round": attempt,
                    "page": page_num,
                    "correction_sent": correction_text,
                    "raw_response": raw_text,
                    "errors": errors,
                    "success": not bool(errors),
                })

                if not errors:
                    log.info("vision_agent_success", page=page_num, attempt=attempt)
                    page_results.append(parsed)
                    break

                if attempt < self._max_iterations:
                    correction_text = (
                        "Your output had these validation errors:\n"
                        + "\n".join(f"  - {e}" for e in errors)
                        + "\n\nPlease correct the JSON and return the full valid response."
                    )
                    # Extend the conversation: LLM sees its own bad output as assistant,
                    # then receives the correction as a new user turn.
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

        # Merge multi-page results into a single output
        merged = self._merge_pages(page_results)
        final = _compute_derived_fields(merged)
        return final, usage_logs, conversation

    async def run(
        self,
        job_input: dict,
        previous_results: dict[str, dict],
    ) -> tuple[dict, list[dict], list[dict]]:
        """
        Unified agent interface called by the generic run_stage task.

        Extracts image_urls from job_input and delegates to extract().
        previous_results is unused by the vision stage (it is always first).
        """
        image_urls: list[str] = job_input.get("image_urls", [])
        if not image_urls:
            raise ValueError(
                "job_input must contain a non-empty 'image_urls' list for VISION stage"
            )
        return await self.extract(image_urls)

    def _merge_pages(self, page_results: list[dict]) -> dict:
        """
        Merge vision output from multiple images (pages) into a single output dict.
        Page numbers are re-assigned sequentially.
        """
        if len(page_results) == 1:
            return page_results[0]

        all_pages = []
        language_hints: set[str] = set()

        for result in page_results:
            language_hints.update(result.get("meta", {}).get("languageHints", []))
            for page in result.get("pages", []):
                all_pages.append(page)

        # Re-number pages sequentially
        for i, page in enumerate(all_pages, start=1):
            page["pageNumber"] = i

        return {
            "meta": {
                "sourceType": "image",
                "pageCount": len(all_pages),
                "languageHints": list(language_hints),
                "engine": "vision-llm",
            },
            "pages": all_pages,
        }

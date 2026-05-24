"""
StructureAgent — vision output → StructureOutput.

Receives the raw text + visual element descriptions from VisionAgent and
extracts typed, validated identity fields.  All interpretation happens here —
VisionAgent deliberately does none.
"""

from __future__ import annotations

import json
import re
import time
from typing import Optional

import structlog
from openai import AsyncOpenAI
from pydantic import ValidationError

from app.agents.exceptions import AgentExhaustedError
from app.config import Settings
from app.models.pipeline import ConversationEntry, UsageEntry
from app.models.structure import StructureOutput

log = structlog.get_logger(__name__)

# ── Document type codes (mirrors NestJS DocumentTypeCode enum) ─────────────────
DOCUMENT_TYPE_CODES = [
    "NATIONAL_ID",
    "PASSPORT",
    "BIRTH_CERT",
    "ALIEN_REGISTRATION_CARD",
    "SOCIAL_SECURITY_CARD",
    "MARRIAGE_CERT",
    "DRIVING_LICENCE",
    "PROFESSIONAL_LICENSE",
    "WORK_ID",
    "STUDENT_ID",
    "HEALTH_INSURANCE_CARD",
    "UNKNOWN",
]

VALID_WARNINGS = {
    "DOCUMENT_TYPE_UNCERTAIN",
    "MULTIPLE_DOB_VALUES_FOUND",
    "MULTIPLE_ID_VALUES_FOUND",
    "MISSING_CRITICAL_FIELD",
    "LOW_EXTRACTION_CONFIDENCE",
    "CONFLICTING_NAME_VALUES",
    "EXPIRED_DOCUMENT",
}

# Injected programmatically by _sanitize() — never emitted by the LLM
_PROGRAMMATIC_WARNINGS = {"LOW_OCR_CONFIDENCE"}

STRUCTURE_SYSTEM_PROMPT = (
    "You are a document understanding engine. "
    "Extract structured identity information from OCR output. "
    "Output ONLY valid JSON — no markdown, no code fences, no text outside the JSON."
)


def _build_user_prompt(vision_output: dict, document_types: list[dict]) -> str:
    vision_json = json.dumps(vision_output, indent=2)
    doc_type_list = "\n".join(f" - {dt['code']}" for dt in document_types)

    return f"""\
You are a document understanding engine.

Your task is to analyze OCR output and extract structured identity information.
You MUST use only the provided OCR data.
You MUST NOT infer or guess missing values.
You MUST NOT use world knowledge beyond what is in the text and visual elements.

Input OCR data:
<<<{vision_json}>>>

The input contains, per page:
  - "text"          : verbatim transcription of all visible text
  - "visualElements": plain-English descriptions of non-text visual elements
                      (flags, photographs, fingerprints, signatures, stamps, etc.)
  - "confidence"    : overall OCR quality for that page (0–1)
  - "fullText"      : all page texts concatenated (convenience field)
  - "averageConfidence": mean confidence across pages

---

DOCUMENT TYPE CLASSIFICATION
Allowed document type codes:
{doc_type_list}

Classification rules:
- Match on keywords in fullText (e.g. "REPUBLIC OF KENYA", "NATIONAL IDENTITY CARD")
- Match on field patterns (ID number format, MRZ lines, header text)
- Match on visual element descriptions (e.g. a Kenyan flag strongly suggests a Kenyan document)
- If uncertain → UNKNOWN, never guess

---

EXTRACTION RULES
- Missing fields → null
- Arrays with no data → []
- Names → UPPERCASE
- Dates → ISO 8601 (YYYY-MM-DD)
- Gender → "Male", "Female", or "Unknown" only
- Country → ISO 3166-1 alpha-2 code (e.g. "KE", "US", "GB")
- Document numbers → extract exactly as written, no formatting changes
- Do NOT correct spelling
- Do NOT assume context
- Record which page numbers you drew from in raw.pagesReferenced

---

BIOMETRICS RULES
Determine presence from the visualElements descriptions across all pages:
- photoPresent        → true if any visualElement describes a photograph or portrait of a person
- fingerprintPresent  → true if any visualElement describes a fingerprint or thumbprint
- signaturePresent    → true if any visualElement describes a handwritten signature
- Default all to false if not found in visualElements

---

SCORING RULES
- All confidence scores MUST be a decimal in [0.0000, 1.0000] rounded to 4 decimal places
- Never return an integer or a value outside [0.0000, 1.0000]

---

WARNING CODES (add to quality.warnings[] when applicable):
- DOCUMENT_TYPE_UNCERTAIN     → could not confidently classify
- MULTIPLE_DOB_VALUES_FOUND   → more than one date of birth detected
- MULTIPLE_ID_VALUES_FOUND    → more than one ID number detected
- MISSING_CRITICAL_FIELD      → fullName or document.number is null
- LOW_EXTRACTION_CONFIDENCE   → fewer than 2 critical fields found
- CONFLICTING_NAME_VALUES     → more than one name value detected
- EXPIRED_DOCUMENT            → expiryDate is in the past

---

Return ONLY valid JSON matching this exact schema. No markdown. No explanation.

{{
  "documentType": {{"code": string, "confidence": number}},
  "country": string | null,
  "person": {{
    "fullName": string | null,
    "givenNames": string[],
    "surname": string | null,
    "dateOfBirth": string | null,
    "placeOfBirth": string | null,
    "gender": "Male" | "Female" | "Unknown"
  }},
  "document": {{
    "number": string | null,
    "serialNumber": string | null,
    "batchNumber": string | null,
    "issuer": string | null,
    "placeOfIssue": string | null,
    "issueDate": string | null,
    "expiryDate": string | null
  }},
  "address": {{
    "raw": string | null,
    "country": string | null,
    "components": [{{"type": string, "value": string}}]
  }},
  "biometrics": {{
    "photoPresent": boolean,
    "fingerprintPresent": boolean,
    "signaturePresent": boolean
  }},
  "additionalFields": [{{"fieldName": string, "fieldValue": string}}],
  "raw": {{
    "pagesReferenced": [number]
  }},
  "quality": {{
    "extractionConfidence": number,
    "warnings": string[]
  }}
}}
"""


def _clean_json(text: str) -> str:
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _validate_structure_output(data: dict) -> list[str]:
    """
    Validate structure agent output before constructing a StructureOutput.
    Returns a list of human-readable error strings for the correction prompt.
    Empty list = valid.
    """
    errors: list[str] = []

    doc_type = data.get("documentType")
    if not isinstance(doc_type, dict):
        errors.append("documentType must be an object")
    else:
        if doc_type.get("code") not in DOCUMENT_TYPE_CODES:
            errors.append(f"documentType.code must be one of: {', '.join(DOCUMENT_TYPE_CODES)}")
        conf = doc_type.get("confidence")
        if not isinstance(conf, (int, float)) or not (0 <= conf <= 1):
            errors.append("documentType.confidence must be a decimal in [0, 1]")

    person = data.get("person")
    if not isinstance(person, dict):
        errors.append("person must be an object")
    elif person.get("gender") not in ("Male", "Female", "Unknown"):
        errors.append('person.gender must be "Male", "Female", or "Unknown"')

    if not isinstance(data.get("document"), dict):
        errors.append("document must be an object")

    address = data.get("address")
    if not isinstance(address, dict):
        errors.append("address must be an object")
    elif not isinstance(address.get("components", []), list):
        errors.append("address.components must be an array")

    if not isinstance(data.get("biometrics"), dict):
        errors.append("biometrics must be an object")

    raw = data.get("raw")
    if not isinstance(raw, dict):
        errors.append("raw must be an object")
    else:
        pages_ref = raw.get("pagesReferenced")
        if not isinstance(pages_ref, list):
            errors.append("raw.pagesReferenced must be an array")
        elif not all(isinstance(p, (int, float)) for p in pages_ref):
            errors.append("raw.pagesReferenced must contain only numbers")

    quality = data.get("quality")
    if not isinstance(quality, dict):
        errors.append("quality must be an object")
    else:
        v = quality.get("extractionConfidence")
        if not isinstance(v, (int, float)) or not (0 <= v <= 1):
            errors.append("quality.extractionConfidence must be a decimal in [0, 1]")
        warnings = quality.get("warnings", [])
        if not isinstance(warnings, list):
            errors.append("quality.warnings must be an array")
        else:
            invalid = [w for w in warnings if w not in VALID_WARNINGS]
            if invalid:
                errors.append(f"quality.warnings contains unknown codes: {invalid}")

    additional_fields = data.get("additionalFields", [])
    if not isinstance(additional_fields, list):
        errors.append("additionalFields must be an array")
    else:
        for i, field in enumerate(additional_fields):
            if not isinstance(field, dict):
                errors.append(f"additionalFields[{i}] must be an object with fieldName and fieldValue")
            else:
                if not isinstance(field.get("fieldName"), str) or not field.get("fieldName"):
                    errors.append(f"additionalFields[{i}].fieldName must be a non-empty string")
                if not isinstance(field.get("fieldValue"), str):
                    errors.append(
                        f"additionalFields[{i}].fieldValue must be a string — "
                        f"got {type(field.get('fieldValue')).__name__}. "
                        "Use an empty string if the value is unknown, or omit this field entirely."
                    )

    return errors


def _sanitize(data: dict, ocr_confidence: float) -> dict:
    """
    Fix edge cases before constructing StructureOutput:
    - Coerce unknown documentType.code to UNKNOWN
    - Round confidence values to 4dp
    - Inject ocrConfidence from vision averageConfidence (never trust LLM for this)
    - Add LOW_OCR_CONFIDENCE warning programmatically when ocrConfidence < 0.75
    - Strip invalid warning codes
    - Coerce pagesReferenced elements to int
    """
    doc_type = data.get("documentType", {})
    if doc_type.get("code") not in DOCUMENT_TYPE_CODES:
        doc_type["code"] = "UNKNOWN"

    quality = data.get("quality", {})
    # ocrConfidence comes from the vision stage, not the LLM
    quality["ocrConfidence"] = round(float(ocr_confidence), 4)
    v = quality.get("extractionConfidence", 0)
    if isinstance(v, (int, float)):
        quality["extractionConfidence"] = round(float(v), 4)

    warnings: list[str] = [w for w in quality.get("warnings", []) if w in VALID_WARNINGS]
    if ocr_confidence < 0.75 and "LOW_OCR_CONFIDENCE" not in warnings:
        warnings.append("LOW_OCR_CONFIDENCE")
    quality["warnings"] = warnings

    raw = data.get("raw", {})
    raw["pagesReferenced"] = [
        int(p) for p in raw.get("pagesReferenced", []) if isinstance(p, (int, float))
    ]

    return data


class StructureAgent:
    """
    Agentic structure extraction — call → validate → auto-correct, max N rounds.
    """

    def __init__(self, settings: Settings) -> None:
        self._model = settings.structure_ai_model
        self._max_iterations = settings.max_agent_iterations
        self._client = AsyncOpenAI(
            base_url=settings.structure_ai_base_url,
            api_key=settings.structure_ai_api_key,
        )

    async def _call_llm(self, messages: list[dict]) -> tuple[str, dict]:
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
        vision_output: dict,
        document_types: Optional[list[dict]] = None,
    ) -> tuple[StructureOutput, list[UsageEntry], list[ConversationEntry]]:
        """
        Run structure extraction using vision output as context.

        Args:
            vision_output: plain dict from VisionAgent (or retrieved from DB JSONB)
            document_types: list of {code} dicts — defaults to all known codes

        Returns:
            (StructureOutput, usage_entries, conversation_entries)
        """
        if document_types is None:
            document_types = [{"code": c} for c in DOCUMENT_TYPE_CODES]

        provider = (
            "ollama"
            if "ollama" in self._model.lower() or "11434" in str(self._client.base_url)
            else "openai"
        )
        prompt = _build_user_prompt(vision_output, document_types)

        messages: list[dict] = [
            {"role": "system", "content": STRUCTURE_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ]
        usage_entries: list[UsageEntry] = []

        # Round 1 opening turns — system identity + user task with full vision output.
        # Stored once; subsequent rounds only add user(correction) + assistant turns.
        conversation: list[ConversationEntry] = [
            ConversationEntry(round=1, role="system", content=STRUCTURE_SYSTEM_PROMPT),
            ConversationEntry(round=1, role="user",   content=prompt),
        ]

        for attempt in range(1, self._max_iterations + 1):
            log.info("structure_agent_calling_llm", attempt=attempt, model=self._model)
            raw_text, raw_usage = await self._call_llm(messages)

            usage_entries.append(UsageEntry(
                stage="STRUCTURE",
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
                errors = _validate_structure_output(parsed)
            except json.JSONDecodeError as e:
                errors = [f"Response is not valid JSON: {e}"]

            conversation.append(ConversationEntry(
                round=attempt, role="assistant",
                content=raw_text,
                success=not bool(errors),
                metadata={"errors": errors} if errors else None,
            ))

            if not errors and parsed is not None:
                log.info("structure_agent_success", attempt=attempt)
                ocr_confidence = float(vision_output.get("averageConfidence", 0.0))
                sanitized = _sanitize(parsed, ocr_confidence)
                try:
                    return StructureOutput.model_validate(sanitized), usage_entries, conversation
                except ValidationError as exc:
                    errors = [
                        f"Output failed schema validation: {err['loc']}: {err['msg']}"
                        for err in exc.errors()
                    ]
                    log.warning("structure_agent_pydantic_validation_failed", attempt=attempt, errors=errors)

            if attempt < self._max_iterations:
                correction_text = (
                    "Your output had these validation errors:\n"
                    + "\n".join(f"  - {e}" for e in errors)
                    + "\n\nPlease correct the JSON and return the full valid response."
                )
                messages.append({"role": "assistant", "content": raw_text})
                messages.append({"role": "user", "content": correction_text})
                conversation.append(ConversationEntry(
                    round=attempt + 1, role="user",
                    content=correction_text,
                ))
                log.warning("structure_agent_correction", attempt=attempt, errors=errors)
            else:
                log.error("structure_agent_failed_all_attempts", errors=errors)
                raise AgentExhaustedError(
                    f"Structure agent failed after {self._max_iterations} attempts: {errors}",
                    conversation,
                )

        raise AgentExhaustedError("Structure agent loop exited without returning", conversation)

    async def run(
        self,
        job_input: dict,
        previous_results: dict[str, dict],
    ) -> tuple[StructureOutput, list[UsageEntry], list[ConversationEntry]]:
        """
        Unified agent interface called by the generic run_stage task.

        Pulls the VISION stage result (plain dict from DB JSONB) from
        previous_results and delegates to extract().
        """
        vision_result = previous_results.get("VISION")
        if not vision_result:
            raise RuntimeError(
                "VISION stage result not found in previous_results — "
                "STRUCTURE cannot run without it"
            )
        return await self.extract(vision_result)

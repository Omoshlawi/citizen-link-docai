"""
StructureAgent — vision output → validated structured document fields.

Receives the raw text + visual element descriptions from VisionAgent and
extracts typed, validated identity fields.  All interpretation happens here —
VisionAgent deliberately does none.

Output schema mirrors NestJS TextExtractionOutputSchema (extraction.dto.ts).
"""

import json
import re
import time
from typing import Any, Optional

import structlog
from openai import AsyncOpenAI

from app.agents.exceptions import AgentExhaustedError
from app.config import Settings

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
    "LOW_OCR_CONFIDENCE",
    "DOCUMENT_TYPE_UNCERTAIN",
    "MULTIPLE_DOB_VALUES_FOUND",
    "MULTIPLE_ID_VALUES_FOUND",
    "MISSING_CRITICAL_FIELD",
    "LOW_EXTRACTION_CONFIDENCE",
    "CONFLICTING_NAME_VALUES",
    "EXPIRED_DOCUMENT",
}

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
- LOW_OCR_CONFIDENCE          → averageConfidence < 0.75
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
    "ocrConfidence": number,
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
    Validate structure agent output. Returns a list of validation errors.
    Empty list = valid.
    """
    errors = []

    # documentType
    doc_type = data.get("documentType")
    if not isinstance(doc_type, dict):
        errors.append("documentType must be an object")
    else:
        code = doc_type.get("code")
        if code not in DOCUMENT_TYPE_CODES:
            errors.append(
                f"documentType.code must be one of: {', '.join(DOCUMENT_TYPE_CODES)}"
            )
        conf = doc_type.get("confidence")
        if not isinstance(conf, (int, float)) or not (0 <= conf <= 1):
            errors.append("documentType.confidence must be a decimal in [0, 1]")

    # person
    person = data.get("person")
    if not isinstance(person, dict):
        errors.append("person must be an object")
    else:
        gender = person.get("gender")
        if gender not in ("Male", "Female", "Unknown"):
            errors.append('person.gender must be "Male", "Female", or "Unknown"')

    # document
    if not isinstance(data.get("document"), dict):
        errors.append("document must be an object")

    # address
    address = data.get("address")
    if not isinstance(address, dict):
        errors.append("address must be an object")
    else:
        if not isinstance(address.get("components", []), list):
            errors.append("address.components must be an array")

    # biometrics
    if not isinstance(data.get("biometrics"), dict):
        errors.append("biometrics must be an object")

    # raw
    raw = data.get("raw")
    if not isinstance(raw, dict):
        errors.append("raw must be an object")
    else:
        pages_ref = raw.get("pagesReferenced")
        if not isinstance(pages_ref, list):
            errors.append("raw.pagesReferenced must be an array")
        elif not all(isinstance(p, (int, float)) for p in pages_ref):
            errors.append("raw.pagesReferenced must contain only numbers")

    # quality
    quality = data.get("quality")
    if not isinstance(quality, dict):
        errors.append("quality must be an object")
    else:
        for field in ("ocrConfidence", "extractionConfidence"):
            v = quality.get(field)
            if not isinstance(v, (int, float)) or not (0 <= v <= 1):
                errors.append(f"quality.{field} must be a decimal in [0, 1]")
        warnings = quality.get("warnings", [])
        if not isinstance(warnings, list):
            errors.append("quality.warnings must be an array")
        else:
            invalid = [w for w in warnings if w not in VALID_WARNINGS]
            if invalid:
                errors.append(f"quality.warnings contains unknown codes: {invalid}")

    return errors


def _sanitize_output(data: dict) -> dict:
    """
    Post-process the structure output:
    - Ensure documentType.code is a valid enum (fallback to UNKNOWN)
    - Round confidence values to 4 decimal places
    - Filter warnings to only valid codes
    - Ensure pagesReferenced contains integers
    """
    doc_type = data.get("documentType", {})
    if doc_type.get("code") not in DOCUMENT_TYPE_CODES:
        doc_type["code"] = "UNKNOWN"

    quality = data.get("quality", {})
    for field in ("ocrConfidence", "extractionConfidence"):
        v = quality.get(field, 0)
        if isinstance(v, (int, float)):
            quality[field] = round(float(v), 4)

    quality["warnings"] = [
        w for w in quality.get("warnings", []) if w in VALID_WARNINGS
    ]

    raw = data.get("raw", {})
    raw["pagesReferenced"] = [
        int(p) for p in raw.get("pagesReferenced", [])
        if isinstance(p, (int, float))
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
        """
        Send the current conversation to the structure LLM.

        messages grows across correction rounds:
          system → user(vision output) → assistant(bad output) → user(correction) → …
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
        vision_output: dict,
        document_types: Optional[list[dict]] = None,
    ) -> tuple[dict, list[dict], list[dict]]:
        """
        Run structure extraction using vision output as context.

        Args:
            vision_output: validated output from VisionAgent
            document_types: list of {code} dicts for type classification
                           (defaults to all known codes if not provided)

        Returns:
            (result, usage_logs, conversation)
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
        correction_text: Optional[str] = None
        usage_logs: list[dict] = []
        conversation: list[dict] = []

        for attempt in range(1, self._max_iterations + 1):
            log.info("structure_agent_calling_llm", attempt=attempt, model=self._model)
            raw_text, usage = await self._call_llm(messages)
            usage_logs.append({"stage": "STRUCTURE", "model": self._model, "provider": provider, **usage})

            errors: list[str] = []
            try:
                parsed = json.loads(_clean_json(raw_text))
                errors = _validate_structure_output(parsed)
            except json.JSONDecodeError as e:
                errors = [f"Response is not valid JSON: {e}"]

            conversation.append({
                "round": attempt,
                "correction_sent": correction_text,
                "raw_response": raw_text,
                "errors": errors,
                "success": not bool(errors),
            })

            if not errors:
                log.info("structure_agent_success", attempt=attempt)
                return _sanitize_output(parsed), usage_logs, conversation

            if attempt < self._max_iterations:
                correction_text = (
                    "Your output had these validation errors:\n"
                    + "\n".join(f"  - {e}" for e in errors)
                    + "\n\nPlease correct the JSON and return the full valid response."
                )
                messages.append({"role": "assistant", "content": raw_text})
                messages.append({"role": "user", "content": correction_text})
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
    ) -> tuple[dict, list[dict], list[dict]]:
        """
        Unified agent interface called by the generic run_stage task.

        Pulls the VISION stage result from previous_results and delegates
        to extract().  job_input is unused by this stage.
        """
        vision_result = previous_results.get("VISION")
        if not vision_result:
            raise RuntimeError(
                "VISION stage result not found in previous_results — "
                "STRUCTURE cannot run without it"
            )
        return await self.extract(vision_result)

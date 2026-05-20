"""
StructureAgent — vision OCR output → validated structured document fields.

Calls the structure LLM with the vision output as context, validates against
the expected schema, and auto-corrects up to MAX_AGENT_ITERATIONS rounds.

Output schema mirrors NestJS TextExtractionOutputSchema (extraction.dto.ts).
"""

import json
import re
import time
from typing import Any, Optional

import structlog
from openai import AsyncOpenAI

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
You MUST NOT use world knowledge beyond what is in the text.

Input OCR data:
<<<{vision_json}>>>

---

DOCUMENT TYPE CLASSIFICATION
Allowed document type codes:
{doc_type_list}

Classification rules:
- Match on keywords in fullText or blocks (e.g. "REPUBLIC OF KENYA", "NATIONAL IDENTITY CARD")
- Match on field patterns (ID number format, MRZ lines, header text)
- Match on layout hints (block order, block types)
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
- Track EVERY block id you used in raw.blocksUsed

---

BIOMETRICS RULES
- photoPresent → true if a photo block exists in vision output
- fingerprintPresent → true if a fingerprint block exists in vision output
- signaturePresent → true if a signature block exists in vision output
- Default all to false if block type not found

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
  "raw": {{"blocksUsed": string[]}},
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
        components = address.get("components", [])
        if not isinstance(components, list):
            errors.append("address.components must be an array")

    # biometrics
    biometrics = data.get("biometrics")
    if not isinstance(biometrics, dict):
        errors.append("biometrics must be an object")

    # quality
    quality = data.get("quality")
    if not isinstance(quality, dict):
        errors.append("quality must be an object")
    else:
        ocr_conf = quality.get("ocrConfidence")
        if not isinstance(ocr_conf, (int, float)) or not (0 <= ocr_conf <= 1):
            errors.append("quality.ocrConfidence must be a decimal in [0, 1]")
        ext_conf = quality.get("extractionConfidence")
        if not isinstance(ext_conf, (int, float)) or not (0 <= ext_conf <= 1):
            errors.append("quality.extractionConfidence must be a decimal in [0, 1]")
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

    async def _call_llm(
        self,
        prompt: str,
        correction_prompt: Optional[str] = None,
    ) -> tuple[str, dict]:
        """
        Call the structure LLM. On correction rounds, prepend the error context.
        Returns (raw_text, usage_dict).
        """
        user_content = prompt
        if correction_prompt:
            user_content = correction_prompt + "\n\n" + prompt

        start = time.perf_counter()
        response = await self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": STRUCTURE_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
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
    ) -> tuple[dict, list[dict]]:
        """
        Run structure extraction using vision output as context.

        Args:
            vision_output: validated output from VisionAgent
            document_types: list of {code, name} dicts for type classification
                           (defaults to all known codes if not provided)

        Returns:
            (result, usage_logs) where:
              result     — validated structure output dict
              usage_logs — list of per-call usage dicts for ai_usage_logs table
        """
        if document_types is None:
            document_types = [{"code": c} for c in DOCUMENT_TYPE_CODES]

        prompt = _build_user_prompt(vision_output, document_types)
        usage_logs: list[dict] = []
        correction_prompt: Optional[str] = None
        last_raw: Optional[str] = None
        last_errors: list[str] = []

        for attempt in range(1, self._max_iterations + 1):
            log.info(
                "structure_agent_calling_llm",
                attempt=attempt,
                model=self._model,
            )
            raw_text, usage = await self._call_llm(prompt, correction_prompt)
            usage_logs.append(
                {
                    "stage": "TEXT",
                    "model": self._model,
                    "provider": "ollama"
                    if "ollama" in self._model.lower() or "11434" in str(self._client.base_url)
                    else "openai",
                    **usage,
                    "success": True,
                }
            )

            try:
                parsed = json.loads(_clean_json(raw_text))
                errors = _validate_structure_output(parsed)

                if not errors:
                    log.info("structure_agent_success", attempt=attempt)
                    return _sanitize_output(parsed), usage_logs

                last_errors = errors
                last_raw = raw_text

            except json.JSONDecodeError as e:
                last_errors = [f"Response is not valid JSON: {e}"]
                last_raw = raw_text

            if attempt < self._max_iterations:
                correction_prompt = (
                    f"Your previous output was invalid. Errors:\n"
                    + "\n".join(f"  - {e}" for e in last_errors)
                    + f"\n\nYour previous output was:\n{last_raw}\n\n"
                    "Please correct the JSON and return the full valid response."
                )
                log.warning(
                    "structure_agent_correction",
                    attempt=attempt,
                    errors=last_errors,
                )
            else:
                log.error(
                    "structure_agent_failed_all_attempts",
                    errors=last_errors,
                )
                raise RuntimeError(
                    f"Structure agent failed after {self._max_iterations} attempts: "
                    f"{last_errors}"
                )

        # Should never reach here
        raise RuntimeError("Structure agent loop exited without returning")

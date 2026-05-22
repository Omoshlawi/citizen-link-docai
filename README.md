# citizen-link-docai

> **Document AI Microservice** for the CitizenLink platform.
>
> Owns all document intelligence: vision OCR and structured field extraction. Any caller fires a single HTTP call and receives event-based webhook callbacks as each pipeline stage completes. Embedding is the caller's responsibility — it happens after the user reviews and confirms the extracted fields.

---

## Table of Contents

- [Why This Service Exists](#why-this-service-exists)
- [Architecture Overview](#architecture-overview)
- [Pipeline Registry](#pipeline-registry)
- [Agent Correction Loop](#agent-correction-loop)
- [Pipeline Stages](#pipeline-stages)
- [Post-Stage Quality Gate](#post-stage-quality-gate)
- [API Endpoints](#api-endpoints)
- [Webhook Events (docai → caller)](#webhook-events-docai--caller)
- [Database Schema](#database-schema)
- [Project Structure](#project-structure)
- [Environment Variables](#environment-variables)
- [Running Locally (without Docker)](#running-locally-without-docker)
- [Running with Docker](#running-with-docker)
- [Development Guide](#development-guide)
- [Adding a New Pipeline](#adding-a-new-pipeline)
- [Deployment Notes](#deployment-notes)
- [Verification Checklist](#verification-checklist)

---

## Why This Service Exists

Before this service, NestJS owned three BullMQ queues for AI processing:
- `case-vision-extraction` — vision OCR
- `case-text-extraction` — structured field extraction
- `case-post-processing` — post-processing enrichment

This created tight coupling between business logic and AI infrastructure. Every model change or prompt tweak required a full NestJS deploy.

**citizen-link-docai extracts all of that into a fully autonomous service.** NestJS now makes two calls:

| Call | Direction | Purpose |
|---|---|---|
| `POST /v1/jobs/extraction` | NestJS → docai | Fire an extraction job (202, async) |
| `POST /api/webhooks/docai/progress` | docai → NestJS | Event-based stage callbacks |
| `POST /v1/embed` | caller → docai | Synchronous embedding for RAG |

**Why is embedding separate from the pipeline?**

Embedding is intentionally not part of the extraction pipeline. AI-extracted fields may contain errors — the user reviews them and makes corrections before confirming. Only confirmed, human-verified data should be embedded into the vector index. Embedding unverified AI output would permanently corrupt semantic search. The caller calls `POST /v1/embed` after the user has reviewed and confirmed the extracted fields.

**Benefits:**
- AI model changes → update one env var, redeploy docai, zero NestJS changes
- Prompt changes → edit one file in docai, zero NestJS changes
- New pipelines → add a `PipelineConfig` entry and a typed endpoint, zero infrastructure changes
- Scale AI processing independently from the NestJS API server
- Full audit trail — cost, latency, token counts, and every message turn per model call
- Agent correction loops with proper multi-turn conversation — bad LLM output is auto-corrected before the caller ever sees it
- Rule-based quality gates fail fast before spending tokens on downstream stages

---

## Architecture Overview

```
Mobile / Web
     │
     ▼
NestJS (business logic)
     │
     ├── POST /v1/jobs/extraction ─────────────────► citizen-link-docai
     │   202 Accepted (fire and forget)                       │
     │                                             ┌──────────────────────────┐
     │                                             │        ARQ Worker        │
     │                                             │                          │
     │                                             │  run_stage("VISION")     │
     │                                             │    ↓ gate check          │
     │                                             │  run_stage("STRUCTURE")  │
     │                                             └──────────┬───────────────┘
     │                                                        │
     │◄── POST /api/webhooks/docai/progress ──────────────────┘
     │    extraction.vision.success
     │    extraction.structure.success
     │    extraction.success  (terminal — full nested result)
     │
     │   [user reviews fields, confirms]
     │
     └── POST /v1/embed ──────────────────────────► citizen-link-docai
         (synchronous, only on confirmed data)
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| **ARQ** (not BullMQ) | Async-native Python (asyncio coroutines). No BullMQ Python port needed. Internal queue — no cross-service sharing. |
| **asyncpg + raw SQL** | Fast, explicit, no ORM overhead. Alembic handles migrations. |
| **Generic job model** (`job_type + input JSONB`) | Any pipeline stores its input in JSONB. New pipelines need no schema changes. |
| **Pipeline registry** | `PipelineConfig` declares stages, gates, and result shape. One `run_stage` task handles all pipelines. |
| **Event-based webhooks** | Dot-notation events (`extraction.vision.success`) encode pipeline, stage, and outcome in one field. No cross-referencing of `stage` + `status` fields. Scales to any pipeline namespace. |
| **Typed endpoints per pipeline** | `POST /v1/jobs/extraction` validates extraction-specific fields. Future pipelines get their own endpoints. Internal model is generic. |
| **Webhook delivery via ARQ** | `task_deliver_webhook` is itself an ARQ task — ARQ retries it automatically on failure. Every attempt logged to `webhook_deliveries`. |
| **Own PostgreSQL** | Fully autonomous — no shared schema with NestJS. No caller IDs stored here at all. |
| **Pre-signed S3 URLs** | Caller generates URLs before calling docai. docai downloads via plain httpx — no S3 SDK needed here. |
| **Per-turn conversation storage** | Each LLM message turn (system/user/assistant) gets its own `stage_conversations` row. Concatenating all turns in order reconstructs the full conversation thread. System prompts are stored per-job — a snapshot of the exact prompt at processing time. |
| **No base64 in DB** | Vision user rows store the signed download URL in `metadata`, not the base64 bytes. The image lives in MinIO; the URL is the audit reference. |
| **Per-job webhook URL** | `webhook_url` is part of the job request body — one docai instance can serve multiple independent callers. |
| **JSONB for variable fields** | Result data, usage metrics, and conversation metadata live in JSONB columns. New fields never require a migration. |

---

## Pipeline Registry

All pipeline configuration lives in `app/pipeline/registry.py`. The single `run_stage` ARQ task dispatches to the correct agent by looking up `(job_type, stage)` in the registry — no `if/elif` logic in the task runner.

```python
PIPELINES: dict[str, PipelineConfig] = {
    "EXTRACTION": PipelineConfig(
        namespace       = "extraction",
        stages          = ["VISION", "STRUCTURE"],
        build_result    = _build_extraction_result,
        post_stage_gate = {"VISION": _gate_vision_quality},
    ),
    # Future pipelines slot in here:
    # "FRAUD_DETECTION": PipelineConfig(namespace="fraud-detection", stages=["CHECK"], ...),
    # "MATCH_VERIFICATION": PipelineConfig(namespace="match", stages=["MATCH"], ...),
}
```

**`PipelineConfig` fields:**

| Field | Type | Purpose |
|---|---|---|
| `namespace` | `str` | Dot-notation event prefix (e.g. `"extraction"`). All webhook event names are derived from this automatically — `{namespace}.{stage.lower()}.success`, `{namespace}.success`, `{namespace}.{stage.lower()}.failed`, `{namespace}.failed`. |
| `stages` | `list[str]` | Ordered stage names — `run_stage` dispatches left-to-right |
| `build_result` | `callable` | Assembles the `{namespace}.success` terminal payload from all stage results. Returns a dict nested by stage name. |
| `post_stage_gate` | `dict[stage → callable]` | Rule-based fast-fail checks run after a stage succeeds, before the success event fires or the next stage is enqueued |

All event strings are constructed dynamically — adding a stage to any pipeline automatically produces the right events with zero manual wiring.

---

## Agent Correction Loop

Both `VisionAgent` and `StructureAgent` maintain a proper multi-turn conversation with the LLM across correction rounds. Each failed round does **not** start fresh — the model sees its own prior attempts in the `assistant` role, which is how it was trained to self-correct.

### What the LLM sees (accumulated context per call)

```
Round 1 call:
  system    → "You are a document reader. You observe and transcribe..."
  user      → [extraction instructions + image]

Round 2 call (if round 1 failed validation):
  system    → "You are a document reader..."
  user      → [extraction instructions + image]
  assistant → "{...bad output from round 1...}"
  user      → "Your output had these errors: [...]. Please correct."

Round 3 call (if round 2 also failed):
  system    → ...
  user      → [initial ask + image]
  assistant → "{...round 1 output...}"
  user      → "Errors: [...]"
  assistant → "{...round 2 output...}"
  user      → "Still invalid. Errors: [...]. Please correct."
```

### How it is stored (delta per round, no duplication)

Each round writes only its new turns to `stage_conversations`. Concatenating all rows in order reconstructs the full thread:

```
round=1  role=system     content="You are a document reader..."
round=1  role=user       content="Read this document image..."   metadata={"url":"...","mime_type":"image/jpeg"}
round=1  role=assistant  content="<bad output>"                  success=false  metadata={"errors":[...]}

round=2  role=user       content="Your output had these errors: ..."
round=2  role=assistant  content="<valid output>"                success=true
```

Round 1 stores the full opening exchange (system + user + assistant). Rounds 2+ store only the correction user message and the new assistant response. The system prompt is stored once per job — a frozen snapshot of the prompt at processing time, so historical records remain accurate if the prompt later changes.

After `MAX_AGENT_ITERATIONS` failures:
- `AgentExhaustedError` raised (carries all conversation turns)
- FAILED stage row stored with all rounds' turns
- `extraction.{stage}.failed` + `extraction.failed` webhooks sent

### Vision Agent — what it extracts

`VisionAgent` is deliberately unstructured. It has two jobs per page:

1. **Text** — verbatim transcription of every visible character, preserving casing, punctuation, and line breaks. No interpretation. `[...]` for illegible portions.
2. **Visual elements** — plain-English prose descriptions of non-text elements that carry identity significance: national symbols (flags, coats of arms), biometric elements (photographs, fingerprints, signatures), security features (stamps, holograms, MRZ strips).

Pages are processed **in parallel** (`asyncio.gather`) — total latency = slowest page, not the sum of all pages. Each page has its own isolated correction loop.

`fullText` and `averageConfidence` are computed deterministically from the page results — never asked from the LLM.

### Structure Agent — what it extracts

`StructureAgent` receives the full `VisionOutput` (all pages, text + visual element descriptions) and extracts typed, validated identity fields. All interpretation happens here — VisionAgent deliberately does none. It uses the visual element descriptions to determine biometrics presence (photo, fingerprint, signature) and country hints (flag descriptions).

---

## Pipeline Stages

```
POST /v1/jobs/extraction received
         │
         ▼
  Create processing_jobs row (PENDING, job_type="EXTRACTION")
  Enqueue: run_stage(job_id, "VISION")
         │
         ▼
[ARQ Worker] run_stage("VISION")
  → Download images via pre-signed URLs
  → VisionAgent: parallel page extraction, multi-turn correction per page
  → Store processing_stages row (VISION / SUCCESS or FAILED)
  → Store stage_conversations rows (one row per message turn per page)
  → Post-stage gate check (OCR confidence + text length)
      → gate fails:  fire extraction.vision.failed + extraction.failed → stop (no retry)
      → gate passes: continue
  → Fire: extraction.vision.success { averageConfidence, fullText, … }
  → Enqueue: run_stage(job_id, "STRUCTURE")
         │
         ▼
[ARQ Worker] run_stage("STRUCTURE")
  → Read VISION result from processing_stages (plain dict from DB JSONB)
  → StructureAgent: multi-turn LLM conversation, max N rounds
  → Store processing_stages row (STRUCTURE / SUCCESS or FAILED)
  → Store stage_conversations rows (one row per message turn)
  → Fire: extraction.structure.success { documentType, person, document, … }
  → Mark job: COMPLETED
  → Fire: extraction.success { vision: {…}, structure: {…} }   ← terminal

[ARQ Worker] task_deliver_webhook  ← retries up to WEBHOOK_MAX_RETRIES times
  → POST to callback_url with X-Internal-Secret header
  → Log every attempt to webhook_deliveries
  → On failure → ARQ retries with backoff
```

**On any stage failure:**
```
  → Store processing_stages row (status: FAILED, error: reason)
  → Store stage_conversations rows (all turns attempted)
  → Mark job: FAILED
  → Fire: extraction.{stage}.failed { reason }          ← stage-specific
  → Fire: extraction.failed { failedAt, reason }         ← flat rollup
```

**Future pipelines** register in `app/pipeline/registry.py` and slot in automatically — no changes to `run_stage`, no worker changes. See [Adding a New Pipeline](#adding-a-new-pipeline).

---

## Post-Stage Quality Gate

After each stage succeeds, before the success event fires or the next stage is enqueued, the pipeline registry can run a rule-based gate function. Gates are declared per-stage in `PipelineConfig.post_stage_gate`.

The gate runs **before** the stage success event — a gate rejection never fires `*.success` for that stage.

### VISION gate (`_gate_vision_quality`)

Two fast checks — no LLM call, no token cost:

| Check | Threshold | Meaning |
|---|---|---|
| `averageConfidence` | `< 0.40` | Image is physically unreadable (blur, damage, bad lighting). Structure agent would be extracting from noise. |
| `len(fullText.strip())` | `< 25 chars` | Almost no text detected — blank upload, solid-colour screenshot, landscape photo, or anything that is not a document. |

When a gate fails:
- The job is marked `FAILED` immediately
- `extraction.vision.failed` fires with a user-friendly `reason` — the caller can forward this directly to the user
- `extraction.failed` fires as the flat rollup (same reason, adds `failedAt: "vision"`)
- No retry is attempted (this is an expected business outcome, not an infrastructure failure)
- No STRUCTURE LLM call is made — zero wasted tokens

**Tuning thresholds:**

```python
# app/pipeline/registry.py
_MIN_OCR_CONFIDENCE = 0.40   # adjust based on operational experience with your vision model
_MIN_TEXT_LENGTH    = 25     # below this, not enough text for meaningful field extraction
```

---

## API Endpoints

### Submission & status

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `GET` | `/health` | None | Liveness — always 200 if the process is alive |
| `GET` | `/health/ready` | None | Readiness — checks DB (`SELECT 1`) + Redis (`PING`) |
| `POST` | `/v1/jobs/extraction` | `X-Internal-Secret` | Submit a document extraction job |
| `GET` | `/v1/jobs` | `X-Internal-Secret` | List jobs (optional `job_type` + `status` filters + pagination) |
| `GET` | `/v1/jobs/{jobId}` | `X-Internal-Secret` | Poll a single job status |
| `POST` | `/v1/embed` | `X-Internal-Secret` | Generate an embedding vector |
| `GET` | `/docs` | None | Swagger UI (disable in production) |

### Inspection (observability)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/stages` | List processing stages; filters: `job_id`, `job_type`, `stage`, `status`; opt-in `include_result` |
| `GET` | `/v1/stages/{stage_id}` | Single stage + optional nested conversations |
| `GET` | `/v1/jobs/{job_id}/stages` | All stages for one job; optional `include_result` + `include_conversations` |
| `GET` | `/v1/conversations` | List message turns; filters: `job_id`, `stage_id`, `stage`, `role`, `success`, `page_num` |
| `GET` | `/v1/stages/{stage_id}/conversations` | All turns for one stage, in conversation order |
| `GET` | `/v1/webhooks` | List delivery attempts; filters: `job_id`, `event` (prefix match), `delivered` |
| `GET` | `/v1/webhooks/{delivery_id}` | Single delivery with full payload |

### POST /v1/jobs/extraction

**Request:**
```json
{
  "case_number": "CL-2025-00042",
  "image_urls": [
    "https://minio.example.com/tmp/doc-page1.jpg?X-Amz-Signature=...",
    "https://minio.example.com/tmp/doc-page2.jpg?X-Amz-Signature=..."
  ],
  "webhook_url": "http://caller-server:2000/api/webhooks/docai/progress",
  "priority": 5
}
```

| Field | Required | Description |
|---|---|---|
| `case_number` | ✅ | Human-readable reference used for logging |
| `image_urls` | ✅ | Pre-signed MinIO/S3 URLs (caller generates; docai downloads via plain HTTP). Min 1 URL. |
| `webhook_url` | ✅ | Where docai POSTs event callbacks |
| `priority` | — | 1 (highest) – 10 (lowest), default `5` |

**Response (202):**
```json
{ "job_id": "550e8400-e29b-41d4-a716-446655440000" }
```

The caller stores `job_id` as `AIExtraction.docaiJobId` so incoming webhooks can be matched to the correct extraction record.

### POST /v1/embed

**Request:**
```json
{
  "text": "JOHN DOE NATIONAL_ID Document number: 12345678",
  "use_case": "document"
}
```

`use_case` is `"document"` (default) or `"search"`. For Ollama nomic-embed-text, this controls the prefix (`search_document:` vs `search_query:`). OpenAI models ignore the prefix.

**Response (200):**
```json
{
  "embedding": [0.0234, -0.0891, "..."],
  "dims": 768,
  "model": "nomic-embed-text"
}
```

---

## Webhook Events (docai → caller)

docai POSTs to the job's `webhook_url` with `X-Internal-Secret: <CALLBACK_SECRET>` for every event. The caller validates this header on every incoming request.

### Event taxonomy

All events follow the pattern `{pipeline-namespace}.{stage}.{outcome}`.

| Event | Terminal | `result` shape | Meaning |
|---|---|---|---|
| `extraction.vision.success` | No | `{ averageConfidence, fullText, … }` | OCR done; structure stage starting |
| `extraction.structure.success` | No | `{ documentType, person, document, … }` | Field extraction done; finalising |
| `extraction.success` | ✅ | `{ vision: {…}, structure: {…} }` | All stages succeeded — full nested result |
| `extraction.vision.failed` | ✅ | `{ reason }` | Gate or agent failure at VISION stage |
| `extraction.structure.failed` | ✅ | `{ reason }` | Agent failure at STRUCTURE stage |
| `extraction.failed` | ✅ | `{ failedAt, reason }` | Flat rollup — fires alongside the stage-specific event |

**Consumers choose their listener strategy:**
- Want full transparency → listen to every event
- Want final result only → listen to `extraction.success`
- Want a single failure handler → listen to `extraction.failed`
- Want to react per-stage → listen to `extraction.vision.success`, `extraction.structure.success`, etc.

### Payload envelope

Every POST has this shape:

```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "event": "extraction.vision.success",
  "result": { ... },
  "timestamp": "2025-01-01T12:00:00.000Z"
}
```

### Event payloads in detail

**`extraction.vision.success`** — raw VisionAgent output:
```json
{
  "fullText": "REPUBLIC OF KENYA\nNATIONAL IDENTITY CARD\nJOHN KAMAU DOE\n...",
  "averageConfidence": 0.91
}
```

**`extraction.structure.success`** — raw StructureAgent output (the extracted fields):
```json
{
  "documentType": { "code": "NATIONAL_ID", "confidence": 0.97 },
  "country": "KE",
  "person": {
    "fullName": "JOHN KAMAU DOE",
    "surname": "DOE",
    "givenNames": ["JOHN", "KAMAU"],
    "dateOfBirth": "1990-05-15",
    "placeOfBirth": "NAIROBI",
    "gender": "Male"
  },
  "document": {
    "number": "12345678",
    "issuer": "REPUBLIC OF KENYA",
    "issueDate": "2015-03-20",
    "expiryDate": null
  },
  "biometrics": { "photoPresent": true, "fingerprintPresent": true, "signaturePresent": false },
  "quality": { "extractionConfidence": 0.88, "warnings": [] }
}
```

**`extraction.success`** — terminal, nested combined result:
```json
{
  "vision": {
    "fullText": "REPUBLIC OF KENYA\nNATIONAL IDENTITY CARD\n...",
    "averageConfidence": 0.91
  },
  "structure": {
    "documentType": { "code": "NATIONAL_ID", "confidence": 0.97 },
    "person": { "fullName": "JOHN KAMAU DOE", "...": "..." },
    "quality": { "extractionConfidence": 0.88, "warnings": [] }
  }
}
```

**`extraction.vision.failed`** / **`extraction.structure.failed`** — stage-specific failure:
```json
{ "reason": "Document image quality is too poor to process (OCR confidence 23% — minimum 40%). Please upload a clearer, well-lit image of the document." }
```

**`extraction.failed`** — flat rollup (fires alongside the stage event):
```json
{
  "failedAt": "vision",
  "reason": "Document image quality is too poor to process (OCR confidence 23% — minimum 40%)."
}
```

---

## Database Schema

docai has its own PostgreSQL database — completely separate from NestJS. No caller IDs are stored here. The caller stores `job_id` on its own `AIExtraction` record to match incoming webhooks.

### `processing_jobs`
Tracks the lifecycle of each job. The `input` JSONB column holds pipeline-specific parameters — new pipelines need no schema changes.

```sql
processing_jobs (
  id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  job_type      TEXT        NOT NULL DEFAULT 'EXTRACTION',  -- 'EXTRACTION' | future types
  input         JSONB       NOT NULL DEFAULT '{}',          -- pipeline-specific parameters
  webhook_url   TEXT        NOT NULL,
  priority      INT         NOT NULL DEFAULT 5,             -- 1 (highest) to 10 (lowest)
  status        TEXT        NOT NULL DEFAULT 'PENDING',     -- PENDING | IN_PROGRESS | COMPLETED | FAILED
  current_stage TEXT,                                       -- VISION | STRUCTURE | null
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

### `processing_stages`
One row per pipeline stage attempt.

```sql
processing_stages (
  id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id       UUID        NOT NULL REFERENCES processing_jobs(id) ON DELETE CASCADE,
  stage        TEXT        NOT NULL,              -- 'VISION' | 'STRUCTURE'
  status       TEXT        NOT NULL,              -- 'SUCCESS' | 'FAILED'
  result       JSONB,                             -- stage output (null on failure)
  error        TEXT,                              -- failure reason (null on success)
  usage        JSONB,                             -- { model, provider, calls:[...], total_input_tokens,
                                                 --   total_output_tokens, total_latency_ms, estimated_cost_usd }
  started_at   TIMESTAMPTZ,
  completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

### `stage_conversations`
One row per LLM **message turn** within a stage. Concatenating all rows for a stage in `round, created_at` order reconstructs the full conversation thread with zero duplication.

```sql
stage_conversations (
  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  stage_id   UUID        NOT NULL REFERENCES processing_stages(id) ON DELETE CASCADE,
  job_id     UUID        NOT NULL REFERENCES processing_jobs(id)   ON DELETE CASCADE,
  round      INT         NOT NULL,        -- correction round (1 = first call, 2+ = corrections)
  page       INT,                         -- image page number (Vision only; null for Structure)
  role       TEXT        NOT NULL,        -- 'system' | 'user' | 'assistant'
  content    TEXT        NOT NULL,        -- message text — prompt or LLM response
  success    BOOLEAN,                     -- NULL for system/user rows; TRUE/FALSE on assistant rows
  metadata   JSONB,                       -- Vision user rows: { url, mime_type }
                                         -- Failed assistant rows: { errors: [...] }
                                         -- All other rows: NULL
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

**Row pattern for a 2-round Vision extraction (page 1):**

| round | page | role | content | success | metadata |
|---|---|---|---|---|---|
| 1 | 1 | system | `You are a document reader…` | NULL | NULL |
| 1 | 1 | user | `Read this document image…` | NULL | `{"url":"https://…","mime_type":"image/jpeg"}` |
| 1 | 1 | assistant | `<bad response>` | false | `{"errors":["pages must be…"]}` |
| 2 | 1 | user | `Your output had these errors: …` | NULL | NULL |
| 2 | 1 | assistant | `<valid response>` | true | NULL |

### `webhook_deliveries`
Audit trail for every callback attempt. The `stage` column stores the full event string (e.g. `extraction.vision.success`).

```sql
webhook_deliveries (
  id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id          UUID        NOT NULL REFERENCES processing_jobs(id) ON DELETE CASCADE,
  stage           TEXT        NOT NULL,   -- full event string, e.g. "extraction.vision.success"
  payload         JSONB       NOT NULL,
  callback_url    TEXT        NOT NULL,
  response_status INT,
  response_body   TEXT,
  attempt_count   INT         NOT NULL DEFAULT 1,
  delivered       BOOLEAN     NOT NULL DEFAULT FALSE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

---

## Project Structure

```
citizen-link-docai/
├── main.py                          # FastAPI app — lifespan, middleware, routers
├── requirements.txt
├── Dockerfile                       # Multi-stage python:3.11-slim build
├── docker-compose.yml               # api + worker + postgres + redis
├── docker-entrypoint.sh             # Validates env, runs alembic upgrade head, starts uvicorn
├── .env.example                     # All environment variables documented
│
├── alembic/                         # Independent DB migration history
│   ├── env.py                       # Reads DATABASE_URL from env, no ORM
│   ├── alembic.ini
│   └── versions/
│       └── 001_initial_schema.py    # Single migration — complete current schema
│
└── app/
    ├── config.py                    # pydantic-settings — all env vars, one source of truth
    ├── database.py                  # asyncpg pool creation + teardown + auto-create DB
    ├── dependencies.py              # require_internal_auth / get_pool
    ├── middleware.py                # RequestIDMiddleware
    ├── exceptions.py                # AppError hierarchy + FastAPI exception handlers
    ├── health.py                    # GET /health + GET /health/ready
    │
    ├── models/                      # Typed domain models — no raw dicts in agent I/O
    │   ├── vision.py                # VisionMeta, VisionPage, VisionOutput (Pydantic)
    │   ├── structure.py             # StructureOutput + nested models (Pydantic)
    │   └── pipeline.py              # ConversationEntry, UsageEntry, UsageSummary, JobRecord
    │
    ├── processing/                  # Job intake
    │   ├── schemas.py               # ExtractionRequest, JobStatusResponse, JobListResponse
    │   ├── repository.py            # Raw SQL on processing_jobs
    │   ├── service.py               # submit_extraction() — creates job + enqueues run_stage
    │   └── router.py                # POST /v1/jobs/extraction, GET /v1/jobs, GET /v1/jobs/{id}
    │
    ├── embedding/                   # Embedding endpoint (caller-initiated, post-confirmation)
    │   ├── schemas.py               # EmbedRequest, EmbedResponse
    │   ├── service.py               # EmbeddingService — wraps AsyncOpenAI (Ollama + OpenAI)
    │   └── router.py                # POST /v1/embed
    │
    ├── inspection/                  # Read-only observability endpoints
    │   ├── schemas.py               # StageResponse, ConversationResponse, WebhookDeliveryResponse
    │   ├── repository.py            # Paginated queries with joins across all four tables
    │   └── router.py                # GET /v1/stages, /v1/conversations, /v1/webhooks + sub-routes
    │
    ├── agents/                      # Agentic LLM processors with multi-turn correction loop
    │   ├── exceptions.py            # AgentExhaustedError — carries conversation turns on failure
    │   ├── vision_agent.py          # Image URLs → VisionOutput (parallel pages, max N rounds each)
    │   └── structure_agent.py       # VisionOutput → StructureOutput (max N rounds)
    │
    └── pipeline/                    # Internal async job queue
        ├── enums.py                 # DocaiEvent (single source of truth for all webhook events),
        │                            # JobStatus, StageStatus, JobType
        ├── registry.py              # PipelineConfig, PIPELINES dict, get_pipeline(), get_agent()
        │                            # Post-stage gates: _gate_vision_quality
        ├── tasks.py                 # ARQ coroutines: run_stage, task_deliver_webhook
        │                            # Helpers: _store_stage, _store_conversation,
        │                            #   _get_stage_result, _notify_failure
        ├── worker.py                # WorkerSettings — registers tasks, startup/shutdown hooks
        └── webhook.py               # HTTP delivery to caller + webhook_deliveries logging
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in all required values.

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | ✅ | — | PostgreSQL connection string for docai's own DB |
| `INTERNAL_SECRET` | ✅ | — | Shared secret — callers send in `X-Internal-Secret` for inbound calls |
| `CALLBACK_SECRET` | ✅ | — | Secret docai sends in `X-Internal-Secret` on every outgoing webhook |
| `REDIS_URL` | — | `redis://localhost:6379` | Redis connection string for ARQ queue |
| `VISION_AI_BASE_URL` | — | `http://localhost:11434/v1` | OpenAI-compatible base URL for vision model |
| `VISION_AI_API_KEY` | — | `ollama` | API key for vision model |
| `VISION_AI_MODEL` | — | `gemma3:4b` | Vision model name |
| `STRUCTURE_AI_BASE_URL` | — | `http://localhost:11434/v1` | Base URL for structure model |
| `STRUCTURE_AI_API_KEY` | — | `ollama` | API key for structure model |
| `STRUCTURE_AI_MODEL` | — | `gemma3:4b` | Structure model name |
| `EMBEDDING_BASE_URL` | — | `http://localhost:11434/v1` | Base URL for embedding model |
| `EMBEDDING_API_KEY` | — | `ollama` | API key for embedding model |
| `EMBEDDING_MODEL` | — | `nomic-embed-text` | Embedding model name |
| `EMBEDDING_IS_OPENAI` | — | `false` | Set `true` for OpenAI `text-embedding-*` (skips nomic prefix) |
| `MAX_AGENT_ITERATIONS` | — | `3` | Max correction rounds before a stage fails |
| `WEBHOOK_MAX_RETRIES` | — | `3` | Max ARQ retry attempts for webhook delivery |
| `LOG_LEVEL` | — | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

> **Generating secrets:**
> ```bash
> openssl rand -hex 32
> ```
> Use the same value for `INTERNAL_SECRET` here and `DOCAI_SERVICE_INTERNAL_SECRET` in NestJS.
> Use the same value for `CALLBACK_SECRET` here and `DOCAI_CALLBACK_SECRET` in NestJS (to validate incoming webhooks).

---

## Running Locally (without Docker)

### 1. Create a virtual environment

```bash
cd citizen-link-docai
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Set up environment

```bash
cp .env.example .env
# Edit .env — set DATABASE_URL, adjust model URLs and secrets
```

```bash
DATABASE_URL=postgresql://docai:password@localhost:5432/citizen-link-docai
REDIS_URL=redis://localhost:6379
INTERNAL_SECRET=dev-secret-change-in-production
CALLBACK_SECRET=same-as-nestjs-docai-callback-secret
VISION_AI_BASE_URL=http://localhost:11434/v1
STRUCTURE_AI_BASE_URL=http://localhost:11434/v1
EMBEDDING_BASE_URL=http://localhost:11434/v1
```

### 3. Run migrations

```bash
.venv/bin/alembic upgrade head
```

### 4. Start the API server

```bash
uvicorn main:app --reload --port 8002
```

### 5. Start the ARQ worker (separate terminal)

```bash
python -m app.pipeline.worker
```

The API server and the worker are **two separate processes**. In production they run in two separate Docker containers.

---

## Running with Docker

```bash
cd citizen-link-docai
cp .env.example .env   # generate INTERNAL_SECRET and CALLBACK_SECRET

docker compose up --build -d
```

Four containers start:
- `api` — FastAPI on port 8002 (runs `alembic upgrade head` on startup)
- `worker` — ARQ worker (no HTTP port)
- `db` — PostgreSQL 16
- `redis` — Redis for ARQ queue

```bash
docker compose logs -f worker   # watch jobs process
docker compose down -v          # stop + delete volumes
```

**Ollama on the host machine:**
```bash
VISION_AI_BASE_URL=http://host.docker.internal:11434/v1
STRUCTURE_AI_BASE_URL=http://host.docker.internal:11434/v1
EMBEDDING_BASE_URL=http://host.docker.internal:11434/v1
```

---

## Development Guide

### Checking liveness and readiness

```bash
curl http://localhost:8002/health
curl http://localhost:8002/health/ready
```

### Testing the embedding endpoint

```bash
curl -X POST http://localhost:8002/v1/embed \
  -H "X-Internal-Secret: $INTERNAL_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"text": "JOHN DOE National ID 12345678", "use_case": "document"}'
```

### Submitting a test extraction job

```bash
curl -X POST http://localhost:8002/v1/jobs/extraction \
  -H "X-Internal-Secret: $INTERNAL_SECRET" \
  -H "Content-Type: application/json" \
  -d '{
    "case_number": "CL-2025-00001",
    "image_urls": ["https://your-presigned-url/image.jpg"],
    "webhook_url": "http://localhost:2000/api/webhooks/docai/progress"
  }'
```

### Inspecting stage results and conversation history

```bash
# Stage results and token costs
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT stage, status, error,
             usage->>'total_input_tokens' AS tokens,
             usage->>'estimated_cost_usd' AS cost_usd,
             completed_at - started_at AS duration
      FROM processing_stages ORDER BY created_at DESC LIMIT 10;"

# Full conversation thread for a specific job (reads like a chat log)
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT sc.round, sc.page, sc.role, sc.success,
             left(sc.content, 80) AS content_preview,
             sc.metadata
      FROM stage_conversations sc
      JOIN processing_stages ps ON ps.id = sc.stage_id
      WHERE ps.job_id = '<job_id>'
      ORDER BY ps.created_at, sc.page NULLS LAST, sc.round, sc.created_at;"

# Failed assistant turns across all jobs (validation errors visible without JSON parsing)
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT sc.job_id, sc.round, sc.page, sc.metadata->'errors' AS errors
      FROM stage_conversations sc
      WHERE sc.role = 'assistant' AND sc.success = false
      ORDER BY sc.created_at DESC LIMIT 20;"

# Webhook delivery history (stage column holds the full event string)
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT stage AS event, delivered, response_status, attempt_count
      FROM webhook_deliveries ORDER BY created_at DESC;"
```

Or use the inspection REST API:

```bash
# All stages for a job with conversations nested
curl "http://localhost:8002/v1/jobs/<job_id>/stages?include_conversations=true" \
  -H "X-Internal-Secret: $INTERNAL_SECRET"

# All failed assistant turns across all jobs
curl "http://localhost:8002/v1/conversations?success=false" \
  -H "X-Internal-Secret: $INTERNAL_SECRET"

# Webhook delivery attempts for a job
curl "http://localhost:8002/v1/webhooks?job_id=<job_id>" \
  -H "X-Internal-Secret: $INTERNAL_SECRET"
```

---

## Adding a New Pipeline

New pipelines register in `app/pipeline/registry.py` and get a typed endpoint. No changes to `run_stage`, no worker changes, no schema migrations.

### 1. Add events to the enum (both services)

```python
# app/pipeline/enums.py :: DocaiEvent
FRAUD_DETECTION_CHECK_SUCCESS = "fraud-detection.check.success"
FRAUD_DETECTION_SUCCESS       = "fraud-detection.success"
FRAUD_DETECTION_CHECK_FAILED  = "fraud-detection.check.failed"
FRAUD_DETECTION_FAILED        = "fraud-detection.failed"
```

```typescript
// src/docai/docai-webhook.schema.ts :: DocaiEvent  (NestJS mirror)
FRAUD_DETECTION_CHECK_SUCCESS = 'fraud-detection.check.success',
FRAUD_DETECTION_SUCCESS       = 'fraud-detection.success',
FRAUD_DETECTION_CHECK_FAILED  = 'fraud-detection.check.failed',
FRAUD_DETECTION_FAILED        = 'fraud-detection.failed',
```

### 2. Write the agent

```python
# app/agents/fraud_agent.py
class FraudAgent:
    async def run(
        self,
        job_input: dict,
        previous_results: dict[str, dict],
    ) -> tuple[FraudOutput, list[UsageEntry], list[ConversationEntry]]:
        ...
```

### 3. Register in the pipeline registry

```python
# app/pipeline/registry.py

def _build_fraud_result(stage_results: dict[str, dict]) -> dict:
    return {"check": stage_results.get("CHECK", {})}

def _make_fraud_agent(settings: Settings) -> Any:
    from app.agents.fraud_agent import FraudAgent
    return FraudAgent(settings)

_AGENT_FACTORIES = {
    ("EXTRACTION", "VISION"):       _make_vision_agent,
    ("EXTRACTION", "STRUCTURE"):    _make_structure_agent,
    ("FRAUD_DETECTION", "CHECK"):   _make_fraud_agent,   # ← new
}

PIPELINES = {
    "EXTRACTION": PipelineConfig(...),
    "FRAUD_DETECTION": PipelineConfig(           # ← new
        namespace    = "fraud-detection",
        stages       = ["CHECK"],
        build_result = _build_fraud_result,
    ),
}
```

### 4. Add a typed endpoint

```python
# app/processing/router.py
@router.post("/jobs/fraud-detection", status_code=202)
async def submit_fraud_detection(request: FraudRequest, ...):
    job_id = await service.submit(
        job_type="FRAUD_DETECTION",
        job_input={"document_id": request.document_id, "image_urls": request.image_urls},
        webhook_url=request.webhook_url,
    )
    return {"job_id": job_id}
```

### 5. Handle in NestJS

Add a case to `DocaiWebhookController` for `DocaiEvent.FRAUD_DETECTION_CHECK_SUCCESS` etc., and implement the corresponding handler in `DocaiWebhookService`.

That's it. `run_stage` picks up `FRAUD_DETECTION` jobs automatically via the registry, constructs all event strings from the namespace, and fires them without any manual wiring.

---

## Deployment Notes

### Two containers, one image

The `api` and `worker` services use the same Docker image:
- `api`: `./docker-entrypoint.sh` → `alembic upgrade head` → `uvicorn`
- `worker`: `python -m app.pipeline.worker`

### Scaling the worker

```bash
docker compose up --scale worker=3 -d
```

ARQ uses Redis as coordination — multiple workers safely dequeue different jobs with no duplication.

### Logs

All log lines are JSON (structlog). Every pipeline task binds `job_id` to the log context:

```bash
docker compose logs worker | grep '"job_id": "your-uuid"'
```

---

## Verification Checklist

```bash
# 1. Liveness
curl http://localhost:8002/health
# → {"status": "ok"}

# 2. Readiness
curl http://localhost:8002/health/ready
# → {"status": "ready", "database": "ok", "redis": "ok"}

# 3. Embedding
curl -X POST http://localhost:8002/v1/embed \
  -H "X-Internal-Secret: $INTERNAL_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"text": "John Doe National ID 12345678"}'
# → {"embedding": [...], "dims": 768, "model": "nomic-embed-text"}

# 4. Auth guard
curl -X POST http://localhost:8002/v1/embed \
  -H "X-Internal-Secret: wrong" \
  -H "Content-Type: application/json" \
  -d '{"text": "test"}'
# → 403 Forbidden

# 5. Tables present
docker compose exec db psql -U docai -d citizen-link-docai -c "\dt"
# → processing_jobs, processing_stages, stage_conversations, webhook_deliveries

# 6. Submit extraction job and trace events
JOB_ID=$(curl -sX POST http://localhost:8002/v1/jobs/extraction \
  -H "X-Internal-Secret: $INTERNAL_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"case_number":"TEST-001","image_urls":["https://..."],"webhook_url":"http://localhost:2000/api/webhooks/docai/progress"}' \
  | jq -r .job_id)

# Watch job progress
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT status, current_stage, updated_at FROM processing_jobs WHERE id = '$JOB_ID';"

# Check conversation turns stored (read like a chat log)
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT round, page, role, success, left(content, 60) AS preview
      FROM stage_conversations sc
      JOIN processing_stages ps ON ps.id = sc.stage_id
      WHERE ps.job_id = '$JOB_ID'
      ORDER BY ps.created_at, sc.page NULLS LAST, sc.round, sc.created_at;"

# Check events fired
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT stage AS event, delivered, response_status, attempt_count
      FROM webhook_deliveries WHERE job_id = '$JOB_ID' ORDER BY created_at;"

# Expected events on success:
#   extraction.vision.success    delivered=true
#   extraction.structure.success delivered=true
#   extraction.success           delivered=true
```

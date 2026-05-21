# citizen-link-docai

> **Document AI Microservice** for the CitizenLink platform.
>
> Owns all document intelligence: vision OCR and structured field extraction. Any caller fires a single HTTP call and receives webhook callbacks as each pipeline stage completes. Embedding is the caller's responsibility — it happens after the user reviews and confirms the extracted fields.

---

## Table of Contents

- [Why This Service Exists](#why-this-service-exists)
- [Architecture Overview](#architecture-overview)
- [Agent Correction Loop](#agent-correction-loop)
- [Pipeline Stages](#pipeline-stages)
- [API Endpoints](#api-endpoints)
- [Webhook Payload (docai → caller)](#webhook-payload-docai--caller)
- [Database Schema](#database-schema)
- [Project Structure](#project-structure)
- [Environment Variables](#environment-variables)
- [Running Locally (without Docker)](#running-locally-without-docker)
- [Running with Docker](#running-with-docker)
- [Development Guide](#development-guide)
- [Adding a New Pipeline Stage](#adding-a-new-pipeline-stage)
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
| `POST /v1/process` | NestJS → docai | Fire an extraction job (202, async) |
| `POST /api/webhooks/docai/progress` | docai → NestJS | Stage callbacks with results |
| `POST /v1/embed` | caller → docai | Synchronous embedding for RAG |

**Why is embedding separate from the pipeline?**

Embedding is intentionally not part of the extraction pipeline. AI-extracted fields may contain errors — the user reviews them and makes corrections before confirming. Only confirmed, human-verified data should be embedded into the vector index. Embedding unverified AI output would permanently corrupt semantic search. The caller calls `POST /v1/embed` after the user has reviewed and confirmed the extracted fields.

**Benefits:**
- AI model changes → update one env var, redeploy docai, zero NestJS changes
- Prompt changes → edit one file in docai, zero NestJS changes
- Scale AI processing independently from the NestJS API server
- Full audit trail — cost, latency, token counts, and every correction round per model call
- Agent correction loops with proper multi-turn conversation — bad LLM output is auto-corrected before the caller ever sees it

---

## Architecture Overview

```
Mobile / Web
     │
     ▼
NestJS (business logic)
     │
     ├── POST /v1/process ──────────────────────► citizen-link-docai
     │   202 Accepted (fire and forget)                    │
     │                                            ┌─────────────────┐
     │                                            │   ARQ Worker    │
     │                                            │                 │
     │                                            │  1. run_vision  │
     │                                            │  2. run_structure│
     │                                            └────────┬────────┘
     │                                                     │
     │◄── POST /api/webhooks/docai/progress ───────────────┘
     │    VISION (in_progress) then COMPLETED (with full result)
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
| **Webhook delivery via ARQ** | `task_deliver_webhook` is itself an ARQ task — ARQ retries it automatically on failure. Every attempt logged to `webhook_deliveries`. |
| **Own PostgreSQL** | Fully autonomous — no shared schema with NestJS. No caller IDs stored here at all. |
| **Pre-signed S3 URLs** | Caller generates URLs before calling docai. docai downloads via plain httpx — no S3 SDK needed here. |
| **Multi-turn agent correction** | Each failed round appends the LLM's bad output as `assistant` and the validation errors as `user`. The model sees its own failure history and corrects itself. |
| **Per-job webhook URL** | `webhook_url` is part of the job request body — one docai instance can serve multiple independent callers. |
| **JSONB for variable fields** | Result data, usage metrics, and conversation metadata live in JSONB columns. New fields never require a migration. |

---

## Agent Correction Loop

Both `VisionAgent` and `StructureAgent` maintain a proper multi-turn conversation with the LLM across correction rounds. Each failed round does **not** start fresh — the model sees its own prior attempts in the `assistant` role, which is how it was trained to self-correct.

```
Round 1:
  system    → "You are a pure OCR engine..."
  user      → [extraction instructions + image]

  → LLM responds with bad output

Round 2:
  system    → "You are a pure OCR engine..."
  user      → [extraction instructions + image]   ← same initial ask
  assistant → "{...bad output from round 1...}"   ← what it actually said
  user      → "Your output had these errors: [...]. Please correct."

  → LLM responds (better output, or still wrong)

Round 3 (if needed):
  system    → ...
  user      → [initial ask]
  assistant → "{...bad output round 1...}"
  user      → "Errors: [...]"
  assistant → "{...bad output round 2...}"
  user      → "Still invalid. Errors: [...]. Please correct."

After MAX_AGENT_ITERATIONS failures:
  → AgentExhaustedError raised (carries full conversation trail)
  → FAILED stage row stored with all rounds
  → FAILED webhook sent to caller: { failedAt: "VISION|STRUCTURE", reason: "..." }
```

Every round — whether it succeeds or fails — is persisted to `stage_conversations` so you have a complete audit trail of how the model arrived at its output (or why it couldn't).

---

## Pipeline Stages

```
POST /v1/process received
         │
         ▼
  Create processing_jobs row (PENDING)
  Enqueue: run_vision
         │
         ▼
[ARQ Worker] run_vision
  → Download images via pre-signed URLs
  → VisionAgent: multi-turn LLM conversation, max N rounds
  → Store processing_stages row (VISION / SUCCESS or FAILED)
  → Store stage_conversations rows (one per LLM call)
  → Send VISION webhook { stage: "VISION", status: "in_progress" }  ← progress signal only
  → Enqueue: run_structure
         │
         ▼
[ARQ Worker] run_structure
  → Read VISION result from processing_stages
  → StructureAgent: multi-turn LLM conversation, max N rounds
  → Store processing_stages row (STRUCTURE / SUCCESS or FAILED)
  → Store stage_conversations rows
  → Mark job: COMPLETED
  → Send COMPLETED webhook { stage: "COMPLETED", result: { fields, ocrConfidence, extractionConfidence } }

[ARQ Worker] task_deliver_webhook  ← retries up to WEBHOOK_MAX_RETRIES times
  → POST to callback_url with X-Internal-Secret header
  → Log every attempt to webhook_deliveries
  → On failure → ARQ retries with backoff
```

**On any stage failure:**
```
  → Store processing_stages row (status: FAILED, error: reason)
  → Store stage_conversations rows (all rounds attempted)
  → Mark job: FAILED
  → Send FAILED webhook { stage: "FAILED", failedAt: "VISION|STRUCTURE", reason: "..." }
```

**Future stages** (fraud detection, quality scoring, etc.) slot between `run_structure` and the COMPLETED signal without any caller contract changes. See [Adding a New Pipeline Stage](#adding-a-new-pipeline-stage).

---

## API Endpoints

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `GET` | `/health` | None | Liveness — always 200 if the process is alive |
| `GET` | `/health/ready` | None | Readiness — checks DB (`SELECT 1`) + Redis (`PING`) |
| `POST` | `/v1/process` | `X-Internal-Secret` + `X-User-Id` | Submit an extraction job |
| `GET` | `/v1/jobs` | `X-Internal-Secret` + `X-User-Id` | List all jobs (optional status filter + pagination) |
| `GET` | `/v1/jobs/{jobId}` | `X-Internal-Secret` + `X-User-Id` | Poll a single job status |
| `POST` | `/v1/embed` | `X-Internal-Secret` | Generate an embedding vector |
| `GET` | `/docs` | None | Swagger UI (disable in production) |

### POST /v1/process

**Request:**
```json
{
  "case_number": "CL-2025-00042",
  "image_urls": [
    "https://minio.example.com/tmp/doc-page1.jpg?X-Amz-Signature=...",
    "https://minio.example.com/tmp/doc-page2.jpg?X-Amz-Signature=..."
  ],
  "webhook_url": "http://caller-server:2000/api/webhooks/docai/progress"
}
```

- `case_number` — human-readable reference used for logging and image path organisation
- `image_urls` — pre-signed MinIO/S3 URLs (caller generates these; docai downloads via plain HTTP)
- `webhook_url` — where docai POSTs stage callbacks

**Response (202):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

The caller stores `job_id` as `AIExtraction.docaiJobId` so webhooks can be matched back to the right extraction record.

### GET /v1/jobs

Optional query params: `status` (PENDING, IN_PROGRESS, COMPLETED, FAILED), `page` (default 1), `page_size` (default 20, max 100).

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

## Webhook Payload (docai → caller)

docai POSTs to the job's `webhook_url` with `X-Internal-Secret: <CALLBACK_SECRET>` on every stage transition.

```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "stage": "VISION | COMPLETED | FAILED",
  "status": "in_progress | completed | failed",
  "result": null,
  "timestamp": "2025-01-01T12:00:00.000Z"
}
```

**Stage-specific result payloads:**

| Stage | `status` | `result` content |
|---|---|---|
| `VISION` | `in_progress` | `null` — progress signal only, no result data |
| `COMPLETED` | `completed` | `{ fields, ocrConfidence, extractionConfidence }` |
| `FAILED` | `failed` | `{ failedAt: "VISION\|STRUCTURE", reason: "error message" }` |

**Caller validates the `X-Internal-Secret` header on every incoming webhook.**

### COMPLETED result shape

```json
{
  "fields": {
    "documentType": { "code": "NATIONAL_ID", "confidence": 0.97 },
    "person": {
      "fullName": "JOHN KAMAU DOE",
      "surname": "DOE",
      "givenNames": ["JOHN", "KAMAU"],
      "dateOfBirth": "1990-05-15",
      "gender": "Male"
    },
    "document": {
      "number": "12345678",
      "issuer": "REPUBLIC OF KENYA",
      "issueDate": "2015-03-20",
      "expiryDate": null
    },
    "quality": {
      "ocrConfidence": 0.91,
      "extractionConfidence": 0.88,
      "warnings": []
    }
  },
  "ocrConfidence": 0.91,
  "extractionConfidence": 0.88
}
```

---

## Database Schema

docai has its own PostgreSQL database — completely separate from NestJS. No caller IDs are stored here. The caller stores `job_id` on its own `AIExtraction` record to match incoming webhooks.

### `processing_jobs`
Tracks the lifecycle of each extraction job.

```sql
processing_jobs (
  id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  case_number   TEXT        NOT NULL,
  image_urls    TEXT[]      NOT NULL,
  webhook_url   TEXT        NOT NULL,
  status        TEXT        NOT NULL DEFAULT 'PENDING',  -- PENDING | IN_PROGRESS | COMPLETED | FAILED
  current_stage TEXT,                                    -- VISION | STRUCTURE | null
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

### `processing_stages`
One row per pipeline stage attempt. Covers both success and failure. All variable data (result output, usage metrics) lives in JSONB so new fields never require a migration.

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
One row per LLM call within a stage. Stable queryable columns hold what you always filter on; variable metadata lives in JSONB.

```sql
stage_conversations (
  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  stage_id   UUID        NOT NULL REFERENCES processing_stages(id) ON DELETE CASCADE,
  job_id     UUID        NOT NULL REFERENCES processing_jobs(id) ON DELETE CASCADE,
  round      INT         NOT NULL,        -- which correction attempt (1 = first call)
  page       INT,                         -- which image page (vision only; null for structure)
  success    BOOLEAN     NOT NULL,        -- did this round pass validation?
  metadata   JSONB,                       -- { correction_sent, raw_response, errors }
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

**Example — 2-page vision extraction that needed one correction on page 2:**

| round | page | success | metadata.errors |
|---|---|---|---|
| 1 | 1 | true | `[]` |
| 1 | 2 | false | `["meta.engine must be vision-llm"]` |
| 2 | 2 | true | `[]` |

### `webhook_deliveries`
Audit trail for every callback attempt sent to the caller.

```sql
webhook_deliveries (
  id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id          UUID        NOT NULL REFERENCES processing_jobs(id) ON DELETE CASCADE,
  stage           TEXT        NOT NULL,
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
│       ├── 001_initial_schema.py    # Base schema
│       ├── 002_slim_processing_jobs.py      # Remove external_* fields
│       ├── 003_extraction_conversation_trail.py  # (superseded by 005)
│       ├── 004_processing_stages.py         # Replace extraction_results + ai_usage_logs
│       └── 005_stage_conversations_table.py # Dedicated conversation rows table
│
└── app/
    ├── config.py                    # pydantic-settings — all env vars, one source of truth
    ├── database.py                  # asyncpg pool creation + teardown + auto-create DB
    ├── dependencies.py              # require_internal_auth / get_pool
    ├── middleware.py                # RequestIDMiddleware — stamps X-Request-ID on every request
    ├── exceptions.py                # AppError hierarchy + FastAPI exception handlers
    ├── health.py                    # GET /health (liveness) + GET /health/ready (DB + Redis)
    │
    ├── processing/                  # Job intake
    │   ├── schemas.py               # ProcessRequest, JobStatusResponse, JobListResponse
    │   ├── repository.py            # Raw SQL on processing_jobs
    │   ├── service.py               # Create job + enqueue run_vision to ARQ
    │   └── router.py                # POST /v1/process, GET /v1/jobs, GET /v1/jobs/{id}
    │
    ├── embedding/                   # Embedding endpoint (caller-initiated, post-confirmation)
    │   ├── schemas.py               # EmbedRequest, EmbedResponse
    │   ├── service.py               # EmbeddingService — wraps AsyncOpenAI (Ollama + OpenAI)
    │   └── router.py                # POST /v1/embed
    │
    ├── agents/                      # Agentic LLM processors with multi-turn correction loop
    │   ├── exceptions.py            # AgentExhaustedError — carries conversation trail on failure
    │   ├── vision_agent.py          # Image URLs → validated OCR output (max N rounds)
    │   └── structure_agent.py       # OCR output → validated document fields (max N rounds)
    │
    └── pipeline/                    # Internal async job queue
        ├── enums.py                 # WebhookStage, WebhookStatus, JobStatus, PipelineStage
        ├── tasks.py                 # ARQ coroutines: run_vision, run_structure,
        │                            #   task_deliver_webhook
        │                            # Helpers: _store_stage, _store_conversation,
        │                            #   _get_stage_result, _build_usage, _notify_failure
        ├── worker.py                # WorkerSettings — registers tasks, startup/shutdown hooks
        └── webhook.py               # HTTP delivery to caller + webhook_deliveries logging
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in all required values.

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | ✅ | — | PostgreSQL connection string for docai's own DB |
| `INTERNAL_SECRET` | ✅ | — | Shared secret — callers send this in `X-Internal-Secret` to authenticate inbound calls |
| `CALLBACK_SECRET` | ✅ | — | Secret docai sends in `X-Internal-Secret` on every outgoing webhook |
| `REDIS_URL` | — | `redis://localhost:6379` | Redis connection string for ARQ queue |
| `VISION_AI_BASE_URL` | — | `http://localhost:11434/v1` | OpenAI-compatible base URL for vision model |
| `VISION_AI_API_KEY` | — | `ollama` | API key for vision model (`ollama` for local) |
| `VISION_AI_MODEL` | — | `gemma3:4b` | Vision model name |
| `STRUCTURE_AI_BASE_URL` | — | `http://localhost:11434/v1` | Base URL for structure/text extraction model |
| `STRUCTURE_AI_API_KEY` | — | `ollama` | API key for structure model |
| `STRUCTURE_AI_MODEL` | — | `gemma3:4b` | Structure model name |
| `EMBEDDING_BASE_URL` | — | `http://localhost:11434/v1` | Base URL for embedding model |
| `EMBEDDING_API_KEY` | — | `ollama` | API key for embedding model |
| `EMBEDDING_MODEL` | — | `nomic-embed-text` | Embedding model name |
| `EMBEDDING_IS_OPENAI` | — | `false` | Set `true` if using OpenAI `text-embedding-*` (skips nomic prefix) |
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

You need Python 3.11+, PostgreSQL, and Redis running locally.

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
# Edit .env — set DATABASE_URL to your local postgres, adjust Ollama URLs
```

Example local `.env` values:
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

The database is created automatically if it doesn't exist.

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

The API server and the worker are **two separate processes**. In production they run in two separate Docker containers (`api` and `worker` services in docker-compose.yml).

---

## Running with Docker

### First-time setup

```bash
cd citizen-link-docai
cp .env.example .env
# Edit .env — generate INTERNAL_SECRET and CALLBACK_SECRET
```

### Start all services

```bash
docker compose up --build -d
```

This starts four containers:
- `api` — FastAPI server on port 8002 (runs `alembic upgrade head` before starting)
- `worker` — ARQ worker (polls Redis for jobs, no HTTP port)
- `db` — PostgreSQL 16
- `redis` — Redis for ARQ queue

### View logs

```bash
docker compose logs -f          # all services
docker compose logs -f api      # API server only
docker compose logs -f worker   # ARQ worker only
```

### Stop

```bash
docker compose down
docker compose down -v   # also delete data volumes
```

### Ollama on the host machine

If Ollama runs on the host (not in Docker), the containers reach it via `host.docker.internal:11434`:

```bash
# In .env:
VISION_AI_BASE_URL=http://host.docker.internal:11434/v1
STRUCTURE_AI_BASE_URL=http://host.docker.internal:11434/v1
EMBEDDING_BASE_URL=http://host.docker.internal:11434/v1
```

---

## Development Guide

### Checking liveness and readiness

```bash
curl http://localhost:8002/health
# → {"status": "ok"}

curl http://localhost:8002/health/ready
# → {"status": "ready", "database": "ok", "redis": "ok"}
```

### Testing the embedding endpoint

```bash
curl -X POST http://localhost:8002/v1/embed \
  -H "X-Internal-Secret: your-secret" \
  -H "Content-Type: application/json" \
  -d '{"text": "JOHN DOE National ID 12345678", "use_case": "document"}'

# → {"embedding": [...768 floats...], "dims": 768, "model": "nomic-embed-text"}
```

### Submitting a test extraction job

```bash
curl -X POST http://localhost:8002/v1/process \
  -H "X-Internal-Secret: your-secret" \
  -H "X-User-Id: test-user-id" \
  -H "Content-Type: application/json" \
  -d '{
    "case_number": "CL-2025-00001",
    "image_urls": ["https://your-presigned-minio-url.com/image.jpg"],
    "webhook_url": "http://localhost:2000/api/webhooks/docai/progress"
  }'

# → {"job_id": "550e8400-e29b-41d4-a716-446655440000"}
```

### Polling job status

```bash
curl http://localhost:8002/v1/jobs/<job_id> \
  -H "X-Internal-Secret: your-secret" \
  -H "X-User-Id: test-user-id"

curl "http://localhost:8002/v1/jobs?status=COMPLETED&page=1&page_size=20" \
  -H "X-Internal-Secret: your-secret" \
  -H "X-User-Id: test-user-id"
```

### Inspecting stage results and usage

```bash
# See all stage results for a job
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT stage, status, error, usage->>'total_input_tokens' AS tokens,
             usage->>'estimated_cost_usd' AS cost_usd,
             completed_at - started_at AS duration
      FROM processing_stages ORDER BY created_at DESC LIMIT 10;"

# See the correction rounds for a job
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT sc.round, sc.page, sc.success, sc.metadata->'errors' AS errors
      FROM stage_conversations sc
      JOIN processing_stages ps ON ps.id = sc.stage_id
      WHERE ps.job_id = '<job_id>'
      ORDER BY sc.page, sc.round;"
```

### Inspecting webhook deliveries

```bash
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT stage, delivered, response_status, attempt_count
      FROM webhook_deliveries ORDER BY created_at DESC;"
```

### Running a new Alembic migration

```bash
# 1. Create the migration file
.venv/bin/alembic revision -m "add_some_column"

# 2. Edit the generated file in alembic/versions/ — use op.execute() for raw SQL
# 3. Apply it
.venv/bin/alembic upgrade head

# 4. Roll back one step if needed
.venv/bin/alembic downgrade -1
```

---

## Adding a New Pipeline Stage

Future stages (fraud detection, quality scoring, etc.) slot between `run_structure` and the COMPLETED signal without changing the caller contract.

1. **Write the task coroutine** in `app/pipeline/tasks.py`:
   ```python
   async def run_quality_check(ctx: dict, job_id: str) -> None:
       pool = ctx["pool"]
       settings = ctx["settings"]
       started_at = datetime.now(timezone.utc)

       repo = ProcessingRepository(pool)
       job = await repo.get_job(job_id)
       await repo.update_status(job_id, "IN_PROGRESS", current_stage="QUALITY_CHECK")

       try:
           # ... your logic ...
           await _store_stage(pool, job_id, "QUALITY_CHECK", status="SUCCESS", result={...}, started_at=started_at)
           await _enqueue_next(settings, "run_structure", job_id)
       except Exception as exc:
           await _store_stage(pool, job_id, "QUALITY_CHECK", status="FAILED", error=str(exc), started_at=started_at)
           await _notify_failure(pool, settings, job_id, "QUALITY_CHECK", str(exc), job)
           raise
   ```

2. **Register it** in `app/pipeline/worker.py`:
   ```python
   from app.pipeline.tasks import run_quality_check

   class WorkerSettings:
       functions = [
           run_vision,
           run_quality_check,   # ← add here
           run_structure,
           task_deliver_webhook,
       ]
   ```

3. **Update the handoff** — in `run_vision`, change:
   ```python
   await _enqueue_next(settings, "run_structure", job_id)
   # to:
   await _enqueue_next(settings, "run_quality_check", job_id)
   ```

4. **No caller contract changes.** The caller still fires `POST /v1/process` and still receives COMPLETED with the full result.

---

## Deployment Notes

### Two containers, one image

The `api` and `worker` services use the same Docker image. The difference is the command:
- `api`: `./docker-entrypoint.sh` → runs `alembic upgrade head` then `uvicorn`
- `worker`: `python -m app.pipeline.worker` → runs the ARQ worker

A single `docker compose up --build` updates both.

### Migrations run automatically

`docker-entrypoint.sh` runs `alembic upgrade head` before starting the API. If migrations fail, the container exits rather than starting with a stale schema.

Only the `api` container runs migrations. The `worker` container starts after the DB healthcheck passes — migrations are already applied.

### Scaling the worker

To process more jobs in parallel, run multiple worker replicas:

```bash
docker compose up --scale worker=3 -d
```

ARQ uses Redis as the coordination layer — multiple workers safely dequeue different jobs with no duplication risk.

### Logs

All log lines are JSON (structlog). Every request includes `request_id`. Every pipeline task binds `job_id` and `stage` to the log context — all lines from one job are trivially searchable:

```bash
docker compose logs worker | grep '"job_id": "your-uuid"'
```

In production, pipe logs to a collector (Datadog, CloudWatch, Loki) and search by `job_id` to trace any extraction end-to-end.

---

## Verification Checklist

After deployment run these in order:

```bash
# 1. Liveness
curl http://localhost:8002/health
# Expected: {"status": "ok"}

# 2. Readiness (DB + Redis must both be ok)
curl http://localhost:8002/health/ready
# Expected: {"status": "ready", "database": "ok", "redis": "ok"}

# 3. Embedding endpoint
curl -X POST http://localhost:8002/v1/embed \
  -H "X-Internal-Secret: $INTERNAL_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"text": "John Doe National ID 12345678"}'
# Expected: {"embedding": [...], "dims": 768, "model": "nomic-embed-text"}

# 4. Auth guard works (wrong secret → 403)
curl -X POST http://localhost:8002/v1/embed \
  -H "X-Internal-Secret: wrong-secret" \
  -H "Content-Type: application/json" \
  -d '{"text": "test"}'
# Expected: 403 Forbidden

# 5. Migrations applied correctly — all 4 tables present
docker compose exec db psql -U docai -d citizen-link-docai -c "\dt"
# Expected: processing_jobs, processing_stages, stage_conversations, webhook_deliveries

# 6. Submit a test job and trace it through the pipeline
curl -X POST http://localhost:8002/v1/process \
  -H "X-Internal-Secret: $INTERNAL_SECRET" \
  -H "X-User-Id: test" \
  -H "Content-Type: application/json" \
  -d '{"case_number":"TEST-001","image_urls":["https://..."],"webhook_url":"http://..."}'

# Then watch it flow:
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT status, current_stage, updated_at FROM processing_jobs ORDER BY created_at DESC LIMIT 1;"

docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT stage, status, error, usage->>'total_input_tokens' AS tokens
      FROM processing_stages ORDER BY created_at DESC;"

docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT round, page, success FROM stage_conversations ORDER BY created_at DESC;"

docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT stage, delivered, response_status, attempt_count
      FROM webhook_deliveries ORDER BY created_at DESC;"
```

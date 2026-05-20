# citizen-link-docai

> **Document AI Microservice** for the CitizenLink platform.
>
> Owns all document intelligence: vision OCR, structured field extraction, central embedding generation, and AI usage logging. NestJS fires a single HTTP call and receives webhook callbacks as each pipeline stage completes.

---

## Table of Contents

- [Why This Service Exists](#why-this-service-exists)
- [Architecture Overview](#architecture-overview)
- [Agent Correction Loop](#agent-correction-loop)
- [Pipeline Stages](#pipeline-stages)
- [API Endpoints](#api-endpoints)
- [Webhook Payload (docai → NestJS)](#webhook-payload-docai--nestjs)
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

Before this service, NestJS owned:
- Vision OCR (BullMQ `case-vision-extraction` queue)
- Text/structure extraction (BullMQ `case-text-extraction` queue)
- Embedding generation
- AI usage logging
- Post-processing (BullMQ `case-post-processing` queue)

This created tight coupling between business logic and AI infrastructure. Every model change, every prompt tweak, every embedding model swap required a NestJS deploy.

**citizen-link-docai extracts all of that into a fully autonomous service.** NestJS now makes two calls:

| Call | Direction | Purpose |
|---|---|---|
| `POST /v1/process` | NestJS → docai | Fire an extraction job (202, async) |
| `POST /api/webhooks/docai/progress` | docai → NestJS | Stage callbacks with results |
| `POST /v1/embed` | citizen-link-ai → docai | Synchronous embedding for RAG |

**Benefits:**
- AI model changes → update one env var, redeploy docai, zero NestJS changes
- Prompt changes → edit one file in docai, zero NestJS changes
- Scale AI processing independently from the NestJS API server
- Full usage audit trail — cost, latency, token counts per model call
- Agent correction loops — bad LLM output is auto-corrected before NestJS ever sees it

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
     │                                            │  3. run_embedding│
     │                                            │  4. run_post_proc│
     │                                            └────────┬────────┘
     │                                                     │
     │◄── POST /api/webhooks/docai/progress ───────────────┘
     │    (one call per stage + final COMPLETED)
     │
     └── (citizen-link-ai)
         POST /v1/embed ──────────────────────────► citizen-link-docai
         (synchronous, for RAG indexing + retrieval)
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| **ARQ** (not BullMQ) | Async-native Python (asyncio coroutines). No BullMQ Python port needed. Internal queue — no cross-service sharing. |
| **asyncpg + raw SQL** | Fast, explicit, consistent with citizen-link-ai. Alembic handles migrations. ORM adds no value for 4 tables. |
| **Webhook delivery via ARQ** | `deliver_webhook` is itself an ARQ task. ARQ retries it automatically on failure. Every attempt logged to `webhook_deliveries`. Manual retry = re-enqueue the task. |
| **Own PostgreSQL** | Fully autonomous — no shared schema with NestJS. Opaque references (NestJS IDs stored as TEXT, no foreign keys). |
| **Pre-signed S3 URLs** | NestJS generates the URLs before calling docai. docai downloads via plain httpx — no S3 SDK needed here. NestJS retains full S3 ownership. |
| **Agent correction loop** | Invalid LLM output is corrected up to `MAX_AGENT_ITERATIONS` rounds before failing. NestJS always receives schema-valid data. |

---

## Agent Correction Loop

Both `VisionAgent` and `StructureAgent` follow the same agentic pattern:

```
Call LLM
    │
    ▼
Validate output against schema
    │
    ├── Valid ──────────────────────────────────► return result
    │
    └── Invalid / missing fields
            │
            ▼
        Build correction prompt:
        "Your previous output was missing these fields: X, Y.
         Here is what you returned: {...}
         Please correct and return the full schema."
            │
            ▼
        Call LLM again (round 2, 3 ...)
            │
            └── After MAX_AGENT_ITERATIONS failures → raise error → stage FAILED
                NestJS receives webhook: { stage: "FAILED", failedAt: "VISION", reason: "..." }
```

This means NestJS never receives malformed JSON, wrong confidence ranges, invalid enum values, or missing required fields — the correction loop handles it internally.

---

## Pipeline Stages

```
POST /v1/process received
         │
         ▼
  Create processing_job (PENDING)
  Enqueue: run_vision
         │
         ▼
[ARQ Worker] run_vision
  → VisionAgent: download images → call LLM → validate → auto-correct (max N rounds)
  → store extraction_results (stage=VISION)
  → log ai_usage_logs (one row per LLM call)
  → enqueue: task_deliver_webhook { stage: VISION, status: completed }
  → enqueue: run_structure
         │
         ▼
[ARQ Worker] run_structure
  → StructureAgent: vision output → call LLM → validate → auto-correct (max N rounds)
  → store extraction_results (stage=TEXT)
  → log ai_usage_logs
  → enqueue: task_deliver_webhook { stage: TEXT, status: completed, result: {...} }
  → enqueue: run_embedding
         │
         ▼
[ARQ Worker] run_embedding
  → EmbeddingService: structured text → vector
  → store extraction_results (stage=EMBEDDING)
  → log ai_usage_logs
  → enqueue: task_deliver_webhook { stage: EMBEDDING, status: completed }
  → enqueue: run_post_processing
         │
         ▼
[ARQ Worker] run_post_processing
  → Compile final result from all stage outputs
  → Update processing_job status: COMPLETED
  → enqueue: task_deliver_webhook { stage: COMPLETED, result: { fields, embedding, ocrConfidence, ... } }

[ARQ Worker] task_deliver_webhook   ← retries up to WEBHOOK_MAX_RETRIES times
  → POST to NESTJS_WEBHOOK_URL with X-Internal-Secret header
  → Log attempt to webhook_deliveries (delivered, response_status, attempt_count)
  → On failure → ARQ retries with backoff
```

**On any stage failure:**
```
  → Update processing_job status: FAILED
  → enqueue: task_deliver_webhook { stage: FAILED, failedAt: "VISION|TEXT|...", reason: "..." }
```

---

## API Endpoints

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `GET` | `/health` | None | Liveness — always 200 if the process is alive |
| `GET` | `/health/ready` | None | Readiness — checks DB (`SELECT 1`) + Redis (`PING`) |
| `POST` | `/v1/process` | `X-Internal-Secret` + `X-User-Id` | Submit an extraction job (NestJS → docai) |
| `GET` | `/v1/jobs/{jobId}` | `X-Internal-Secret` + `X-User-Id` | Poll job status (optional — webhooks are primary) |
| `POST` | `/v1/embed` | `X-Internal-Secret` | Generate an embedding vector (citizen-link-ai or NestJS) |
| `GET` | `/docs` | None | Swagger UI (disable in production) |

### POST /v1/process

**Request:**
```json
{
  "external_case_id": "nestjs-case-uuid",
  "external_document_id": "nestjs-document-uuid",
  "external_extraction_id": "nestjs-extraction-uuid",
  "external_user_id": "nestjs-user-uuid",
  "case_type": "FOUND",
  "case_number": "CL-2025-00042",
  "image_urls": [
    "https://s3.amazonaws.com/bucket/doc-page1.jpg?X-Amz-Signature=...",
    "https://s3.amazonaws.com/bucket/doc-page2.jpg?X-Amz-Signature=..."
  ],
  "webhook_url": "http://nestjs-server:2000/api/webhooks/docai/progress"
}
```

**Response (202):**
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "PENDING"
}
```

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
  "embedding": [0.0234, -0.0891, ...],
  "dims": 768,
  "model": "nomic-embed-text"
}
```

---

## Webhook Payload (docai → NestJS)

docai POSTs to `NESTJS_WEBHOOK_URL` with `X-Internal-Secret: <NESTJS_INTERNAL_SECRET>` on every stage transition.

```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "externalCaseId": "nestjs-case-uuid",
  "externalExtractionId": "nestjs-extraction-uuid",
  "stage": "VISION | TEXT | EMBEDDING | COMPLETED | FAILED",
  "status": "completed | failed",
  "result": { },
  "timestamp": "2025-01-01T12:00:00.000Z"
}
```

**Stage-specific result payloads:**

| Stage | `result` content |
|---|---|
| `VISION` | `{}` (intermediate — no data forwarded) |
| `TEXT` | Full structure extraction output (document fields, confidence, warnings) |
| `EMBEDDING` | `{}` (intermediate) |
| `COMPLETED` | `{ fields, embedding: { vector, dims, model }, ocrConfidence, extractionConfidence }` |
| `FAILED` | `{ failedAt: "VISION|TEXT|...", reason: "error message" }` |

**NestJS validates the `X-Internal-Secret` header on every incoming webhook.**

---

## Database Schema

docai has its own PostgreSQL database — completely separate from NestJS. NestJS IDs are stored as plain `TEXT` (no foreign keys to the NestJS DB).

```sql
-- Job tracking
processing_jobs (
  id                     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  external_case_id       TEXT        NOT NULL,   -- NestJS DocumentCase.id
  external_document_id   TEXT        NOT NULL,   -- NestJS Document.id
  external_extraction_id TEXT        NOT NULL,   -- NestJS AIExtraction.id
  external_user_id       TEXT        NOT NULL,   -- NestJS User.id
  case_type              TEXT        NOT NULL,   -- 'LOST' | 'FOUND'
  case_number            TEXT        NOT NULL,
  image_urls             TEXT[]      NOT NULL,   -- Pre-signed S3 URLs
  webhook_url            TEXT        NOT NULL,   -- NestJS callback URL
  status                 TEXT        NOT NULL DEFAULT 'PENDING',
  current_stage          TEXT,                   -- 'VISION' | 'TEXT' | 'EMBEDDING' | 'POST_PROCESSING'
  error_message          TEXT,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- Per-stage AI outputs
extraction_results (
  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id     UUID        NOT NULL REFERENCES processing_jobs(id) ON DELETE CASCADE,
  stage      TEXT        NOT NULL,    -- 'VISION' | 'TEXT' | 'EMBEDDING'
  result     JSONB       NOT NULL,
  confidence FLOAT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- Every model call logged
ai_usage_logs (
  id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id              UUID        REFERENCES processing_jobs(id) ON DELETE SET NULL,
  stage               TEXT,
  model               TEXT        NOT NULL,
  provider            TEXT        NOT NULL,    -- 'ollama' | 'openai'
  input_tokens        INT,
  output_tokens       INT,
  estimated_cost_usd  FLOAT,
  latency_ms          INT         NOT NULL,
  success             BOOLEAN     NOT NULL DEFAULT TRUE,
  error_message       TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- Webhook delivery audit
webhook_deliveries (
  id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id          UUID        NOT NULL REFERENCES processing_jobs(id) ON DELETE CASCADE,
  stage           TEXT        NOT NULL,
  payload         JSONB       NOT NULL,
  nestjs_url      TEXT        NOT NULL,
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
├── main.py                         # FastAPI app — lifespan, middleware, routers
├── requirements.txt
├── Dockerfile                      # Multi-stage python:3.11-slim build
├── docker-compose.yml              # api + worker + postgres + redis
├── docker-entrypoint.sh            # Validates env, runs alembic upgrade head, starts uvicorn
├── .env.example                    # All environment variables documented
│
├── alembic/                        # Independent DB migration history
│   ├── env.py                      # Reads DATABASE_URL from env, no ORM
│   ├── alembic.ini
│   └── versions/
│       └── 001_initial_schema.py   # All 4 tables (raw SQL via op.execute)
│
└── app/
    ├── config.py                   # pydantic-settings — all env vars, one source of truth
    ├── database.py                 # asyncpg pool creation + teardown
    ├── dependencies.py             # require_internal_auth / require_service_auth / get_pool
    ├── middleware.py               # RequestIDMiddleware — stamps X-Request-ID on every request
    ├── exceptions.py               # AppError hierarchy + FastAPI exception handlers
    ├── health.py                   # GET /health (liveness) + GET /health/ready (DB + Redis)
    │
    ├── processing/                 # Job intake
    │   ├── schemas.py              # ProcessRequest, JobStatusResponse
    │   ├── repository.py           # Raw SQL on processing_jobs
    │   ├── service.py              # Create job + enqueue run_vision to ARQ
    │   └── router.py               # POST /v1/process, GET /v1/jobs/{id}
    │
    ├── embedding/                  # Central embedding endpoint
    │   ├── schemas.py              # EmbedRequest, EmbedResponse
    │   ├── service.py              # EmbeddingService — wraps AsyncOpenAI (Ollama + OpenAI)
    │   └── router.py               # POST /v1/embed
    │
    ├── agents/                     # Agentic LLM processors with correction loop
    │   ├── vision_agent.py         # Image URLs → validated OCR output (max N rounds)
    │   └── structure_agent.py      # OCR output → validated document fields (max N rounds)
    │
    ├── pipeline/                   # Internal async job queue
    │   ├── tasks.py                # ARQ coroutines: run_vision, run_structure,
    │   │                           #   run_embedding, run_post_processing, task_deliver_webhook
    │   ├── worker.py               # WorkerSettings — registers tasks, startup/shutdown hooks
    │   └── webhook.py              # HTTP delivery to NestJS + webhook_deliveries logging
    │
    └── usage/                      # AI usage logging
        ├── repository.py           # Raw INSERT into ai_usage_logs
        └── service.py              # Batch logging, cost estimation, never breaks pipeline
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in all required values.

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | ✅ | — | PostgreSQL connection string for docai's own DB |
| `INTERNAL_SECRET` | ✅ | — | Shared secret — NestJS sends this in `X-Internal-Secret` to authenticate calls |
| `NESTJS_WEBHOOK_URL` | ✅ | — | NestJS URL docai POSTs stage callbacks to |
| `NESTJS_INTERNAL_SECRET` | ✅ | — | Secret docai sends to NestJS on every webhook call |
| `REDIS_URL` | ✅ | `redis://localhost:6379` | Redis connection string for ARQ queue |
| `VISION_AI_BASE_URL` | ✅ | `http://localhost:11434/v1` | OpenAI-compatible base URL for vision model |
| `VISION_AI_API_KEY` | ✅ | `ollama` | API key for vision model (`ollama` for local) |
| `VISION_AI_MODEL` | ✅ | `gemma3:4b` | Vision model name |
| `STRUCTURE_AI_BASE_URL` | ✅ | `http://localhost:11434/v1` | Base URL for structure/text extraction model |
| `STRUCTURE_AI_API_KEY` | ✅ | `ollama` | API key for structure model |
| `STRUCTURE_AI_MODEL` | ✅ | `gemma3:4b` | Structure model name |
| `EMBEDDING_BASE_URL` | ✅ | `http://localhost:11434/v1` | Base URL for embedding model |
| `EMBEDDING_API_KEY` | ✅ | `ollama` | API key for embedding model |
| `EMBEDDING_MODEL` | ✅ | `nomic-embed-text` | Embedding model name |
| `EMBEDDING_IS_OPENAI` | — | `false` | Set `true` if using OpenAI `text-embedding-*` (skips nomic prefix) |
| `MAX_AGENT_ITERATIONS` | — | `3` | Max correction rounds before a stage fails |
| `WEBHOOK_MAX_RETRIES` | — | `3` | Max ARQ retry attempts for webhook delivery |
| `LOG_LEVEL` | — | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

> **Generating a secret:**
> ```bash
> openssl rand -hex 32
> ```
> Use the same value for `INTERNAL_SECRET` here and `DOCAI_SERVICE_INTERNAL_SECRET` in NestJS.

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
NESTJS_WEBHOOK_URL=http://localhost:2000/api/webhooks/docai/progress
NESTJS_INTERNAL_SECRET=same-as-nestjs-internal-secret

VISION_AI_BASE_URL=http://localhost:11434/v1
STRUCTURE_AI_BASE_URL=http://localhost:11434/v1
EMBEDDING_BASE_URL=http://localhost:11434/v1
```

### 3. Create the database

```bash
createdb citizen-link-docai
```

### 4. Run migrations

```bash
alembic upgrade head
```

### 5. Start the API server

```bash
uvicorn main:app --reload --port 8002
```

### 6. Start the ARQ worker (separate terminal)

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
# Edit .env — generate INTERNAL_SECRET, set NESTJS_WEBHOOK_URL, etc.
```

### Start all services

```bash
docker compose up --build -d
```

This starts four containers:
- `api` — FastAPI server on port 8002 (runs `alembic upgrade head` before starting)
- `worker` — ARQ worker (polls Redis for jobs, no HTTP port)
- `db` — PostgreSQL with pgvector
- `redis` — Redis for ARQ queue

### View logs

```bash
# All services
docker compose logs -f

# API server only
docker compose logs -f api

# Worker only
docker compose logs -f worker
```

### Stop

```bash
docker compose down
# To also delete data volumes:
docker compose down -v
```

### Rebuild after code changes

```bash
docker compose up --build -d
```

### Ollama on the host machine

If Ollama runs on the host (not in Docker), the containers reach it via `host.docker.internal:11434`. This is already configured in `.env.example` and the `extra_hosts` directive in `docker-compose.yml` maps it correctly on Linux.

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
# Liveness — is the process alive?
curl http://localhost:8002/health
# → {"status": "ok"}

# Readiness — are DB and Redis reachable?
curl http://localhost:8002/health/ready
# → {"status": "ready", "database": "ok", "redis": "ok"}
# → {"status": "not_ready", "database": "error", "redis": "ok"}  (503 if any fails)
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
    "external_case_id": "case-uuid",
    "external_document_id": "doc-uuid",
    "external_extraction_id": "extraction-uuid",
    "external_user_id": "user-uuid",
    "case_type": "FOUND",
    "case_number": "CL-2025-00001",
    "image_urls": ["https://your-presigned-s3-url.com/image.jpg"],
    "webhook_url": "http://localhost:2000/api/webhooks/docai/progress"
  }'

# → {"jobId": "uuid", "status": "PENDING"}
```

### Polling job status

```bash
curl http://localhost:8002/v1/jobs/<jobId> \
  -H "X-Internal-Secret: your-secret" \
  -H "X-User-Id: test-user-id"
```

### Inspecting usage logs

```bash
# Docker
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT stage, model, latency_ms, estimated_cost_usd, success FROM ai_usage_logs ORDER BY created_at DESC LIMIT 20;"

# Local
psql citizen-link-docai \
  -c "SELECT stage, model, latency_ms, estimated_cost_usd FROM ai_usage_logs;"
```

### Inspecting webhook deliveries

```bash
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT stage, delivered, response_status, attempt_count FROM webhook_deliveries ORDER BY created_at DESC;"
```

### Inspecting job status

```bash
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT id, status, current_stage, case_number, created_at FROM processing_jobs ORDER BY created_at DESC LIMIT 10;"
```

### Running a new Alembic migration

```bash
# 1. Create the migration file
alembic revision -m "add_some_column"

# 2. Edit the generated file in alembic/versions/ — use op.execute() for raw SQL
# 3. Apply it
alembic upgrade head

# 4. Roll back one step if needed
alembic downgrade -1
```

---

## Adding a New Pipeline Stage

To add a stage between existing ones (e.g., an image quality check before vision):

1. **Write the task coroutine** in `app/pipeline/tasks.py`:
   ```python
   async def run_quality_check(ctx: dict, job_id: str) -> None:
       pool = ctx["pool"]
       settings = ctx["settings"]
       # ... your logic ...
       await _enqueue_next(settings, "run_vision", job_id)
   ```

2. **Register it** in `app/pipeline/worker.py`:
   ```python
   from app.pipeline.tasks import run_quality_check

   class WorkerSettings:
       functions = [
           run_quality_check,   # ← add here
           run_vision,
           ...
       ]
   ```

3. **Update the handoff** — in `run_vision`, the processing service currently enqueues `run_vision` as the first stage. Change `app/processing/service.py` to enqueue `run_quality_check` instead.

4. **No API changes needed.** NestJS fires `POST /v1/process` exactly the same way. If you want NestJS to receive a webhook for your new stage, call `_enqueue_webhook(stage="QUALITY_CHECK", ...)` from inside the task.

---

## Deployment Notes

### Two containers, one image

The `api` and `worker` services use the same Docker image. The difference is the command:
- `api`: `./docker-entrypoint.sh` → runs alembic then uvicorn
- `worker`: `python -m app.pipeline.worker` → runs ARQ worker

This means a single `docker compose up --build` updates both.

### Migrations run automatically

`docker-entrypoint.sh` runs `alembic upgrade head` before starting the API. If migrations fail, the container exits rather than starting with a stale schema.

Only the `api` container runs migrations. The `worker` container starts immediately after db healthcheck passes — migrations are already applied by the time jobs start being processed.

### Scaling the worker

To process more jobs in parallel, run multiple worker replicas:

```bash
docker compose up --scale worker=3 -d
```

ARQ uses Redis as the coordination layer — multiple workers safely dequeue different jobs. There is no job duplication risk.

### Logs

All log lines are JSON (structlog). Every request includes `request_id`. Every pipeline task binds `job_id` and `stage` to the log context so all lines from one job are trivially searchable:

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
# Expected: {"error": {"code": "FORBIDDEN", "message": "Forbidden"}}

# 5. Migrations applied correctly
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "\dt"
# Expected: processing_jobs, extraction_results, ai_usage_logs, webhook_deliveries

# 6. Full extraction job (requires NestJS integration)
#    Submit a FOUND case in the app → watch AIExtraction.status update:
#    PENDING → IN_PROGRESS → COMPLETED
#    Watch usage logs:
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT stage, model, latency_ms FROM ai_usage_logs ORDER BY created_at;"
#    Watch webhook deliveries:
docker compose exec db psql -U docai -d citizen-link-docai \
  -c "SELECT stage, delivered, response_status FROM webhook_deliveries ORDER BY created_at;"
```

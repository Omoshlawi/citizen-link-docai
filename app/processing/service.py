"""
ProcessingService — validates requests, persists jobs, enqueues pipelines.

One submit_* method per pipeline type. Each method is responsible for:
  1. Building the pipeline-specific input dict from the typed request
  2. Persisting the job as PENDING (generic internal model)
  3. Enqueuing run_stage for the first stage
  4. Returning the job UUID

The registry is the single source of truth for stage ordering — no stage
name is hard-coded here.
"""

import structlog
from arq import create_pool as arq_create_pool
from arq.connections import RedisSettings

from app.config import Settings
from app.pipeline.registry import get_pipeline
from app.processing.repository import ProcessingRepository
from app.processing.schemas import ExtractionRequest

log = structlog.get_logger(__name__)


class ProcessingService:
    def __init__(self, pool, settings: Settings) -> None:
        self._repo = ProcessingRepository(pool)
        self._settings = settings

    # ── Public submit methods (one per pipeline) ───────────────────────────────

    async def submit_extraction(self, request: ExtractionRequest) -> str:
        """
        Enqueue an OCR + structured field extraction job.

        Returns the job UUID — NestJS stores this on AIExtraction.docaiJobId
        so incoming webhooks (which carry only jobId) can be looked up.
        """
        job_input = {
            "case_number": request.case_number,
            "image_urls": request.image_urls,
        }
        return await self._enqueue("EXTRACTION", job_input, request.webhook_url, request.priority)

    # Future pipelines slot in here without touching the worker or registry:
    #
    # async def submit_fraud_check(self, request: FraudCheckRequest) -> str:
    #     job_input = {"document_id": request.document_id, ...}
    #     return await self._enqueue("FRAUD_DETECTION", job_input, request.webhook_url, request.priority)
    #
    # async def submit_match_verification(self, request: MatchVerificationRequest) -> str:
    #     job_input = {"claim_id": request.claim_id, "candidate_ids": request.candidate_ids}
    #     return await self._enqueue("MATCH_VERIFICATION", job_input, request.webhook_url, request.priority)

    # ── Internal helper ────────────────────────────────────────────────────────

    async def _enqueue(
        self,
        job_type: str,
        job_input: dict,
        webhook_url: str,
        priority: int,
    ) -> str:
        """
        Persist the job and enqueue the first stage via ARQ.

        Validates the job_type against the registry before writing to the DB —
        an unknown type raises ValueError immediately (before any DB write).
        """
        pipeline = get_pipeline(job_type)  # fast-fail for unknown job_type
        first_stage = pipeline.stages[0]

        job_id = await self._repo.create_job(
            job_type=job_type,
            input=job_input,
            webhook_url=webhook_url,
            priority=priority,
        )

        log.info(
            "job_created",
            job_id=job_id,
            job_type=job_type,
            first_stage=first_stage,
            priority=priority,
        )

        redis_settings = RedisSettings.from_dsn(self._settings.redis_url)
        arq_pool = await arq_create_pool(redis_settings)
        await arq_pool.enqueue_job("run_stage", job_id, first_stage)
        await arq_pool.close()

        log.info("pipeline_enqueued", job_id=job_id, stage=first_stage)
        return job_id

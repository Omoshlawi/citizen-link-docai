"""
ProcessingService — validates the request, persists the job, enqueues the pipeline.

The service is the only place that touches both the repository and the ARQ queue.
It keeps the router thin and the logic testable.
"""

import structlog
from arq import create_pool as arq_create_pool
from arq.connections import RedisSettings

from app.config import Settings
from app.processing.repository import ProcessingRepository
from app.processing.schemas import ProcessRequest

log = structlog.get_logger(__name__)


class ProcessingService:
    def __init__(self, pool, settings: Settings) -> None:
        self._repo = ProcessingRepository(pool)
        self._settings = settings

    async def submit_job(self, request: ProcessRequest) -> str:
        """
        1. Persist the job as PENDING.
        2. Enqueue run_vision as the first ARQ pipeline stage.
        3. Return the job UUID so NestJS can poll /v1/jobs/{id} if needed.
        """
        job_id = await self._repo.create_job(
            case_number=request.case_number,
            image_urls=request.image_urls,
            webhook_url=request.webhook_url,
        )

        log.info("job_created", job_id=job_id, case_number=request.case_number)

        # Enqueue the first pipeline stage
        redis_settings = RedisSettings.from_dsn(self._settings.redis_url)
        arq_pool = await arq_create_pool(redis_settings)
        await arq_pool.enqueue_job("run_vision", job_id)
        await arq_pool.close()

        log.info("pipeline_enqueued", job_id=job_id, stage="VISION")
        return job_id

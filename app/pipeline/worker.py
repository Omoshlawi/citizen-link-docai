"""
ARQ worker entry point.

Run with:
  python -m app.pipeline.worker

ARQ reads WorkerSettings from this module. On startup it:
  1. Opens the asyncpg connection pool (stored in ctx so all tasks share it)
  2. Registers all task coroutines
  3. Polls Redis for jobs and dispatches them to the appropriate coroutine

The worker process is separate from the API server — both run in their own
containers (see docker-compose.yml).
"""

from arq.connections import RedisSettings

from app.config import get_settings
from app.database import create_pool, close_pool
from app.pipeline.tasks import (
    run_stage,
    task_deliver_webhook,
)


async def startup(ctx: dict) -> None:
    """
    Called once when the worker process starts.
    Opens the asyncpg pool and stores it in ctx so all task coroutines can access it.
    """
    settings = get_settings()
    ctx["settings"] = settings
    ctx["pool"] = await create_pool(settings)


async def shutdown(ctx: dict) -> None:
    """
    Called once when the worker process stops.
    Closes the asyncpg pool gracefully.
    """
    await close_pool(ctx["pool"])


settings = get_settings()


class WorkerSettings:
    """
    ARQ WorkerSettings — read by `arq` when starting the worker.

    functions: list of coroutines that ARQ can dispatch jobs to.
    redis_settings: connection config for the ARQ Redis instance.
    on_startup / on_shutdown: lifecycle hooks for shared resources.
    max_tries: how many times ARQ retries a failed task (deliver_webhook uses this).
    """

    functions = [
        run_stage,
        task_deliver_webhook,
    ]

    redis_settings = RedisSettings.from_dsn(settings.redis_url)

    on_startup = startup
    on_shutdown = shutdown

    # Retry failed jobs up to webhook_max_retries times (applies to all tasks).
    # For pipeline stages we want fast failure — consider overriding per-task
    # once ARQ supports per-function retry config.
    max_tries = settings.webhook_max_retries

    # How long (seconds) ARQ keeps job results in Redis before expiry
    keep_result = 3600

    # Queue poll interval — 0.5s is a good balance between latency and CPU
    poll_delay = 0.5


if __name__ == "__main__":
    import asyncio
    from arq import run_worker

    run_worker(WorkerSettings)

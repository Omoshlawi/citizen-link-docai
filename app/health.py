"""
Health check endpoints.

GET /health       — liveness probe  (is the process alive?)
GET /health/ready — readiness probe (checks DB + Redis)
"""

import asyncpg
import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from app.dependencies import get_pool

log = structlog.get_logger(__name__)
router = APIRouter(tags=["health"])


@router.get("/health")
async def liveness():
    """Process is alive. Always returns 200 if the server is running."""
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(pool: asyncpg.Pool = Depends(get_pool)):
    """
    Readiness check — verifies DB and Redis connectivity.
    Returns 200 only when both are healthy.
    """
    checks = {"database": "error", "redis": "error"}
    healthy = True

    # Check database
    try:
        await pool.fetchval("SELECT 1")
        checks["database"] = "ok"
    except Exception as exc:
        log.error("readiness_db_check_failed", error=str(exc))
        healthy = False

    # Check Redis via ARQ pool
    try:
        import redis.asyncio as aioredis
        from app.config import get_settings
        settings = get_settings()
        async with aioredis.from_url(settings.redis_url) as r:
            await r.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        log.error("readiness_redis_check_failed", error=str(exc))
        healthy = False

    status = "ready" if healthy else "not_ready"
    code = 200 if healthy else 503
    return JSONResponse(status_code=code, content={"status": status, **checks})

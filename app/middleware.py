"""
Request-level middleware.

RequestIDMiddleware: stamps every request with a UUID and binds it to
the structlog context. All log lines within a request automatically include
request_id — makes distributed tracing trivial.
"""

import time
import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

log = structlog.get_logger(__name__)


class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())

        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
        )

        start = time.perf_counter()
        log.info("request_started")

        response = await call_next(request)

        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        log.info("request_completed", status_code=response.status_code, duration_ms=duration_ms)

        response.headers["X-Request-ID"] = request_id
        return response

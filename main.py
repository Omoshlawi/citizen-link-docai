"""
CitizenLink Document AI Service — FastAPI application entry point.

Startup:
  1. Configure structured logging
  2. Open asyncpg connection pool
  3. Register routes and middleware

Shutdown:
  1. Close connection pool gracefully

Run locally:
  uvicorn main:app --reload --port 8002

Worker (separate process):
  python -m app.pipeline.worker
"""

import logging
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.database import close_pool, create_pool
from app.exceptions import AppError, app_error_handler, generic_error_handler
from app.middleware import RequestIDMiddleware

# ── Import routers ─────────────────────────────────────────────────────────────
from app.health import router as health_router
from app.processing.router import router as processing_router
from app.embedding.router import router as embedding_router


def configure_logging(log_level: str) -> None:
    """
    Configure structlog to output JSON lines.

    In development you'll see nicely formatted JSON.
    In production a log aggregator (Datadog, CloudWatch) can parse these lines automatically.
    """
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, log_level.upper(), logging.INFO),
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.dev.ConsoleRenderer() if log_level == "DEBUG" else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan — runs setup before the server accepts requests,
    and teardown after it stops.
    """
    settings = get_settings()
    configure_logging(settings.log_level)

    # Startup: open DB pool and store it in app.state so routes can access it
    app.state.pool = await create_pool(settings)
    app.state.settings = settings

    yield  # ← server is live, handling requests

    # Shutdown: close DB pool cleanly
    await close_pool(app.state.pool)


# ── Create app ─────────────────────────────────────────────────────────────────
app = FastAPI(
    title="CitizenLink Document AI Service",
    description="Document processing microservice — vision extraction, structure extraction, embedding, and AI usage logging",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url=None,
)

# ── Middleware ─────────────────────────────────────────────────────────────────
app.add_middleware(RequestIDMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Internal service only — NestJS and citizen-link-ai are the callers
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Exception handlers ─────────────────────────────────────────────────────────
app.add_exception_handler(AppError, app_error_handler)
app.add_exception_handler(Exception, generic_error_handler)

# ── Routes ─────────────────────────────────────────────────────────────────────
app.include_router(health_router)
app.include_router(processing_router, prefix="/v1")
app.include_router(embedding_router, prefix="/v1")

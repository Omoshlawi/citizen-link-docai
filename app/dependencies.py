"""
Shared FastAPI dependencies.

require_internal_auth — validates X-Internal-Secret and extracts X-User-Id.
require_service_auth  — validates X-Internal-Secret only (no user context, e.g. /v1/embed).
get_pool              — returns the asyncpg connection pool.
get_settings          — returns the Settings singleton.
"""

import asyncpg
import structlog
from fastapi import Depends, Header, Request

from app.config import Settings, get_settings
from app.exceptions import AuthError

log = structlog.get_logger(__name__)


def get_pool(request: Request) -> asyncpg.Pool:
    """Return the connection pool stored in app.state at startup."""
    return request.app.state.pool


async def require_internal_auth(
    x_internal_secret: str = Header(alias="X-Internal-Secret"),
    x_user_id: str = Header(alias="X-User-Id"),
    settings: Settings = Depends(get_settings),
) -> str:
    """
    Validates the internal secret and returns the user_id.
    Used for endpoints called by NestJS on behalf of a user.
    """
    if x_internal_secret != settings.internal_secret:
        log.warning("invalid_internal_secret_attempt")
        raise AuthError()

    structlog.contextvars.bind_contextvars(user_id=x_user_id)
    return x_user_id


async def require_service_auth(
    x_internal_secret: str = Header(alias="X-Internal-Secret"),
    settings: Settings = Depends(get_settings),
) -> None:
    """
    Validates the internal secret only — no user context.
    Used for service-to-service endpoints like /v1/embed (called by citizen-link-ai).
    """
    if x_internal_secret != settings.internal_secret:
        log.warning("invalid_internal_secret_attempt")
        raise AuthError()

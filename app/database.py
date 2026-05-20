"""
Database connection pool — asyncpg.

citizen-link-docai has its OWN PostgreSQL database.
Schema is managed by Alembic migrations in alembic/versions/.
Raw SQL throughout — no ORM. Easy to read, easy to debug in psql.

auto-creates the database on first run if it does not exist.
"""

from urllib.parse import urlparse, urlunparse

import asyncpg
import structlog

from app.config import Settings

log = structlog.get_logger(__name__)


async def ensure_database_exists(settings: Settings) -> None:
    """
    Create the target database if it does not exist.

    Connects to the default 'postgres' system database first (which always
    exists), checks for the target DB, and issues CREATE DATABASE if missing.
    This makes local dev and first-time Docker deploys self-healing — no
    manual createdb step required.
    """
    parsed = urlparse(settings.database_url)
    db_name = parsed.path.lstrip("/")

    # Build a URL pointing at the 'postgres' system database on the same host
    system_url = urlunparse(parsed._replace(path="/postgres"))

    log.info("checking_database", db_name=db_name)

    conn = await asyncpg.connect(system_url, timeout=10)
    try:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", db_name
        )
        if not exists:
            # Identifiers cannot be parameterised — safe because db_name comes
            # from our own config, not user input.
            await conn.execute(f'CREATE DATABASE "{db_name}"')
            log.info("database_created", db_name=db_name)
        else:
            log.info("database_already_exists", db_name=db_name)
    finally:
        await conn.close()


async def create_pool(settings: Settings) -> asyncpg.Pool:
    """Open the connection pool. Called once at application startup."""
    await ensure_database_exists(settings)

    log.info("connecting_to_database", url=settings.database_url.split("@")[-1])
    pool = await asyncpg.create_pool(
        settings.database_url,
        min_size=2,
        max_size=10,
        command_timeout=10,
    )
    log.info("database_pool_ready")
    return pool


async def close_pool(pool: asyncpg.Pool) -> None:
    """Close the connection pool gracefully on shutdown."""
    await pool.close()
    log.info("database_pool_closed")

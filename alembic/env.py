"""
Alembic environment for citizen-link-docai.

Reads DATABASE_URL from the environment (same .env as the app).
Uses raw SQL migrations via op.execute() — no ORM, consistent with asyncpg query layer.

Auto-creates the target database if it does not exist so that
'alembic upgrade head' works on a fresh PostgreSQL instance.
"""

import os
from logging.config import fileConfig
from urllib.parse import urlparse, urlunparse

import psycopg2
from alembic import context
from dotenv import load_dotenv
from sqlalchemy import engine_from_config, pool

# Load .env so DATABASE_URL is available when running alembic from the CLI
load_dotenv()

# ── Alembic Config object ───────────────────────────────────────────────────────
config = context.config

# Interpret the config file for Python logging (if present)
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# ── Inject DATABASE_URL from environment ───────────────────────────────────────
# Alembic uses synchronous SQLAlchemy under the hood for migration execution.
# We convert the asyncpg URL (postgresql://...) to a sync psycopg2-compatible one.
# Since alembic only runs op.execute() raw SQL, no SQLAlchemy ORM is needed.
database_url = os.environ.get("DATABASE_URL", "")
if database_url.startswith("postgresql+asyncpg://"):
    database_url = database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
elif not database_url.startswith("postgresql://"):
    raise RuntimeError(
        "DATABASE_URL must start with postgresql:// or postgresql+asyncpg://"
    )

# ── Auto-create database if missing ────────────────────────────────────────────
def _ensure_database_exists(url: str) -> None:
    """
    Connect to the 'postgres' system database and create the target DB if absent.
    Uses psycopg2 (synchronous) because Alembic's env.py is synchronous.
    """
    parsed = urlparse(url)
    db_name = parsed.path.lstrip("/")
    system_url = urlunparse(parsed._replace(path="/postgres"))

    conn = psycopg2.connect(system_url)
    conn.autocommit = True  # CREATE DATABASE cannot run inside a transaction
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        if not cur.fetchone():
            cur.execute(f'CREATE DATABASE "{db_name}"')
            print(f"[alembic] Created database: {db_name}")
        cur.close()
    finally:
        conn.close()

_ensure_database_exists(database_url)

config.set_main_option("sqlalchemy.url", database_url)

# No ORM metadata — raw SQL migrations only
target_metadata = None


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (no DB connection needed — outputs SQL)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (connects to DB and applies changes)."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

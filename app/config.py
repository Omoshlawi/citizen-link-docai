"""
Centralised configuration — reads from environment variables / .env file.

Every configurable value lives here. No os.getenv() calls elsewhere.
pydantic-settings validates types and provides defaults automatically.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── Database (own PostgreSQL — not shared with NestJS) ─────────────────────
    database_url: str

    # ── Redis (for ARQ job queue) ──────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379"

    # ── Internal Auth ──────────────────────────────────────────────────────────
    # Shared secret between NestJS and this service.
    # NestJS sends it in X-Internal-Secret header so we know the request is legit.
    internal_secret: str

    # ── Callback (webhook sent by docai after each pipeline stage) ────────────
    # URL of the caller's webhook receiver — docai POSTs stage updates here.
    callback_url: str
    # Secret docai sends on every callback so the caller can validate the request.
    callback_secret: str

    # ── Vision Model ───────────────────────────────────────────────────────────
    vision_ai_base_url: str = "http://localhost:11434/v1"
    vision_ai_api_key: str = "ollama"
    vision_ai_model: str = "gemma3:4b"

    # ── Structure/Text Extraction Model ────────────────────────────────────────
    structure_ai_base_url: str = "http://localhost:11434/v1"
    structure_ai_api_key: str = "ollama"
    structure_ai_model: str = "gemma3:4b"

    # ── Embedding Model ────────────────────────────────────────────────────────
    # Central embedding for all services — NestJS and citizen-link-ai call /v1/embed.
    embedding_base_url: str = "http://localhost:11434/v1"
    embedding_api_key: str = "ollama"
    embedding_model: str = "nomic-embed-text"
    # False → Ollama native format (/api/embeddings, nomic task prefixes)
    # True  → OpenAI-compatible (/v1/embeddings, text-embedding-3-small)
    embedding_is_openai: bool = False

    # ── Tuning ─────────────────────────────────────────────────────────────────
    # Max auto-correction rounds in vision/structure agents before giving up
    max_agent_iterations: int = 3
    # Max ARQ retry attempts for failed webhook deliveries
    webhook_max_retries: int = 3

    # ── Logging ────────────────────────────────────────────────────────────────
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """
    Returns a cached Settings instance.
    Parsed once per process — not on every request.
    """
    return Settings()

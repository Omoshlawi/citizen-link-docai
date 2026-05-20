"""
EmbeddingService — generates vector embeddings via OpenAI-compatible API.

Supports two backends:
  - nomic-embed-text (Ollama) — 768-dim, uses search_document/search_query prefix
  - OpenAI text-embedding-* — 1536-dim or 3072-dim, no prefix needed

Config via environment:
  EMBEDDING_BASE_URL, EMBEDDING_API_KEY, EMBEDDING_MODEL, EMBEDDING_IS_OPENAI
"""

import time

import structlog
from openai import AsyncOpenAI

from app.config import Settings

log = structlog.get_logger(__name__)


class EmbeddingService:
    """
    Thin async wrapper around the OpenAI-compatible embeddings API.

    Both Ollama (nomic-embed-text) and the real OpenAI API expose the same
    /v1/embeddings endpoint — AsyncOpenAI handles both with just a base_url swap.
    """

    def __init__(self, settings: Settings) -> None:
        self._model = settings.embedding_model
        self._is_openai = settings.embedding_is_openai
        self._client = AsyncOpenAI(
            base_url=settings.embedding_base_url,
            api_key=settings.embedding_api_key,
        )

    async def embed(self, text: str, use_case: str = "document") -> list[float]:
        """
        Generate an embedding vector for the given text.

        For nomic-embed-text the prompt is prefixed so the model knows whether
        this is a document being indexed or a query being searched:
          document → "search_document: <text>"
          search   → "search_query: <text>"

        OpenAI models don't need the prefix — it is omitted when EMBEDDING_IS_OPENAI=true.
        """
        if self._is_openai:
            input_text = text
        else:
            prefix = "search_query: " if use_case == "search" else "search_document: "
            input_text = prefix + text

        start = time.perf_counter()
        response = await self._client.embeddings.create(
            model=self._model,
            input=input_text,
        )
        latency_ms = round((time.perf_counter() - start) * 1000, 2)

        embedding = response.data[0].embedding
        log.info(
            "embedding_generated",
            model=self._model,
            dims=len(embedding),
            use_case=use_case,
            latency_ms=latency_ms,
        )
        return embedding

    @property
    def model(self) -> str:
        return self._model

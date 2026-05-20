"""
Embedding router — POST /v1/embed

Synchronous embedding endpoint consumed by:
  - citizen-link-ai  (RAG indexer + retriever)
  - NestJS post-processing stage (after docai pipeline — stores vector in documents table)

Authentication: X-Internal-Secret only (no user context needed — service-to-service).
"""

import structlog
from fastapi import APIRouter, Depends, Request

from app.config import Settings, get_settings
from app.dependencies import require_service_auth
from app.embedding.schemas import EmbedRequest, EmbedResponse
from app.embedding.service import EmbeddingService
from app.exceptions import ProcessingError

log = structlog.get_logger(__name__)
router = APIRouter(tags=["embedding"])


def get_embedding_service(
    request: Request,
    settings: Settings = Depends(get_settings),
) -> EmbeddingService:
    """
    Return a cached EmbeddingService from app.state, or create one on first use.
    This avoids re-creating the AsyncOpenAI client on every request.
    """
    if not hasattr(request.app.state, "embedding_service"):
        request.app.state.embedding_service = EmbeddingService(settings)
    return request.app.state.embedding_service


@router.post("/embed", response_model=EmbedResponse)
async def embed(
    body: EmbedRequest,
    _: None = Depends(require_service_auth),
    svc: EmbeddingService = Depends(get_embedding_service),
) -> EmbedResponse:
    """
    Generate a vector embedding for the provided text.

    Returns the embedding vector, its dimension count, and the model name.
    The caller is responsible for storing/using the vector.
    """
    try:
        vector = await svc.embed(body.text, use_case=body.use_case)
    except Exception as exc:
        log.error("embedding_failed", error=str(exc))
        raise ProcessingError("Embedding generation failed.")

    return EmbedResponse(
        embedding=vector,
        dims=len(vector),
        model=svc.model,
    )

"""
Embedding endpoint request/response schemas.
"""

from typing import Literal

from pydantic import BaseModel, Field


class EmbedRequest(BaseModel):
    text: str = Field(..., min_length=1, description="Text to embed")
    use_case: Literal["document", "search"] = Field(
        default="document",
        description=(
            "Prefix hint for nomic-embed-text: "
            "'document' → search_document prefix, "
            "'search' → search_query prefix"
        ),
    )


class EmbedResponse(BaseModel):
    embedding: list[float]
    dims: int
    model: str

"""
Vision stage output models.

VisionPage   — one page's text transcription + visual element descriptions.
VisionOutput — full multi-page result with derived fullText + averageConfidence.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class VisionMeta(BaseModel):
    pageCount: int
    engine: str = "vision-llm"


class VisionPage(BaseModel):
    pageNumber: int
    confidence: float
    text: str
    visualElements: list[str] = Field(default_factory=list)


class VisionOutput(BaseModel):
    meta: VisionMeta
    pages: list[VisionPage]
    fullText: str = ""
    averageConfidence: float = 0.0

    @classmethod
    def from_dict(cls, data: dict) -> VisionOutput:
        """Deserialise a raw dict (e.g. from DB JSONB) into a VisionOutput."""
        return cls.model_validate(data)

    def to_dict(self) -> dict:
        """Serialise to a plain dict suitable for JSONB storage."""
        return self.model_dump()

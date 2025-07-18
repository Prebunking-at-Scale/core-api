from typing import Any
from uuid import UUID, uuid4

from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from pydantic import BaseModel, Field


class TranscriptSentence(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    source: str  # Speech-to-text, OCR, etc
    text: str  # The actual text of the sentence
    start_time_s: float  # Start time in seconds
    embedding: list[float]
    metadata: dict[str, Any] = {}


class Transcript(BaseModel):
    video_id: UUID | None
    sentences: list[TranscriptSentence]


class TranscriptDTO(PydanticDTO[Transcript]):
    config = DTOConfig(
        exclude={
            "video_id",
            "embedding",
        },
    )


class TranscriptSentenceResponse(BaseModel):
    """Response model for transcript sentences without embeddings"""
    id: UUID
    source: str
    text: str
    start_time_s: float
    metadata: dict[str, Any] = {}


class TranscriptResponse(BaseModel):
    """Response model for transcripts without embeddings"""
    video_id: UUID | None
    sentences: list[TranscriptSentenceResponse]

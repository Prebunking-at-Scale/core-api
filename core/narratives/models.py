from typing import Any
from uuid import UUID

from pydantic import BaseModel

from core.entities.models import EntityInput


class NarrativeInput(BaseModel):
    title: str
    description: str
    claim_ids: list[UUID] = []
    topic_ids: list[UUID] = []
    entities: list[EntityInput] | None = None
    metadata: dict[str, Any] = {}


class NarrativePatchInput(BaseModel):
    title: str | None = None
    description: str | None = None
    claim_ids: list[UUID] | None = None
    topic_ids: list[UUID] | None = None
    entities: list[EntityInput] | None = None
    metadata: dict[str, Any] | None = None


class TopicSummary(BaseModel):
    id: UUID
    topic: str


class NarrativeSummary(BaseModel):
    """Lightweight summary of a narrative for dashboard display."""

    id: UUID
    title: str
    topics: list[TopicSummary] = []
    platforms: list[str] = []
    total_views: int = 0
    total_likes: int = 0
    total_comments: int = 0
    claim_count: int = 0
    video_count: int = 0
    language_count: int = 0


# Alias for backwards compatibility
ViralNarrativeSummary = NarrativeSummary

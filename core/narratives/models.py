from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from core.topics.models import Topic
from core.videos.claims.models import Claim


class Narrative(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    title: str
    description: str
    claims: list[Claim] = []
    topics: list[Topic] = []
    videos: list[Any] = []
    metadata: dict[str, Any] = {}
    created_at: datetime | None = None
    updated_at: datetime | None = None


class NarrativeInput(BaseModel):
    title: str
    description: str
    claim_ids: list[UUID] = []
    topic_ids: list[UUID] = []
    metadata: dict[str, Any] = {}

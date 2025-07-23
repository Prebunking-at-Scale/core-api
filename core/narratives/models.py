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
    claims: list[Claim] = Field(default_factory=list)
    topics: list[Topic] = Field(default_factory=list)
    videos: list[Any] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class NarrativeInput(BaseModel):
    title: str
    description: str
    claim_ids: list[UUID] = Field(default_factory=list)
    topic_ids: list[UUID] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from pydantic import BaseModel, Field

from core.topics.models import Topic


class Claim(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    video_id: UUID | None = None  # Reference to the video
    claim: str  # The claim made in the video
    start_time_s: float  # When in the video the claim starts
    embedding: list[float] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)  # Additional metadata about the claim
    topics: list[Topic] = Field(default_factory=list)  # Associated topics
    video: Any | None = None  # Video information
    narratives: list[Any] = Field(default_factory=list)  # Associated narratives
    created_at: datetime | None = None
    updated_at: datetime | None = None


class VideoClaims(BaseModel):
    video_id: UUID | None
    claims: list[Claim]


class VideoClaimsDTO(PydanticDTO[VideoClaims]):
    config = DTOConfig(
        exclude={
            "video_id",
            "embedding",
        },
    )


class ClaimUpdate(BaseModel):
    entities: list[UUID] = Field(default_factory=list)  # Future entity IDs
    topics: list[UUID] = Field(default_factory=list)  # Topic IDs to associate

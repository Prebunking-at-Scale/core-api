from typing import Any
from uuid import UUID, uuid4

from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from pydantic import BaseModel, Field


class Claim(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    claim: str  # The claim made in the video
    start_time_s: float  # When in the video the claim starts
    embedding: list[float] | None = None
    metadata: dict[str, Any] = {}  # Additional metadata about the claim


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

from unittest.mock import Base
import annotated_types
from uuid import UUID, uuid4

from datetime import datetime
from typing import Annotated, Any

from litestar.plugins.pydantic import PydanticDTO
from litestar.dto import DTOConfig
from pydantic import BaseModel, Field


class Video(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    title: str
    description: str
    platform: str
    source_url: str
    destination_path: str
    uploaded_at: datetime | None
    views: int | None = None
    likes: int | None = None
    comments: int | None = None
    channel: str | None = None
    channel_followers: int | None = None
    scrape_topic: str | None = None
    scrape_keyword: str | None = None
    metadata: dict[str, Any] = {}


class VideoPatch(PydanticDTO[Video]):
    config = DTOConfig(
        partial=True,
        include={
            "title",
            "description",
            "views",
            "likes",
            "comments",
            "channel_followers",
            "metadata",
        },
    )


class VideoFilters(BaseModel):
    platform: list[str] | None = None
    channel: list[str] | None = None
    metadata: str | None = None
    cursor: UUID | None = None
    limit: Annotated[int, annotated_types.Gt(0)] = 25


class VideoClaims(BaseModel):
    video_id: UUID
    claim: str  # The claim made in the video
    start_time_s: float  # When in the video the claim starts
    metadata: dict[str, Any] | None = None  # Additional metadata about the claim

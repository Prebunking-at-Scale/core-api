from datetime import datetime
from typing import Annotated, Any
from uuid import UUID, uuid4

import annotated_types
from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from pydantic import BaseModel, Field

from core.videos.claims.models import VideoClaims
from core.videos.transcripts.models import Transcript


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


class AnalysedVideo(Video):
    transcript: Transcript | None = None
    claims: VideoClaims | None = None


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
    semantic: str | None = None
    limit: Annotated[int, annotated_types.Gt(0)] = 25

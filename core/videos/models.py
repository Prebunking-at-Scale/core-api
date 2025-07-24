from typing import Annotated, Any
from uuid import UUID

import annotated_types
from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from pydantic import BaseModel

from core.models import Narrative, Transcript, Video
from core.videos.claims.models import VideoClaims


class AnalysedVideo(Video):
    transcript: Transcript | None = None
    claims: VideoClaims | None = None
    narratives: list[Narrative] = []


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

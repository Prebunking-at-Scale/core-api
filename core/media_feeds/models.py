import abc
from datetime import datetime
from typing import Literal
from uuid import UUID, uuid4

from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from pydantic import BaseModel, Field, JsonValue

Platform = Literal["youtube", "instagram", "tiktok"]


class MediaFeed(BaseModel, abc.ABC):
    id: UUID = Field(default_factory=uuid4)
    organisation_id: UUID
    is_archived: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None


class KeywordFeed(MediaFeed):
    topic: str
    keywords: list[str]


class ChannelFeed(MediaFeed):
    channel: str
    platform: Platform


class AllFeeds(BaseModel):
    channel_feeds: list[ChannelFeed]
    keyword_feeds: list[KeywordFeed]


class Cursor(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    target: str = Field(
        description="The target identifier, either a channel name or keyword topic"
    )
    platform: Platform
    cursor: JsonValue = {}
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ChannelFeedDTO(PydanticDTO[ChannelFeed]):
    config = DTOConfig(
        exclude={
            "id",
            "organisation_id",
            "is_archived",
            "created_at",
            "updated_at",
        },
    )


class KeywordFeedDTO(PydanticDTO[KeywordFeed]):
    config = DTOConfig(
        exclude={
            "id",
            "organisation_id",
            "is_archived",
            "created_at",
            "updated_at",
        },
    )


class CursorDTO(PydanticDTO[Cursor]):
    config = DTOConfig(
        exclude={
            "created_at",
            "updated_at",
        },
    )

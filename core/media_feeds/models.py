import abc
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from pydantic import BaseModel, Field, Json


class Feed(BaseModel, abc.ABC):
    id: UUID = Field(default_factory=uuid4)
    organisation_id: UUID
    created_by_user_id: UUID
    is_archived: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None


class KeywordFeed(Feed):
    topic: str
    keywords: list[str]


class ChannelFeed(Feed):
    channel: str
    platform: str


class Cursor(BaseModel):
    feed_id: UUID
    cursor: Json[Any] = {}


class ChannelFeedDTO(PydanticDTO[ChannelFeed]):
    config = DTOConfig(
        exclude={
            "id",
            "created_at",
            "updated_at",
        },
    )


class KeywordFeedDTO(PydanticDTO[KeywordFeed]):
    config = DTOConfig(
        exclude={
            "id",
            "created_at",
            "updated_at",
        },
    )

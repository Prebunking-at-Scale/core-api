import abc
import re
from datetime import datetime
from typing import Literal, get_args
from uuid import UUID, uuid4

from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from pydantic import BaseModel, Field, JsonValue, field_validator

Platform = Literal["youtube", "instagram", "tiktok"]
VALID_PLATFORMS: tuple[str, ...] = get_args(Platform)


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


def parse_channel_from_url(url: str) -> tuple[Platform, str]:
    youtube_patterns = [
        r"(?:youtube\.com|m\.youtube\.com)/(?:channel|c|user)/([a-zA-Z0-9_-]+)",
        r"(?:youtube\.com|m\.youtube\.com)/(@[a-zA-Z0-9_-]+)",
        r"youtu\.be/([a-zA-Z0-9_-]+)",
    ]

    instagram_patterns = [
        r"(?:instagram\.com|instagr\.am)/([a-zA-Z0-9._]+)/?(?:\?|$)",
    ]

    tiktok_patterns = [
        r"tiktok\.com/(@[a-zA-Z0-9._]+)/?(?:\?|$)",
    ]

    for pattern in youtube_patterns:
        match = re.search(pattern, url, flags=re.IGNORECASE)
        if match:
            return "youtube", match.group(1)

    for pattern in instagram_patterns:
        match = re.search(pattern, url, flags=re.IGNORECASE)
        if match:
            return "instagram", match.group(1)

    for pattern in tiktok_patterns:
        match = re.search(pattern, url, flags=re.IGNORECASE)
        if match:
            return "tiktok", match.group(1)

    raise ValueError(
        f"Could not parse channel from URL: {url}. "
        f"Expected formats: youtube.com/@channel, youtube.com/channel/ID, "
        f"instagram.com/username, tiktok.com/@username"
    )


class ChannelURLRequest(BaseModel):
    url: str

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("URL cannot be empty")
        return v.strip()

    def parse_channel_info(self) -> tuple[Platform, str]:
        return parse_channel_from_url(self.url)


class ChannelURLRequestDTO(PydanticDTO[ChannelURLRequest]):
    pass


class SkippedChannel(BaseModel):
    channel: str
    platform: Platform


class BulkChannelUploadResult(BaseModel):
    created: list[ChannelFeed]
    skipped: list[SkippedChannel]
    errors: list[str]

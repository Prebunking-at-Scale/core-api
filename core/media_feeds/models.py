import abc
import re
from datetime import datetime
from typing import Literal
from uuid import UUID, uuid4

from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from pydantic import BaseModel, Field, JsonValue, field_validator

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


class ChannelURLRequest(BaseModel):
    url: str

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("URL cannot be empty")
        return v.strip()

    def parse_channel_info(self) -> tuple[Platform, str]:
        url = self.url.lower()
        
        youtube_patterns = [
            r"(?:youtube\.com/channel/|youtube\.com/c/|youtube\.com/user/|youtube\.com/@)([^/?&]+)",
            r"youtu\.be/([^/?&]+)"
        ]
        
        instagram_patterns = [
            r"instagram\.com/([^/?&]+)",
            r"instagr\.am/([^/?&]+)"
        ]
        
        tiktok_patterns = [
            r"tiktok\.com/@([^/?&]+)",
            r"tiktok\.com/([^/@?&]+)"
        ]
        
        for pattern in youtube_patterns:
            match = re.search(pattern, url)
            if match:
                return "youtube", match.group(1)
        
        for pattern in instagram_patterns:
            match = re.search(pattern, url)
            if match:
                return "instagram", match.group(1)
        
        for pattern in tiktok_patterns:
            match = re.search(pattern, url)
            if match:
                return "tiktok", match.group(1)
        
        raise ValueError("URL is not a valid YouTube, Instagram, or TikTok channel URL")


class ChannelURLRequestDTO(PydanticDTO[ChannelURLRequest]):
    pass

import uuid

from datetime import datetime
from typing import Any

from litestar.plugins.pydantic import PydanticDTO
from litestar.dto import DTOConfig

from core.models import IDAuditModel


class Video(IDAuditModel):
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
    metadata: dict[str, Any] | None = None


class VideoCreate(PydanticDTO[Video]):
    config = DTOConfig(
        exclude={
            "id",
            "created_at",
            "updated_id",
        },
    )


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


class TranscriptSentence(IDAuditModel):
    transcript_id: uuid.UUID
    source: str  # Speech-to-text, OCR, etc
    text: str  # The actual text of the sentence
    start_time_s: float  # Start time in seconds
    metadata: dict[str, Any] | None = None


class Transcript(IDAuditModel):
    video_id: uuid.UUID
    sentences: list[TranscriptSentence]
    metadata: dict[str, Any] | None = None


class VideoClaims(IDAuditModel):
    video_id: uuid.UUID
    claim: str  # The claim made in the video
    start_time_s: float  # When in the video the claim starts
    metadata: dict[str, Any] | None = None  # Additional metadata about the claim

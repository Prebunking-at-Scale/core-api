from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Video(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    title: str
    description: str
    platform: str
    source_url: str
    destination_path: str = ""
    uploaded_at: datetime | None
    views: int | None = None
    likes: int | None = None
    comments: int | None = None
    channel: str | None = None
    channel_followers: int | None = None
    scrape_topic: str | None = None
    scrape_keyword: str | None = None
    metadata: dict[str, Any] = {}


class TranscriptSentence(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    source: str  # Speech-to-text, OCR, etc
    text: str  # The actual text of the sentence
    start_time_s: float  # Start time in seconds
    metadata: dict[str, Any] = {}


class Transcript(BaseModel):
    video_id: UUID | None
    sentences: list[TranscriptSentence]


class Topic(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    topic: str
    metadata: dict[str, Any] = {}
    created_at: datetime | None = None
    updated_at: datetime | None = None


class Entity(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    wikidata_id: str
    name: str
    metadata: dict[str, Any] = {}
    created_at: datetime | None = None
    updated_at: datetime | None = None


class Claim(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    video_id: UUID | None = None  # Reference to the video
    claim: str  # The claim made in the video
    start_time_s: float  # When in the video the claim starts
    metadata: dict[str, Any] = {}  # Additional metadata about the claim
    entities: list[Entity] = []
    created_at: datetime | None = None
    updated_at: datetime | None = None


class Narrative(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    title: str
    description: str
    claims: list[Claim] = []
    topics: list[Topic] = []
    entities: list[Entity] = []
    videos: list[Video] = []
    metadata: dict[str, Any] = {}
    created_at: datetime | None = None
    updated_at: datetime | None = None

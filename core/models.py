from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class NarrativeSpreadLevel(str, Enum):
    """
    The two axes of docs/narrative-spread-level-redesign.md give four corners of meaning:
    VIRAL is big and still climbing, EARLY_SURGE is small but climbing, CONSOLIDATED is
    big and flat, TRENDING is the broad middle. They do not tile the plane — a narrative
    that is small *and* flat gets no badge at all, which is `spread_level IS NULL` rather
    than NONE.

    ALERT and WATCH are retired: the classifier no longer emits them. They remain here
    (and in the Postgres enum) only so consumers still filtering on them keep working
    until they migrate.
    """

    NONE = "none"
    VIRAL = "viral"
    EARLY_SURGE = "early_surge"
    CONSOLIDATED = "consolidated"
    TRENDING = "trending"
    ALERT = "alert"
    WATCH = "watch"


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


class VideoStats(BaseModel):
    video_id: UUID
    views: int | None = None
    likes: int | None = None
    comments: int | None = None
    channel_followers: int | None = None
    recorded_at: datetime | None = None


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
    narrative_context: str | None = None
    claims: list[Claim] = []
    topics: list[Topic] = []
    entities: list[Entity] = []
    videos: list[Video] = []
    metadata: dict[str, Any] = {}
    created_at: datetime | None = None
    updated_at: datetime | None = None
    spread_level: NarrativeSpreadLevel | None = None


class NarrativeFeedback(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    user_id: UUID
    narrative_id: UUID
    feedback_score: float = Field(ge=0.0, le=1.0)
    feedback_text: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class NarrativeFeedbackSummary(BaseModel):
    score_count: int = Field(ge=0, description="Number of users who rated the narrative")
    average_score: float | None = Field(
        default=None, description="Average feedback score across users (null when none exist)"
    )


class ClaimNarrativeFeedback(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    user_id: UUID
    claim_id: UUID
    narrative_id: UUID
    feedback_score: float = Field(ge=0.0, le=1.0)
    feedback_text: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


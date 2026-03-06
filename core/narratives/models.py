from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from core.entities.models import EntityInput
from core.models import Claim, Entity, Topic, Video


class NarrativeInput(BaseModel):
    title: str
    description: str
    claim_ids: list[UUID] = []
    topic_ids: list[UUID] = []
    entities: list[EntityInput] | None = None
    metadata: dict[str, Any] = {}


class NarrativePatchInput(BaseModel):
    title: str | None = None
    description: str | None = None
    claim_ids: list[UUID] | None = None
    topic_ids: list[UUID] | None = None
    entities: list[EntityInput] | None = None
    metadata: dict[str, Any] | None = None


class TopicSummary(BaseModel):
    id: UUID
    topic: str


class NarrativeSummary(BaseModel):
    """Lightweight summary of a narrative for dashboard and list views."""

    id: UUID
    title: str
    description: str = ""
    topics: list[TopicSummary] = []
    platforms: list[str] = []
    total_views: int = 0
    total_likes: int = 0
    total_comments: int = 0
    claim_count: int = 0
    video_count: int = 0
    language_count: int = 0
    entity_count: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None


# Alias for backwards compatibility
ViralNarrativeSummary = NarrativeSummary

# Alias for list endpoints
NarrativeListItem = NarrativeSummary


class NarrativeDetail(BaseModel):
    """Full narrative with preview of claims/videos and total counts."""

    id: UUID
    title: str
    description: str
    topics: list[Topic] = []
    entities: list[Entity] = []
    claims: list[Claim] = []  # Preview items
    claim_count: int = 0  # Total count
    videos: list[Video] = []  # Preview items
    video_count: int = 0  # Total count
    total_views: int = 0
    total_likes: int = 0
    total_comments: int = 0
    platforms: list[str] = []
    language_count: int = 0
    metadata: dict[str, Any] = {}
    created_at: datetime | None = None
    updated_at: datetime | None = None


class NarrativeStatsDataPoint(BaseModel):
    """A single data point in the narrative stats time series."""

    date: datetime
    views: int = 0
    likes: int = 0
    comments: int = 0
    cumulative_views: int = 0
    cumulative_likes: int = 0
    cumulative_comments: int = 0
    video_count: int = 0
    cumulative_video_count: int = 0


class NarrativeStatsTotals(BaseModel):
    """Total stats for a narrative."""

    views: int = 0
    likes: int = 0
    comments: int = 0
    video_count: int = 0


class NarrativeStats(BaseModel):
    """Time-series stats for a narrative, used for evolution charts."""

    narrative_id: UUID
    time_series: list[NarrativeStatsDataPoint] = []
    totals: NarrativeStatsTotals = NarrativeStatsTotals()

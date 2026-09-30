from datetime import datetime
from typing import Any, Generic, Literal, TypeVar
from uuid import UUID

from pydantic import BaseModel, field_validator
from pydantic.dataclasses import dataclass

from core.models import NarrativeSpreadPattern
from core.narratives.models import NarrativeSummary, TopicSummary

T = TypeVar("T")

SearchTab = Literal["narratives", "claims", "videos"]

# Exact counts stop here; beyond it a tab shows "10,000+" (docs/search.md).
COUNT_CAP = 10_000

MatchSource = Literal["direct", "claims", "narrative"]


class SearchFilters(BaseModel):
    """The search's filters, shared by the three tabs and the counts (docs/search.md).
    Values within a filter are ORed; filters are ANDed. min_score/max_score only apply
    to claims and spread_patterns only to narratives."""

    topic_ids: list[UUID] = []
    entity_ids: list[UUID] = []
    keywords: list[str] = []
    keyword_mode: Literal["any", "all"] = "any"
    languages: list[str] = []
    platforms: list[str] = []
    channels: list[str] = []
    start_date: datetime | None = None
    end_date: datetime | None = None
    min_score: float | None = None
    max_score: float | None = None
    spread_patterns: list[NarrativeSpreadPattern] = []

    @field_validator("keywords", "languages", "platforms", "channels")
    @classmethod
    def _drop_blank(cls, values: list[str]) -> list[str]:
        cleaned: list[str] = []
        for value in values:
            value = value.strip()
            if value and value not in cleaned:
                cleaned.append(value)
        return cleaned


@dataclass
class SearchPage(Generic[T]):
    """A page of results. total stops at COUNT_CAP, with total_capped set."""

    data: T
    total: int
    total_capped: bool
    page: int
    size: int


class TabCount(BaseModel):
    total: int
    capped: bool


class SearchCounts(BaseModel):
    narratives: TabCount
    claims: TabCount
    videos: TabCount


class SearchNarrative(NarrativeSummary):
    """A narrative result: matched on its own fields ("direct") or needed one of its
    claims for a topic or keyword ("claims")."""

    match_source: MatchSource = "direct"


class NarrativeRef(BaseModel):
    id: UUID
    title: str


class EntityRef(BaseModel):
    id: UUID
    name: str


class ClaimVideo(BaseModel):
    id: UUID
    title: str
    description: str = ""
    platform: str
    source_url: str
    channel: str | None = None
    uploaded_at: datetime | None = None
    views: int | None = None
    likes: int | None = None
    comments: int | None = None
    metadata: dict[str, Any] = {}


class SearchClaim(BaseModel):
    """A claim result. Its topics are its own (metadata.topics), never its
    narrative's. match_source is "narrative" when an entity filter only held through
    the claim's narrative, with those entities in via_entities."""

    id: UUID
    video_id: UUID | None = None
    claim: str
    start_time_s: float
    metadata: dict[str, Any] = {}
    created_at: datetime | None = None
    updated_at: datetime | None = None
    topics: list[TopicSummary] = []
    narratives: list[NarrativeRef] = []
    video: ClaimVideo | None = None
    match_source: MatchSource = "direct"
    via_entities: list[EntityRef] = []


class SearchVideo(BaseModel):
    """A video result, without transcript or claims. match_source is "claims" when a
    keyword matched one of its claims and not its title."""

    id: UUID
    title: str
    description: str
    platform: str
    source_url: str
    destination_path: str = ""
    uploaded_at: datetime | None = None
    views: int | None = None
    likes: int | None = None
    comments: int | None = None
    channel: str | None = None
    channel_followers: int | None = None
    metadata: dict[str, Any] = {}
    match_source: MatchSource = "direct"


class ChannelOption(BaseModel):
    channel: str
    platform: str
    is_own: bool = False


class LanguageOption(BaseModel):
    language: str
    count: int

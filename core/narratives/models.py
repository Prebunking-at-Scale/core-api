from datetime import date, datetime
from enum import Enum
from typing import Any, Generic, NamedTuple, TypedDict, TypeVar
from uuid import UUID

from pydantic import BaseModel

from core.entities.models import EntityInput
from core.models import Claim, Entity, NarrativeSpreadPattern, Topic, Video

class IndicatorPayload(TypedDict):
    """
    One analysis indicator as carried by get_bulk_analysis_indicators_for_date:
    the raw indicator_value plus its free-form metadata blob (JSONB), which holds
    the percentile rank the spread-pattern classification compares on.
    """

    value: float
    metadata: dict[str, Any]


class ViralityScoreRank(NamedTuple):
    """
    One virality score as ranked within its run: where the narrative sits in the cohort,
    and what it actually scored.

    Both travel together because they answer different questions and the detail view
    shows both — `score` is the narrative's own magnitude (reach is a view count,
    engagement a per-view ratio), `percentile` is only its position relative to the rest.
    """

    percentile: float
    score: float


class NarrativeInput(BaseModel):
    title: str
    description: str
    narrative_context: str | None = None
    claim_ids: list[UUID] = []
    topic_ids: list[UUID] = []
    entities: list[EntityInput] | None = None
    metadata: dict[str, Any] = {}


class NarrativePatchInput(BaseModel):
    title: str | None = None
    description: str | None = None
    narrative_context: str | None = None
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
    score_count: int = 0
    average_score: float | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    spread_pattern: NarrativeSpreadPattern | None = None


# Alias for backwards compatibility
ViralNarrativeSummary = NarrativeSummary

# Alias for list endpoints
NarrativeListItem = NarrativeSummary


class NarrativeDetail(BaseModel):
    """Full narrative with preview of claims/videos and total counts."""

    id: UUID
    title: str
    description: str
    narrative_context: str | None = None
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
    spread_pattern: NarrativeSpreadPattern | None = None


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


class NarrativeViralityScoreType(str, Enum):
    """
    The two virality-STATE signals. `velocity_score` was a third, and was dropped: it
    measures change, and change belongs on the acceleration axis. Historical rows still
    carry it, but nothing parses score_type back into this enum — the percentile query
    keys on the raw column — so an old value simply matches nothing.
    """

    ENGAGEMENT_SCORE = "engagement_score"
    REACH_SCORE = "reach_score"


class NarrativeAnalysisIndicatorType(str, Enum):
    COMPOSITE_VIRALITY = "composite_virality"
    ACCELERATION_RATE = "acceleration_rate"


IndicatorMetadata = TypeVar("IndicatorMetadata")


class CompositeViralityMetadata(BaseModel):
    """
    `percentile` is the narrative's PERCENT_RANK of `indicator_value` across the run's
    cohort, and it is the number the classifier actually reads — the raw blend is only
    an intermediate. Consumers rendering a position ("top 5%") must use it and not
    `indicator_value`, which is a weighted blend of two ranks and not itself a rank.

    Optional because it is filled in only once every narrative in the run has been
    scored (see `_attach_percentiles`), so a row read mid-run may not carry it yet.

    `velocity_percentile`/`velocity_weight` were dropped with the velocity term (D6);
    rows written before that still carry them and are simply ignored here.

    `reach_score` and `engagement_score` are the RAW scores the two percentiles were
    ranked from — reach is the narrative's summed view count, engagement its
    (likes + 5×comments) / views ratio. They are what the detail view headlines, because
    a percentile answers "larger than whom" and never "how large". Optional: rows written
    before this was recorded carry only the ranks, and the client falls back to showing
    the percentile in the headline as it did before.
    """

    engagement_percentile: float
    reach_percentile: float
    engagement_weight: float
    reach_weight: float
    percentile: float | None = None
    reach_score: float | None = None
    engagement_score: float | None = None


class AccelerationRateMetadata(BaseModel):
    """
    `percentile` carries the same meaning as on the composite axis, but ranks over a
    different cohort — only the narratives visited on `calc_date`, because a rate we did
    not measure today is uncomputable rather than zero (D0/D3). The two percentiles are
    therefore never comparable to each other, only each to a constant on its own axis.

    `refreshed_videos` and `mean_gap_days` describe how much of the narrative was
    actually re-measured, which is what tells a reader how much weight the rate deserves.
    """

    change_engagement: float
    change_video_count: float
    change_views: float
    engagement_weight: float
    video_volume_weight: float
    views_weight: float
    percentile: float | None = None
    refreshed_videos: int | None = None
    mean_gap_days: float | None = None


class AnalysisIndicator(BaseModel, Generic[IndicatorMetadata]):
    id: UUID
    indicator_value: float
    indicator_type: NarrativeAnalysisIndicatorType
    calculated_at: datetime
    metadata: IndicatorMetadata | None = None


class NarrativeAnalysisIndicatorsResponse(BaseModel):
    narrative_id: UUID
    composite_virality: AnalysisIndicator[CompositeViralityMetadata]
    acceleration_rate: AnalysisIndicator[AccelerationRateMetadata] | None = None
    date: date

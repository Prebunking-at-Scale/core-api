import logging
from datetime import date, datetime
from typing import Any, AsyncContextManager, Callable
from uuid import UUID

from core.entities.service import EntityService
from core.models import Claim, Narrative, NarrativeAlertLevel, Video
from core.narratives.models import (
    AnalysisIndicator,
    NarrativeAnalysisIndicatorType,
    NarrativeAnalysisIndicatorsResponse,
    NarrativeDetail,
    NarrativeInput,
    NarrativeListItem,
    NarrativePatchInput,
    NarrativeStats,
    NarrativeSummary,
    ViralNarrativeSummary,
)
from core.narratives.repo import NarrativeRepository
from core.narratives.api import NarrativesApiClient
from core.narratives.models import NarrativeViralityScoreType
from core.uow import ConnectionFactory, uow

logger = logging.getLogger(__name__)

_api = NarrativesApiClient()

# Virality score weights and parameters
VIRALITY_SCORE_LIKES_WEIGHT = 1        # weight of likes relative to comments
VIRALITY_SCORE_COMMENTS_WEIGHT = 5     # weight of comments relative to likes
VIRALITY_SCORE_REACH_CAP_LIMIT = 10    # cap reach score at 10x the average views
VIRALITY_SCORE_VELOCITY_DAYS_BACK = 2  # window (days) used for velocity score

# Composite virality score weights (must sum to 1)
COMPOSITE_ENGAGEMENT_WEIGHT = 0.50
COMPOSITE_REACH_WEIGHT = 0.30
COMPOSITE_VELOCITY_WEIGHT = 0.20

# Acceleration rate score weights (must sum to 1)
ACCELERATION_ENGAGEMENT_WEIGHT = 0.40
ACCELERATION_VIDEO_VOLUME_WEIGHT = 0.35
ACCELERATION_VIEWS_WEIGHT = 0.25

# Hard cap on individual change_* components inside acceleration_rate.
# Without it, a single video going from 1 → 10k views (change=9999) drowns
# the weighted sum and makes the per-dimension weights meaningless.
ACCELERATION_CHANGE_CAP = 5.0

def _merge_narrative_context(
    existing: str | None, new: str | None
) -> str | None:
    """Return the latest narrative_context, falling back to existing when new is empty."""
    return new if new else existing


class NarrativeService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[NarrativeRepository]:
        return uow(NarrativeRepository, self._connection_factory)

    async def create_narrative(self, narrative: NarrativeInput) -> Narrative:
        async with self.repo() as repo:
            if not await repo.claims_exist(narrative.claim_ids):
                raise ValueError("one or more claims not found")

            # Process entities first
            entity_ids = []
            if narrative.entities:
                entity_service = EntityService(self._connection_factory)
                entity_ids = await entity_service.process_entities(narrative.entities)

            # First check if a narrative with the same title exists
            existing_narrative = await repo.find_by_title(narrative.title)

            # If no narrative with the same title exists, check for narrative_id in metadata
            if not existing_narrative:
                narrative_id_in_metadata = narrative.metadata.get("narrative_id")
                if narrative_id_in_metadata:
                    existing_narrative = await repo.find_by_narrative_id_in_metadata(
                        narrative_id_in_metadata
                    )

            if existing_narrative:
                # Merge claim_ids and topic_ids with existing ones
                existing_claim_ids = [claim.id for claim in existing_narrative.claims]
                merged_claim_ids = list(set(existing_claim_ids + narrative.claim_ids))

                existing_topic_ids = [topic.id for topic in existing_narrative.topics]
                merged_topic_ids = list(set(existing_topic_ids + narrative.topic_ids))

                # Merge entity_ids with existing ones
                existing_entity_ids = [entity.id for entity in existing_narrative.entities]
                merged_entity_ids = list(set(existing_entity_ids + entity_ids))

                merged_narrative_context = _merge_narrative_context(
                    existing_narrative.narrative_context,
                    narrative.narrative_context,
                )

                updated_narrative = await repo.update_narrative(
                    narrative_id=existing_narrative.id,
                    title=narrative.title,
                    description=narrative.description,
                    narrative_context=merged_narrative_context,
                    claim_ids=merged_claim_ids,
                    topic_ids=merged_topic_ids,
                    entity_ids=merged_entity_ids,
                    metadata=narrative.metadata,
                )
                if updated_narrative is None:
                    raise ValueError(f"Failed to update narrative with ID {existing_narrative.id}")
                return updated_narrative

            return await repo.create_narrative(
                title=narrative.title,
                description=narrative.description,
                claim_ids=narrative.claim_ids,
                topic_ids=narrative.topic_ids,
                entity_ids=entity_ids,
                metadata=narrative.metadata,
                narrative_context=narrative.narrative_context,
            )

    async def get_narrative(self, narrative_id: UUID) -> Narrative | None:
        async with self.repo() as repo:
            return await repo.get_narrative(narrative_id)

    async def get_narrative_detail(
        self,
        narrative_id: UUID,
        claims_limit: int = 10,
        videos_limit: int = 10,
    ) -> NarrativeDetail | None:
        async with self.repo() as repo:
            return await repo.get_narrative_detail(
                narrative_id, claims_limit=claims_limit, videos_limit=videos_limit
            )

    async def get_narrative_claims(
        self, narrative_id: UUID, limit: int, offset: int
    ) -> tuple[list[Claim], int]:
        async with self.repo() as repo:
            if not await repo.narrative_exists(narrative_id):
                raise ValueError("narrative not found")
            return await repo.get_narrative_claims(narrative_id, limit, offset)

    async def get_narrative_videos(
        self, narrative_id: UUID, limit: int, offset: int
    ) -> tuple[list[Video], int]:
        async with self.repo() as repo:
            if not await repo.narrative_exists(narrative_id):
                raise ValueError("narrative not found")
            return await repo.get_narrative_videos(narrative_id, limit, offset)

    async def get_narrative_stats(self, narrative_id: UUID) -> NarrativeStats | None:
        async with self.repo() as repo:
            return await repo.get_narrative_stats(narrative_id)

    async def get_narratives_by_claim(self, claim_id: UUID) -> list[Narrative]:
        async with self.repo() as repo:
            return await repo.get_narratives_by_claim(claim_id)

    async def get_narratives_by_claim_list(self, claim_id: UUID) -> list[NarrativeListItem]:
        async with self.repo() as repo:
            return await repo.get_narratives_by_claim_list(claim_id)

    async def get_all_narratives(
        self,
        limit: int = 100,
        offset: int = 0,
        topic_id: UUID | None = None,
        entity_id: UUID | None = None,
        text: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        first_content_start: datetime | None = None,
        first_content_end : datetime | None = None,
        language: str | None = None,
    ) -> tuple[list[Narrative], int]:
        async with self.repo() as repo:
            narratives = await repo.get_all_narratives(
                limit=limit,
                offset=offset,
                topic_id=topic_id,
                entity_id=entity_id,
                text=text,
                start_date=start_date,
                end_date=end_date,
                first_content_start=first_content_start,
                first_content_end=first_content_end,
                language=language
            )
            total = await repo.count_all_narratives(
                topic_id=topic_id,
                entity_id=entity_id,
                text=text,
                start_date=start_date,
                end_date=end_date,
                first_content_start=first_content_start,
                first_content_end=first_content_end,
                language=language
            )
            return narratives, total

    async def get_all_narratives_list(
        self,
        limit: int = 100,
        offset: int = 0,
        topic_id: UUID | None = None,
        entity_id: UUID | None = None,
        text: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        first_content_start: datetime | None = None,
        first_content_end: datetime | None = None,
        language: str | None = None,
        alert_levels: list[str] | None = None,
        sort: str | None = None,
    ) -> tuple[list[NarrativeListItem], int]:
        async with self.repo() as repo:
            narratives = await repo.get_all_narratives_list(
                limit=limit,
                offset=offset,
                topic_id=topic_id,
                entity_id=entity_id,
                text=text,
                start_date=start_date,
                end_date=end_date,
                first_content_start=first_content_start,
                first_content_end=first_content_end,
                language=language,
                alert_levels=alert_levels,
                sort=sort,
            )
            total = await repo.count_all_narratives(
                topic_id=topic_id,
                entity_id=entity_id,
                text=text,
                start_date=start_date,
                end_date=end_date,
                first_content_start=first_content_start,
                first_content_end=first_content_end,
                language=language,
                alert_levels=alert_levels,
            )
            return narratives, total

    async def get_narratives_by_entity(
        self, entity_id: UUID, limit: int = 100, offset: int = 0
    ) -> tuple[list[Narrative], int]:
        async with self.repo() as repo:
            narratives = await repo.get_all_narratives(
                limit=limit, offset=offset, entity_id=entity_id
            )
            total = await repo.count_all_narratives(entity_id=entity_id)
            return narratives, total

    async def update_narrative(
        self,
        narrative_id: UUID,
        data: NarrativePatchInput,
    ) -> Narrative | None:
        async with self.repo() as repo:

            existing_narrative = await repo.get_narrative(narrative_id)
            if not existing_narrative:
                return None

            entity_ids = None
            if data.entities is not None:
                entity_service = EntityService(self._connection_factory)
                entity_ids = await entity_service.process_entities(data.entities)

            # Concatenate narrative_context with existing one
            merged_narrative_context = None
            if data.narrative_context is not None:
                merged_narrative_context = _merge_narrative_context(
                    existing_narrative.narrative_context,
                    data.narrative_context,
                )

            updated = await repo.update_narrative(
                narrative_id=narrative_id,
                title=data.title,
                description=data.description,
                narrative_context=merged_narrative_context,
                claim_ids=data.claim_ids,
                topic_ids=data.topic_ids,
                entity_ids=entity_ids,
                metadata=data.metadata,
            )

        # Sync to external API after successful local update
        if updated:
            external_id = updated.metadata.get("narrative_id")
            if external_id and (data.title is not None or data.narrative_context is not None):
                await self._sync_external_narrative(
                    external_narrative_id=external_id,
                    title=updated.title,
                    narrative_context=updated.narrative_context if data.narrative_context is not None else None,
                )

        return updated

    async def delete_narrative(self, narrative_id: UUID) -> None:
        async with self.repo() as repo:
            narrative = await repo.get_narrative(narrative_id)
            if narrative and narrative.metadata.get("narrative_id"):
                await self._delete_external_narrative(
                    narrative.metadata["narrative_id"]
                )

            await repo.delete_narrative(narrative_id)

    async def _delete_external_narrative(self, external_narrative_id: str) -> None:
        """Delete a narrative from the external narratives API."""
        if not _api.is_configured():
            return

        response = await _api.delete_narrative(external_narrative_id)

        if response.status_code == 404:
            logger.info(
                f"Narrative {external_narrative_id} not found on external API, "
                "continuing with local delete"
            )
            return

        if response.status_code >= 400:
            logger.error(
                f"External API delete error: status={response.status_code}, "
                f"response={response.text}"
            )
            response.raise_for_status()

        logger.info(f"Deleted narrative {external_narrative_id} from external API")
    
    async def _sync_external_narrative(
        self,
        external_narrative_id: str,
        title: str,
        narrative_context: str | None = None,
    ) -> None:
        """Sync narrative fields to the external narratives API.

        Logs a warning on failure but does not raise.
        """
        if not _api.is_configured():
            return

        try:
            response = await _api.update_narrative(
                external_narrative_id,
                title=title,
                narrative_context=narrative_context,
            )

            if response.status_code >= 400:
                logger.warning(
                    f"External API sync error: status={response.status_code}, "
                    f"response={response.text}"
                )
            else:
                logger.info(
                    f"Synced narrative {external_narrative_id} to external API"
                )
        except Exception as e:
            logger.warning(
                f"Failed to sync narrative {external_narrative_id} to external API: {e}"
            )

    async def update_metadata(
        self, narrative_id: UUID, metadata: dict[str, Any]
    ) -> dict[str, Any]:
        async with self.repo() as repo:
            updated = await repo.update_narrative(
                narrative_id=narrative_id,
                metadata=metadata,
            )
            if not updated:
                raise ValueError("narrative not found")
            return updated.metadata

    async def get_narratives_by_topic(
        self, topic_id: UUID, limit: int = 100, offset: int = 0
    ) -> tuple[list[Narrative], int]:
        async with self.repo() as repo:
            return await repo.get_narratives_by_topic(
                topic_id, limit=limit, offset=offset
            )

    async def get_viral_narratives(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[Narrative]:
        async with self.repo() as repo:
            return await repo.get_viral_narratives(
                limit=limit, offset=offset, hours=hours
            )

    async def get_prevalent_narratives(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[Narrative]:
        async with self.repo() as repo:
            return await repo.get_prevalent_narratives(
                limit=limit, offset=offset, hours=hours
            )

    async def get_viral_narratives_summary(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[ViralNarrativeSummary]:
        async with self.repo() as repo:
            return await repo.get_viral_narratives_summary(
                limit=limit, offset=offset, hours=hours
            )

    async def get_prevalent_narratives_summary(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[NarrativeSummary]:
        async with self.repo() as repo:
            return await repo.get_prevalent_narratives_summary(
                limit=limit, offset=offset, hours=hours
            )

    async def get_average_views_for_all_narratives(self) -> float:
        async with self.repo() as repo:
            return await repo.get_average_views_for_all_narratives()
        
    async def calculate_narrative_virality_scores(
        self, narrative_id: UUID, average_views: float | None = None
    ) -> tuple[float, float, float]:
        """
        Calculate and store virality scores for a single narrative.
            - engagement_score: based on likes and comments relative to views
            - reach_score: based on views relative to average views for all narratives
            - velocity_score: based on recent views relative to total views
         The average_views parameter can be provided to avoid redundant calculations when processing multiple narratives in a batch
        """

        async with self.repo() as repo:
            average_views_for_all_narratives = average_views if average_views is not None else await repo.get_average_views_for_all_narratives()
            narrative_stats = await self.get_narrative_stats(narrative_id)
            if not narrative_stats:
                raise ValueError("narrative not found")
            narrative_total_stats = narrative_stats.totals
            # calculate engagement_score
            engagement_score = (narrative_total_stats.likes * VIRALITY_SCORE_LIKES_WEIGHT + narrative_total_stats.comments * VIRALITY_SCORE_COMMENTS_WEIGHT) / narrative_total_stats.views if narrative_total_stats.views > 0 else 0

            await repo.insert_narrative_virality_score(
                narrative_id=narrative_id,
                score_value=engagement_score,
                score_type=NarrativeViralityScoreType.ENGAGEMENT_SCORE,
                metadata={
                    "likes": narrative_total_stats.likes,
                    "comments": narrative_total_stats.comments,
                    "views": narrative_total_stats.views,
                    "likes_weight": VIRALITY_SCORE_LIKES_WEIGHT,
                    "comments_weight": VIRALITY_SCORE_COMMENTS_WEIGHT,
                },
            )

            # calculate reach_score
            reach_score = min(narrative_total_stats.views / average_views_for_all_narratives, VIRALITY_SCORE_REACH_CAP_LIMIT) / VIRALITY_SCORE_REACH_CAP_LIMIT if average_views_for_all_narratives > 0 else 0
            await repo.insert_narrative_virality_score(
                narrative_id=narrative_id,
                score_value=reach_score,
                score_type=NarrativeViralityScoreType.REACH_SCORE,
                metadata={
                    "views": narrative_total_stats.views,
                    "average_views_for_all_narratives": average_views_for_all_narratives,
                    "reach_cap_limit": VIRALITY_SCORE_REACH_CAP_LIMIT,
                },
            )

            # calculate velocity_score
            last_days_stats = await repo.get_narrative_stats_delta_for_period(narrative_id=narrative_id, days_back=VIRALITY_SCORE_VELOCITY_DAYS_BACK)
            velocity_score = last_days_stats.views / (narrative_total_stats.views) if narrative_total_stats.views > 0 else 0

            await repo.insert_narrative_virality_score(
                narrative_id=narrative_id,
                score_value=velocity_score,
                score_type=NarrativeViralityScoreType.VELOCITY_SCORE,
                metadata={
                    "views_last_days": last_days_stats.views,
                    "total_views": narrative_total_stats.views,
                    "velocity_days_back": VIRALITY_SCORE_VELOCITY_DAYS_BACK,
                },
            )

            return engagement_score, reach_score, velocity_score

    async def calculate_composite_virality_for_date(self, calc_date: date) -> None:
        async with self.repo() as repo:
            all_percentiles = await repo.get_all_virality_percentiles_for_date(calc_date)
            records: list[tuple[UUID, float, NarrativeAnalysisIndicatorType, dict[str, Any] | None]] = []
            for narrative_id, percentiles in all_percentiles.items():
                composite = (
                    percentiles.get(NarrativeViralityScoreType.ENGAGEMENT_SCORE, 0) * COMPOSITE_ENGAGEMENT_WEIGHT
                    + percentiles.get(NarrativeViralityScoreType.REACH_SCORE, 0) * COMPOSITE_REACH_WEIGHT
                    + percentiles.get(NarrativeViralityScoreType.VELOCITY_SCORE, 0) * COMPOSITE_VELOCITY_WEIGHT
                )
                metadata = {
                    "engagement_percentile": percentiles.get(NarrativeViralityScoreType.ENGAGEMENT_SCORE, 0),
                    "reach_percentile": percentiles.get(NarrativeViralityScoreType.REACH_SCORE, 0),
                    "velocity_percentile": percentiles.get(NarrativeViralityScoreType.VELOCITY_SCORE, 0),
                    "engagement_weight": COMPOSITE_ENGAGEMENT_WEIGHT,
                    "reach_weight": COMPOSITE_REACH_WEIGHT,
                    "velocity_weight": COMPOSITE_VELOCITY_WEIGHT,
                }
                records.append((narrative_id, composite, NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY, metadata))
            await repo.bulk_insert_narrative_analysis_indicators(records)
    
    async def calculate_acceleration_rate_for_date(self, calc_date: date) -> None:
        async with self.repo() as repo:
            stats_rows = await repo.get_bulk_narrative_stats_comparison(calc_date)
            records: list[tuple[UUID, float, NarrativeAnalysisIndicatorType, dict[str, Any] | None]] = []
            for row in stats_rows:
                current_engagement = (
                    row["current_likes"] * VIRALITY_SCORE_LIKES_WEIGHT
                    + row["current_comments"] * VIRALITY_SCORE_COMMENTS_WEIGHT
                ) / row["current_views"] if row["current_views"] > 0 else 0.0

                prev_engagement = (
                    row["prev_likes"] * VIRALITY_SCORE_LIKES_WEIGHT
                    + row["prev_comments"] * VIRALITY_SCORE_COMMENTS_WEIGHT
                ) / row["prev_views"] if row["prev_views"] > 0 else 0.0

                # When there is no previous-period baseline the percent-change
                # is undefined; we treat it as 0 instead of the previous 1.0
                # fallback, which was conflating "freshly observed" with "100%
                # growth" and silently inflating accel for new narratives.
                #
                # The per-dimension change is capped at ACCELERATION_CHANGE_CAP
                # so that a single video with a 1-view baseline can't push
                # change_views into the thousands and drown the weighting.
                change_engagement = min(
                    (current_engagement - prev_engagement) / prev_engagement
                    if prev_engagement > 0 else 0.0,
                    ACCELERATION_CHANGE_CAP,
                )
                change_video_count = min(
                    (row["current_video_count"] - row["prev_video_count"]) / row["prev_video_count"]
                    if row["prev_video_count"] > 0 else 0.0,
                    ACCELERATION_CHANGE_CAP,
                )
                change_views = min(
                    (row["current_views"] - row["prev_views"]) / row["prev_views"]
                    if row["prev_views"] > 0 else 0.0,
                    ACCELERATION_CHANGE_CAP,
                )

                acceleration_rate = (
                    change_engagement * ACCELERATION_ENGAGEMENT_WEIGHT
                    + change_video_count * ACCELERATION_VIDEO_VOLUME_WEIGHT
                    + change_views * ACCELERATION_VIEWS_WEIGHT
                )
                records.append((
                    row["narrative_id"],
                    acceleration_rate,
                    NarrativeAnalysisIndicatorType.ACCELERATION_RATE,
                    {
                        "change_engagement": change_engagement,
                        "change_video_count": change_video_count,
                        "change_views": change_views,
                        "engagement_weight": ACCELERATION_ENGAGEMENT_WEIGHT,
                        "video_volume_weight": ACCELERATION_VIDEO_VOLUME_WEIGHT,
                        "views_weight": ACCELERATION_VIEWS_WEIGHT,
                    },
                ))
            await repo.bulk_insert_narrative_analysis_indicators(records)
    
    async def update_narrative_alert_levels(self, calc_date: date) -> int:
        """
        Classify each narrative based on today's composite_virality and acceleration_rate
        and persist the result in the alert_level column.
        Returns the number of narratives updated.
        """

        async with self.repo() as repo:
            indicators = await repo.get_bulk_analysis_indicators_for_date(calc_date)
            records: list[tuple] = []
            for narrative_id, values in indicators.items():
                composite = values.get("composite_virality", 0.0)
                acceleration = values.get("acceleration_rate", 0.0)

                if composite > 0.85 and acceleration > 1.0:
                    level = NarrativeAlertLevel.VIRAL
                elif composite < 0.65 and acceleration > 2.0:
                    level = NarrativeAlertLevel.EARLY_SURGE
                elif composite > 0.70 and acceleration > 1.5:
                    level = NarrativeAlertLevel.ALERT
                elif composite > 0.55 and acceleration > 1.2:
                    level = NarrativeAlertLevel.WATCH
                elif composite > 0.85:
                    # Plateaued-but-popular: very high composite (top ~15%)
                    # without active acceleration. Without this branch these
                    # narratives slot into NONE alongside truly inactive ones,
                    # which loses signal for the editorial team.
                    level = NarrativeAlertLevel.WATCH
                else:
                    level = NarrativeAlertLevel.NONE

                records.append((narrative_id, level))

            if records:
                await repo.bulk_update_narrative_alert_levels(records)
            return len(records)

    async def run_narrative_analysis_indicators_pipeline(
        self,
        batch_size: int = 100,
        hours: int = 24,
        calc_date: date | None = None,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> tuple[int, int]:
        """
        Run the full narrative analysis indicators pipeline:
          1. Calculate per-narrative virality scores (engagement, reach, velocity) in batches.
          2. Compute composite virality and acceleration rate indicators for the day.
          3. Classify and persist alert levels for every narrative.

        Args:
            batch_size: Number of narratives to process per batch.
            hours: Time window used to scope which narratives are considered.
            calc_date: Date to use for indicator/alert calculations (defaults to today).
            on_progress: Optional callback invoked after each batch with
                         (total_processed, errors) so callers can report progress.

        Returns:
            (total_processed, errors) counts from phase 1.
        """
        target_date = calc_date or date.today()

        average_views = await self.get_average_views_for_all_narratives()

        # Phase 1 — per-narrative virality scores
        offset = 0
        total_processed = 0
        errors = 0
        while True:
            narratives = await self.get_prevalent_narratives_summary(
                limit=batch_size, offset=offset, hours=hours
            )
            if not narratives:
                break

            for narrative in narratives:
                try:
                    await self.calculate_narrative_virality_scores(
                        narrative.id, average_views=average_views
                    )
                    total_processed += 1
                except Exception as e:
                    logger.error(f"Error calculating virality for narrative {narrative.id}: {e}")
                    errors += 1

            if on_progress:
                on_progress(total_processed, errors)

            offset += batch_size

        # Phase 2 — composite indicators
        await self.calculate_composite_virality_for_date(calc_date=target_date)
        await self.calculate_acceleration_rate_for_date(calc_date=target_date)

        # Phase 3 — alert level classification
        await self.update_narrative_alert_levels(calc_date=target_date)

        return total_processed, errors

    async def get_narrative_analysis_indicators(
        self, narrative_id: UUID, date: date | None = None
    ) -> NarrativeAnalysisIndicatorsResponse | None:
        async with self.repo() as repo:
            if date is not None:
                date_from = datetime.combine(date, datetime.min.time())
                date_to = datetime.combine(date, datetime.max.time())
            else:
                date_from = None
                date_to = None
            rows = await repo.get_narrative_analysis_indicators(narrative_id, date_from, date_to)

        if not rows:
            return None

        by_type = {row["indicator_type"]: row for row in rows}
        if NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY not in by_type \
                or NarrativeAnalysisIndicatorType.ACCELERATION_RATE not in by_type:
            return None

        cv = by_type[NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY]
        ar = by_type[NarrativeAnalysisIndicatorType.ACCELERATION_RATE]

        return NarrativeAnalysisIndicatorsResponse(
            narrative_id=narrative_id,
            composite_virality=AnalysisIndicator(
                id=cv["id"],
                indicator_value=cv["indicator_value"],
                indicator_type=NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY,
                calculated_at=cv["calculated_at"],
                metadata=cv["metadata"],
            ),
            acceleration_rate=AnalysisIndicator(
                id=ar["id"],
                indicator_value=ar["indicator_value"],
                indicator_type=NarrativeAnalysisIndicatorType.ACCELERATION_RATE,
                calculated_at=ar["calculated_at"],
                metadata=ar["metadata"],
            ),
            date=cv["calculated_at"].date(),
        )

    async def delete_claim_from_narrative(self, narrative_id: UUID, claim_id: UUID) -> None:
        async with self.repo() as repo:
            narrative = await repo.get_narrative(narrative_id)
            if not narrative:
                raise ValueError("narrative not found")

            if not any(claim.id == claim_id for claim in narrative.claims):
                raise ValueError("claim not associated with narrative")

            # The external API identifies narratives by their own id (stored in
            # metadata.narrative_id), not by our local narrative_id. Resolve it
            # first, mirroring _delete_external_narrative / _sync_external_narrative.
            external_narrative_id = narrative.metadata.get("narrative_id")
            if _api.is_configured() and external_narrative_id:
                response = await _api.delete_claim_on_narrative(
                    external_narrative_id, claim_id
                )
                if response.status_code == 404:
                    logger.info(
                        f"Narrative {external_narrative_id} or claim {claim_id} not found "
                        "on external API, continuing with local delete"
                    )
                elif response.status_code >= 400:
                    logger.error(
                        f"External API delete error: status={response.status_code}, "
                        f"response={response.text}"
                    )
                    response.raise_for_status()
                else:
                    logger.info(
                        f"Deleted claim {claim_id} from narrative {external_narrative_id} "
                        "on external API"
                    )
            elif _api.is_configured():
                logger.warning(
                    f"Narrative {narrative_id} has no external narrative_id in metadata; "
                    "skipping external claim delete (local delete only)"
                )

            await repo.delete_claim_from_narrative(narrative_id, claim_id)

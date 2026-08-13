import logging
from bisect import bisect_left
from datetime import date, datetime, timedelta
from typing import Any, AsyncContextManager, Callable
from uuid import UUID

from core.config import (
    ACCELERATION_ENGAGEMENT_WEIGHT,
    ACCELERATION_VIEWS_WEIGHT,
    SPREAD_ACCEL_HI,
    SPREAD_ACCEL_LO,
    SPREAD_ACCEL_MID,
    SPREAD_COMPOSITE_HI,
    SPREAD_COMPOSITE_LO,
    SPREAD_COMPOSITE_MID,
    COMPOSITE_ENGAGEMENT_WEIGHT,
    COMPOSITE_REACH_WEIGHT,
    VIRALITY_SCORE_COMMENTS_WEIGHT,
    VIRALITY_SCORE_LIKES_WEIGHT,
)
from core.entities.service import EntityService
from core.models import Claim, Narrative, NarrativeSpreadPattern, Video
from core.narratives.api import NarrativesApiClient
from core.narratives.models import (
    AnalysisIndicator,
    NarrativeAnalysisIndicatorsResponse,
    NarrativeAnalysisIndicatorType,
    NarrativeDetail,
    NarrativeInput,
    NarrativeListItem,
    NarrativePatchInput,
    NarrativeStats,
    NarrativeSummary,
    NarrativeViralityScoreType,
    ViralNarrativeSummary,
)
from core.narratives.repo import NarrativeRepository
from core.uow import ConnectionFactory, uow

logger = logging.getLogger(__name__)

_api = NarrativesApiClient()

# The weights, the baseline age bound and the six region boundaries are read from the
# environment (see core.config for values, defaults and tuning notes) and imported at
# the top of this module.
#
# THE WHOLE DESIGN IN TWO SENTENCES (docs/narrative-spread-pattern-redesign.md):
#
#   C1  We only rank what we measured. A narrative we did not look at is *unmeasured*,
#       not *quiet*; it is excluded from the ranking, never ranked as the least-active
#       one. This is why the two axes have different cohorts.
#   C2  We measure exactly two things: how viral a narrative already is (a STATE, on
#       composite) and how fast that virality is changing (a RATE, on acceleration).
#       Every state signal on composite, every change signal on acceleration, nothing
#       straddling. `velocity` used to straddle, and that mixing is what made movers
#       look like noise.
#
# Both axes are compared by their PERCENT_RANK within their own cohort, never by raw
# values. The two are not on comparable scales — composite is a bounded blend of
# percentile ranks, acceleration is an unbounded ratio whose median is near zero — and
# a rank is self-calibrating against a scraper whose coverage drifts over time.

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
        spread_patterns: list[str] | None = None,
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
                spread_patterns=spread_patterns,
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
                spread_patterns=spread_patterns,
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

    async def calculate_narrative_virality_scores(self, calc_date: date) -> int:
        """
        Score the virality-state axis for every measured narrative, in one pass.

        Writes an engagement_score and a reach_score row per narrative. `velocity_score`
        is no longer computed: it is a *change* measure, and C2 puts every change signal
        on the acceleration axis. Keeping it on the level axis double-counted growth —
        which is precisely what made the two axes look independent when they were not.

        Returns the number of narratives scored.
        """
        async with self.repo() as repo:
            cohort = await repo.get_composite_cohort(calc_date)
            records: list[tuple[UUID, float, NarrativeViralityScoreType, dict[str, Any] | None]] = []
            for row in cohort:
                records.append((
                    row["narrative_id"],
                    row["engagement_score"],
                    NarrativeViralityScoreType.ENGAGEMENT_SCORE,
                    {
                        "likes": row["likes"],
                        "comments": row["comments"],
                        "views": row["views"],
                        "likes_weight": VIRALITY_SCORE_LIKES_WEIGHT,
                        "comments_weight": VIRALITY_SCORE_COMMENTS_WEIGHT,
                    },
                ))
                records.append((
                    row["narrative_id"],
                    row["reach_score"],
                    NarrativeViralityScoreType.REACH_SCORE,
                    {
                        "views": row["views"],
                        "video_count": row["video_count"],
                    },
                ))
            if records:
                await repo.bulk_insert_narrative_virality_scores(records, calc_date=calc_date)
            return len(cohort)

    @staticmethod
    def _percent_ranks(values: list[float]) -> list[float]:
        """
        PERCENT_RANK over `values`, matching Postgres: (rank - 1) / (rows - 1), so the
        minimum scores 0.0 and the maximum 1.0. Ties share the lowest rank. A single
        row scores 0.0, as it does in SQL.
        """
        if len(values) < 2:
            return [0.0] * len(values)
        ordered = sorted(values)
        denominator = len(values) - 1
        return [bisect_left(ordered, value) / denominator for value in values]

    @staticmethod
    def _attach_percentiles(
        records: list[tuple[UUID, float, NarrativeAnalysisIndicatorType, dict[str, Any] | None]],
    ) -> None:
        """
        A percentile is only meaningful relative to the rest of the run's cohort, so
        it can only be filled in once every narrative in the run has been scored.
        """
        percentiles = NarrativeService._percent_ranks([record[1] for record in records])
        for record, percentile in zip(records, percentiles, strict=True):
            metadata = record[3]
            if metadata is not None:
                metadata["percentile"] = percentile

    async def calculate_composite_virality_for_date(self, calc_date: date) -> None:
        async with self.repo() as repo:
            all_percentiles = await repo.get_all_virality_percentiles_for_date(calc_date)
            records: list[tuple[UUID, float, NarrativeAnalysisIndicatorType, dict[str, Any] | None]] = []
            for narrative_id, ranks in all_percentiles.items():
                engagement = ranks.get(NarrativeViralityScoreType.ENGAGEMENT_SCORE)
                reach = ranks.get(NarrativeViralityScoreType.REACH_SCORE)
                composite = (
                    (engagement.percentile if engagement else 0) * COMPOSITE_ENGAGEMENT_WEIGHT
                    + (reach.percentile if reach else 0) * COMPOSITE_REACH_WEIGHT
                )
                # The raw scores ride along with the ranks: the detail view headlines the
                # narrative's own size and keeps the rank as the line beneath it, which a
                # percentile alone cannot support.
                metadata = {
                    "engagement_percentile": engagement.percentile if engagement else 0,
                    "reach_percentile": reach.percentile if reach else 0,
                    "engagement_weight": COMPOSITE_ENGAGEMENT_WEIGHT,
                    "reach_weight": COMPOSITE_REACH_WEIGHT,
                    "engagement_score": engagement.score if engagement else None,
                    "reach_score": reach.score if reach else None,
                }
                records.append((narrative_id, composite, NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY, metadata))
            self._attach_percentiles(records)
            await repo.bulk_insert_narrative_analysis_indicators(records, calc_date=calc_date)

    async def calculate_acceleration_rate_for_date(self, calc_date: date) -> None:
        """
        Score the change-in-virality axis over the narratives we actually visited today.

        Each component is a growth RATE — per elapsed day, per D4. The repository has
        already divided each video's view gain by the days since that video was last
        fetched, so a video last seen four days ago no longer contributes four days of
        growth as if it were one and rank high for having gone unmeasured.

        There is deliberately no cap on any component. The old ACCELERATION_CHANGE_CAP
        existed because an unnormalised gain from a 1-view baseline could reach the
        thousands; dividing by the elapsed gap and ranking the result (never the raw
        value) removes the need for a magic clamp.

        Nor is there a bound on how old a baseline may be. Growth per day is the whole
        normalisation: a twenty-day gap is divided by twenty exactly as a four-day gap
        is divided by four.

        The three components are RANKED across the cohort and the ranks are blended, not
        the raw values. See the weights in config.py for why: a weighted sum of raw
        components distributes influence by scale rather than by weight, and these three
        do not share a scale. Ranking also retires the max(0.0, ...) floor that used to
        sit on the blended rate — a decliner now ranks *below* the flat on the component
        it declined in, instead of being merged with them, which is the open question the
        floor's comment described.
        """
        async with self.repo() as repo:
            stats_rows = await repo.get_acceleration_cohort(calc_date)
            components: list[dict[str, Any]] = []
            for row in stats_rows:
                mean_gap = row["mean_gap_days"]

                # Denominators are the BALANCED PANEL — every linked video seen on both
                # days — while the numerator is the movement of the refreshed ones. The
                # result is the narrative's own daily growth with unmeasured videos held
                # flat, which is a lower bound rather than a sample extrapolated to the
                # whole. See get_acceleration_cohort for the measurement that forced it.
                panel_prev_views = row["panel_prev_views"]
                panel_cur_views = row["panel_cur_views"]
                change_views = (
                    row["daily_view_gain"] / panel_prev_views if panel_prev_views > 0 else 0.0
                )

                # Engagement: a quality modifier, not a growth term. Measured over the
                # same panel as views, on both sides. Mixing the two populations here is
                # the trap this rewrite exists to avoid — a panel denominator against a
                # refreshed-subset numerator inflates the ratio by 1/coverage, which for
                # a narrative covered at 0.62% turns a +11% engagement change into +180.
                prev_engagement = (
                    (row["panel_prev_likes"] * VIRALITY_SCORE_LIKES_WEIGHT
                     + row["panel_prev_comments"] * VIRALITY_SCORE_COMMENTS_WEIGHT) / panel_prev_views
                    if panel_prev_views > 0 else 0.0
                )
                cur_engagement = (
                    (row["panel_cur_likes"] * VIRALITY_SCORE_LIKES_WEIGHT
                     + row["panel_cur_comments"] * VIRALITY_SCORE_COMMENTS_WEIGHT) / panel_cur_views
                    if panel_cur_views > 0 else 0.0
                )
                change_engagement = (
                    ((cur_engagement - prev_engagement) / prev_engagement) / mean_gap
                    if prev_engagement > 0 and mean_gap > 0 else 0.0
                )

                # Video count is DESCRIPTIVE ONLY — it is not a component of the rate.
                # It measures how many videos we have linked to the narrative today
                # against yesterday, which is the scraper's progress rather than the
                # narrative's, and as a scored term it was the single largest source of
                # wrong `viral` badges (see config.py for the measurements that retired
                # it). It stays in the metadata because "2 → 3 videos" is a fact worth
                # showing a reader beside the rate; it is simply not ranked on.
                prev_videos = row["prev_videos"]
                change_video_count = (
                    (row["cur_videos"] - prev_videos) / prev_videos if prev_videos > 0 else 0.0
                )

                refreshed_baseline = row["baseline_views"]
                components.append({
                    "narrative_id": row["narrative_id"],
                    "change_engagement": change_engagement,
                    "change_video_count": change_video_count,
                    "change_views": change_views,
                    "refreshed_videos": row["refreshed_videos"],
                    "mean_gap_days": mean_gap,
                    "panel_videos": prev_videos,
                    "cur_videos": row["cur_videos"],
                    "prev_videos": prev_videos,
                    "panel_baseline_views": panel_prev_views,
                    "refreshed_baseline_views": refreshed_baseline,
                    # How much of the narrative the day's number actually saw. The one
                    # figure a reader needs to weigh everything above, and the one the
                    # old formula spent without ever reporting.
                    "coverage": (
                        refreshed_baseline / panel_prev_views if panel_prev_views > 0 else 0.0
                    ),
                })

            # Ranks are only meaningful against the rest of the cohort, so every
            # component is ranked once the whole cohort is scored — the same two-pass
            # shape _attach_percentiles uses for the blended rate below.
            ranks = {
                key: self._percent_ranks([component[key] for component in components])
                for key in ("change_views", "change_engagement")
            }

            records: list[tuple[UUID, float, NarrativeAnalysisIndicatorType, dict[str, Any] | None]] = []
            for index, component in enumerate(components):
                views_rank = ranks["change_views"][index]
                engagement_rank = ranks["change_engagement"][index]
                acceleration_rate = (
                    engagement_rank * ACCELERATION_ENGAGEMENT_WEIGHT
                    + views_rank * ACCELERATION_VIEWS_WEIGHT
                )
                narrative_id = component.pop("narrative_id")
                records.append((
                    narrative_id,
                    acceleration_rate,
                    NarrativeAnalysisIndicatorType.ACCELERATION_RATE,
                    {
                        **component,
                        # The raw components stay in the metadata beside their ranks: the
                        # rank is what the badge read, the raw value is what the panel
                        # can show a reader as a growth figure. change_video_count has a
                        # raw value and no rank, because it is reported and not scored.
                        "views_percentile": views_rank,
                        "engagement_percentile": engagement_rank,
                        "engagement_weight": ACCELERATION_ENGAGEMENT_WEIGHT,
                        "views_weight": ACCELERATION_VIEWS_WEIGHT,
                    },
                ))
            self._attach_percentiles(records)
            await repo.bulk_insert_narrative_analysis_indicators(records, calc_date=calc_date)

    @staticmethod
    def _classify(
        composite: float, acceleration: float, views_measured: bool
    ) -> NarrativeSpreadPattern | None:
        """
        Place a narrative on the percentile plane. Returns None for the no-badge region.

        The four regions are RECTANGLES, not quadrants, and two consequences of that are
        easy to get wrong:

          - `early_surge` is for SMALL narratives only. It is not "anything climbing":
            a large climber is `viral` if it clears the top of both axes and `trending`
            otherwise. The composite ceiling is the whole point of the label.
          - The labels do not tile the plane. Small AND flat — the bottom-left — gets no
            badge at all.

        Order matters: `viral` is carved out of the `trending` box, so it is tested
        first. Every boundary compares one axis to a constant on that same axis, never
        to the other axis's percentile, which is what lets the two axes rank over
        different cohorts without needing a shared denominator.

        `views_measured` is NOT a third boundary — it is an evidence precondition on the
        strongest label, and it leaves the geometry above untouched. It was added when a
        narrative whose scraper footprint went from 2 linked videos to 3 scored
        0.35 * 0.5 = 0.175 and ranked in the top 3.5% of the 2026-08-12 cohort with no
        view measurement behind it at all; the video-volume term that made that possible
        has since been removed from the rate entirely (config.py).

        The gate outlived that term, because removing it does not make a zero-coverage
        narrative measurable — it only stops it from ranking high. A narrative with
        nothing re-fetched now sits at the bottom of the views component along with
        everything else that did not move, which is a tie between "flat" and "unseen"
        that the rate cannot break. `viral` is the one label asserting a narrative is
        spreading *now*, so it is the one that must be paid for with an observation:
        at least one video re-fetched inside the window, i.e. `refreshed_videos > 0`.

        The other three labels do not need the gate. `consolidated` and `trending` claim
        no more than the rate can support, and `early_surge` sits under the composite
        ceiling where the cost of a false positive is small. A blocked `viral` falls
        through to `trending`, which is the same claim minus the part we could not
        evidence, rather than to no badge.
        """
        if composite >= SPREAD_COMPOSITE_HI and acceleration >= SPREAD_ACCEL_HI and views_measured:
            return NarrativeSpreadPattern.VIRAL
        if composite <= SPREAD_COMPOSITE_LO and acceleration >= SPREAD_ACCEL_MID:
            return NarrativeSpreadPattern.EARLY_SURGE
        if composite >= SPREAD_COMPOSITE_MID and acceleration <= SPREAD_ACCEL_LO:
            return NarrativeSpreadPattern.CONSOLIDATED
        if composite >= SPREAD_COMPOSITE_LO and acceleration >= SPREAD_ACCEL_LO:
            return NarrativeSpreadPattern.TRENDING
        return None

    async def update_narrative_spread_patterns(self, calc_date: date) -> int:
        """
        Classify each narrative on the percentile ranks of BOTH axes and persist the
        result in the spread_pattern column.

            viral         composite >= 0.80  and  acceleration >= 0.80
                          and at least one video actually re-fetched in the window
            early_surge   composite <= 0.40  and  acceleration >= 0.50
            consolidated  composite >= 0.50  and  acceleration <= 0.40
            trending      composite >= 0.40  and  acceleration >= 0.40
            (no badge)    everything else — small and flat

        Only the acceleration cohort is classifiable. Every label makes a claim about
        today ("climbing", "flat"), and we cannot make that claim about a narrative we
        did not look at. Composite's much larger pool exists to give a *stable rank*,
        not to badge more narratives: ranking size against the whole corpus is what
        stops a narrative's "how big am I" answer from swinging with whoever else
        happened to get scraped.

        A narrative that was visited but did not grow stays in, ranked at an honest
        zero — that is required rather than tolerated, because `consolidated` means big
        *and flat*, so a large narrative that genuinely stopped growing must be able to
        reach it. What is excluded is the *unmeasured*, which merely wears a zero
        because its carried-forward snapshot is identical on both days.

        Narratives in the no-badge region, and any that cannot be scored, have their
        spread_pattern cleared rather than left holding a stale badge.

        Returns the number of narratives classified (badged or explicitly cleared).
        """

        async with self.repo() as repo:
            indicators = await repo.get_bulk_analysis_indicators_for_date(calc_date)
            badged: list[tuple[UUID, NarrativeSpreadPattern]] = []
            scored: list[UUID] = []
            for narrative_id, values in indicators.items():
                composite_indicator = values.get("composite_virality")
                acceleration_indicator = values.get("acceleration_rate")
                if composite_indicator is None or acceleration_indicator is None:
                    continue

                composite = composite_indicator["metadata"].get("percentile")
                acceleration = acceleration_indicator["metadata"].get("percentile")
                if composite is None or acceleration is None:
                    continue

                # Absent key means a row written before the redesign, which recorded no
                # coverage at all. Unknown coverage is not evidence of coverage, so it
                # reads as unmeasured and the row cannot reach `viral` — the same C1
                # rule the rest of this method follows.
                refreshed_videos = acceleration_indicator["metadata"].get("refreshed_videos") or 0

                scored.append(narrative_id)
                pattern = self._classify(composite, acceleration, refreshed_videos > 0)
                if pattern is not None:
                    badged.append((narrative_id, pattern))

            if badged:
                await repo.bulk_update_narrative_spread_patterns(badged)
            if scored:
                # Everything outside the badged set loses its badge — including the
                # no-badge region, which is an absence rather than a pattern.
                await repo.clear_spread_patterns_except([narrative_id for narrative_id, _ in badged])
            return len(scored)

    async def run_narrative_analysis_indicators_pipeline(
        self,
        batch_size: int = 100,
        hours: int = 24,
        calc_date: date | None = None,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> tuple[int, int]:
        """
        Run the full narrative analysis indicators pipeline:
          1. Score the virality-state axis for every measured narrative.
          2. Compute composite virality and acceleration rate indicators for the day.
          3. Classify and persist spread patterns.

        Args:
            batch_size: Retained for API compatibility; phase 1 is a single bulk query
                        and no longer paginates. The per-narrative loop it replaced
                        opened two nested transactions and issued three inserts per
                        narrative, which the expanded composite cohort (~22k rather than
                        the ~2k the dashboard query returned) would have turned into
                        ~44k transactions per run.
            hours: Retained for API compatibility; no longer used. The old pipeline
                   scoped phase 1 with the dashboard's pagination query, whose `NOW()`
                   anchor and 24-hour window made the composite cohort a function of
                   when the job ran. Composite now ranks over every narrative measured
                   at least once, which is both the design and a cheaper query.
            calc_date: Date to use for indicator/alert calculations. Defaults to
                       yesterday — the last *completed* scraping day. Acceleration
                       compares carried-forward stats "as of calc_date" against "as of
                       calc_date - 1", so the difference is driven entirely by videos
                       scraped on calc_date itself. Defaulting to today would run
                       against a day that has barely been scraped (the job fires just
                       after midnight UTC), leaving current == prev and acceleration
                       == 0 for almost every narrative.
            on_progress: Optional callback invoked once with (total_processed, errors).

        Returns:
            (total_processed, errors) counts from phase 1.
        """
        target_date = calc_date or (date.today() - timedelta(days=1))

        # Phase 1 — virality-state scores over every measured narrative
        total_processed = 0
        errors = 0
        try:
            total_processed = await self.calculate_narrative_virality_scores(
                calc_date=target_date
            )
        except Exception as e:
            logger.error(f"Error calculating virality scores for {target_date}: {e}")
            errors = 1

        if on_progress:
            on_progress(total_processed, errors)

        # Phase 2 — the two axes
        await self.calculate_composite_virality_for_date(calc_date=target_date)
        await self.calculate_acceleration_rate_for_date(calc_date=target_date)

        # Phase 3 — spread pattern classification
        await self.update_narrative_spread_patterns(calc_date=target_date)

        return total_processed, errors

    async def get_narrative_analysis_indicators(
        self, narrative_id: UUID, date: date | None = None
    ) -> NarrativeAnalysisIndicatorsResponse | None:
        """
        The two axes answer to different evidence, so they arrive independently.

        Composite is a level and is scored for every narrative with any stats ever
        (~22k); acceleration is a rate and is scored only for the narratives visited on
        the day (~2k). Requiring both would therefore hide the composite of roughly
        nine narratives in ten behind a null response — reporting "we have measured
        nothing" for a narrative whose size we know perfectly well, which is exactly the
        conflation of *unmeasured* with *quiet* that D0 exists to prevent.

        So composite is required and acceleration is optional. A caller that gets
        `acceleration_rate=None` should read it as "not re-measured on this date", never
        as zero.
        """
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

        # rows are ordered by calculated_at DESC, so the first row per type is the
        # most recent; setdefault keeps it and ignores older same-day reruns.
        by_type: dict[str, Any] = {}
        for row in rows:
            by_type.setdefault(row["indicator_type"], row)
        if NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY not in by_type:
            return None

        cv = by_type[NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY]
        ar = by_type.get(NarrativeAnalysisIndicatorType.ACCELERATION_RATE)

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
            ) if ar is not None else None,
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

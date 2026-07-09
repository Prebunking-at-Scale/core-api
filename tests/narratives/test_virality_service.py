"""
Mock-only tests for the virality scoring pipeline in NarrativeService.

No database is used. Repo and service sub-methods are replaced with AsyncMocks so
that every input is fully controlled and every expected output can be derived by hand.

Patrón de mock para el repo (async context manager):

    mock_repo = AsyncMock()
    mock_repo.<method>.return_value = <value>
    mock_cm = AsyncMock()
    mock_cm.__aenter__.return_value = mock_repo
    mock_cm.__aexit__.return_value = None

    with patch.object(service, "repo", return_value=mock_cm):
        result = await service.<method>(...)
"""
import uuid
from datetime import date
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from core.models import NarrativeAlertLevel
from core.narratives.models import (
    NarrativeAnalysisIndicatorType,
    NarrativeStats,
    NarrativeStatsTotals,
    NarrativeSummary,
    NarrativeViralityScoreType,
)
from core.narratives.service import INDICATOR_WINDOW_HOURS, NarrativeService
from tests.narratives.conftest import NarrativeSummaryFactory


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_repo_cm(mock_repo: AsyncMock) -> AsyncMock:
    """Wrap a mock repo in an async context manager mock."""
    cm = AsyncMock()
    cm.__aenter__.return_value = mock_repo
    cm.__aexit__.return_value = None
    return cm


def _make_narrative_stats(views: int, likes: int, comments: int, video_count: int = 1) -> NarrativeStats:
    narrative_id = uuid.uuid4()
    return NarrativeStats(
        narrative_id=narrative_id,
        totals=NarrativeStatsTotals(views=views, likes=likes, comments=comments, video_count=video_count),
    )


# ---------------------------------------------------------------------------
# Section 1 — calculate_narrative_virality_scores
# ---------------------------------------------------------------------------

class TestCalculateNarrativeViralityScores:
    """
    Weights used by the service:
        engagement  = (likes * 1 + comments * 5) / views
        reach       = min(views / avg_views, 10) / 10
        velocity    = delta_views / total_views
    """

    async def test_engagement_score_formula(self, narrative_service: NarrativeService):
        # views=1000, likes=50, comments=10 → (50 + 50) / 1000 = 0.1
        narrative_id = uuid.uuid4()
        stats = _make_narrative_stats(views=1000, likes=50, comments=10)
        delta = NarrativeStatsTotals(views=200, likes=10, comments=2)

        mock_repo = AsyncMock()
        mock_repo.get_narrative_stats_delta_for_period.return_value = delta

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)), \
             patch.object(narrative_service, "get_narrative_stats", return_value=stats):
            engagement, _, _ = await narrative_service.calculate_narrative_virality_scores(
                narrative_id, average_views=1000.0
            )

        assert engagement == pytest.approx(0.1)

    async def test_engagement_score_zero_views(self, narrative_service: NarrativeService):
        narrative_id = uuid.uuid4()
        stats = _make_narrative_stats(views=0, likes=50, comments=10)
        delta = NarrativeStatsTotals(views=0)

        mock_repo = AsyncMock()
        mock_repo.get_narrative_stats_delta_for_period.return_value = delta

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)), \
             patch.object(narrative_service, "get_narrative_stats", return_value=stats):
            engagement, reach, velocity = await narrative_service.calculate_narrative_virality_scores(
                narrative_id, average_views=1000.0
            )

        assert engagement == 0.0
        assert reach == 0.0
        assert velocity == 0.0

    async def test_reach_score_formula(self, narrative_service: NarrativeService):
        # views=2500, avg=500, cap=10 → min(2500/500, 10) / 10 = 5/10 = 0.5
        narrative_id = uuid.uuid4()
        stats = _make_narrative_stats(views=2500, likes=10, comments=2)
        delta = NarrativeStatsTotals(views=100)

        mock_repo = AsyncMock()
        mock_repo.get_narrative_stats_delta_for_period.return_value = delta

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)), \
             patch.object(narrative_service, "get_narrative_stats", return_value=stats):
            _, reach, _ = await narrative_service.calculate_narrative_virality_scores(
                narrative_id, average_views=500.0
            )

        assert reach == pytest.approx(0.5)

    async def test_reach_score_capped(self, narrative_service: NarrativeService):
        # views=100_000, avg=500 → min(200, 10) / 10 = 1.0
        narrative_id = uuid.uuid4()
        stats = _make_narrative_stats(views=100_000, likes=10, comments=2)
        delta = NarrativeStatsTotals(views=500)

        mock_repo = AsyncMock()
        mock_repo.get_narrative_stats_delta_for_period.return_value = delta

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)), \
             patch.object(narrative_service, "get_narrative_stats", return_value=stats):
            _, reach, _ = await narrative_service.calculate_narrative_virality_scores(
                narrative_id, average_views=500.0
            )

        assert reach == pytest.approx(1.0)

    async def test_reach_score_zero_average(self, narrative_service: NarrativeService):
        # avg=0 → reach=0.0
        narrative_id = uuid.uuid4()
        stats = _make_narrative_stats(views=5000, likes=10, comments=2)
        delta = NarrativeStatsTotals(views=100)

        mock_repo = AsyncMock()
        mock_repo.get_narrative_stats_delta_for_period.return_value = delta

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)), \
             patch.object(narrative_service, "get_narrative_stats", return_value=stats):
            _, reach, _ = await narrative_service.calculate_narrative_virality_scores(
                narrative_id, average_views=0.0
            )

        assert reach == 0.0

    async def test_velocity_score_formula(self, narrative_service: NarrativeService):
        # delta_views=200, total_views=1000 → 200/1000 = 0.2
        narrative_id = uuid.uuid4()
        stats = _make_narrative_stats(views=1000, likes=10, comments=2)
        delta = NarrativeStatsTotals(views=200)

        mock_repo = AsyncMock()
        mock_repo.get_narrative_stats_delta_for_period.return_value = delta

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)), \
             patch.object(narrative_service, "get_narrative_stats", return_value=stats):
            _, _, velocity = await narrative_service.calculate_narrative_virality_scores(
                narrative_id, average_views=500.0
            )

        assert velocity == pytest.approx(0.2)

    async def test_velocity_score_zero_total_views(self, narrative_service: NarrativeService):
        narrative_id = uuid.uuid4()
        stats = _make_narrative_stats(views=0, likes=0, comments=0)
        delta = NarrativeStatsTotals(views=0)

        mock_repo = AsyncMock()
        mock_repo.get_narrative_stats_delta_for_period.return_value = delta

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)), \
             patch.object(narrative_service, "get_narrative_stats", return_value=stats):
            _, _, velocity = await narrative_service.calculate_narrative_virality_scores(
                narrative_id, average_views=500.0
            )

        assert velocity == 0.0

    async def test_narrative_not_found_raises(self, narrative_service: NarrativeService):
        narrative_id = uuid.uuid4()
        mock_repo = AsyncMock()

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)), \
             patch.object(narrative_service, "get_narrative_stats", return_value=None):
            with pytest.raises(ValueError, match="narrative not found"):
                await narrative_service.calculate_narrative_virality_scores(
                    narrative_id, average_views=500.0
                )


# ---------------------------------------------------------------------------
# Section 2 — calculate_composite_virality_for_date
# ---------------------------------------------------------------------------

class TestCalculateCompositeViralityForDate:
    """
    Weights: engagement=0.5, reach=0.30, velocity=0.20
    """

    async def test_composite_all_ones(self, narrative_service: NarrativeService):
        # All percentiles = 1.0 → composite = 0.5 + 0.3 + 0.2 = 1.0
        narrative_id = uuid.uuid4()
        percentiles = {
            narrative_id: {
                NarrativeViralityScoreType.ENGAGEMENT_SCORE: 1.0,
                NarrativeViralityScoreType.REACH_SCORE: 1.0,
                NarrativeViralityScoreType.VELOCITY_SCORE: 1.0,
            }
        }
        mock_repo = AsyncMock()
        mock_repo.get_all_virality_percentiles_for_date.return_value = percentiles

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_composite_virality_for_date(date.today())

        inserted = mock_repo.bulk_insert_narrative_analysis_indicators.call_args[0][0]
        assert len(inserted) == 1
        _id, composite, indicator_type, _meta = inserted[0]
        assert _id == narrative_id
        assert composite == pytest.approx(1.0)
        assert indicator_type == NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY

    async def test_composite_partial_percentiles(self, narrative_service: NarrativeService):
        # Only engagement=0.8, others missing → 0.8 * 0.5 = 0.4
        narrative_id = uuid.uuid4()
        percentiles = {
            narrative_id: {
                NarrativeViralityScoreType.ENGAGEMENT_SCORE: 0.8,
            }
        }
        mock_repo = AsyncMock()
        mock_repo.get_all_virality_percentiles_for_date.return_value = percentiles

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_composite_virality_for_date(date.today())

        inserted = mock_repo.bulk_insert_narrative_analysis_indicators.call_args[0][0]
        _, composite, _, _ = inserted[0]
        assert composite == pytest.approx(0.4)

    async def test_composite_weighted_combination(self, narrative_service: NarrativeService):
        # engagement=0.6, reach=0.4, velocity=0.8
        # 0.6*0.5 + 0.4*0.3 + 0.8*0.2 = 0.30 + 0.12 + 0.16 = 0.58
        narrative_id = uuid.uuid4()
        percentiles = {
            narrative_id: {
                NarrativeViralityScoreType.ENGAGEMENT_SCORE: 0.6,
                NarrativeViralityScoreType.REACH_SCORE: 0.4,
                NarrativeViralityScoreType.VELOCITY_SCORE: 0.8,
            }
        }
        mock_repo = AsyncMock()
        mock_repo.get_all_virality_percentiles_for_date.return_value = percentiles

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_composite_virality_for_date(date.today())

        inserted = mock_repo.bulk_insert_narrative_analysis_indicators.call_args[0][0]
        _, composite, _, _ = inserted[0]
        assert composite == pytest.approx(0.58)

    async def test_composite_empty_percentiles(self, narrative_service: NarrativeService):
        mock_repo = AsyncMock()
        mock_repo.get_all_virality_percentiles_for_date.return_value = {}

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_composite_virality_for_date(date.today())

        inserted = mock_repo.bulk_insert_narrative_analysis_indicators.call_args[0][0]
        assert inserted == []


# ---------------------------------------------------------------------------
# Section 3 — calculate_acceleration_rate_for_date
# ---------------------------------------------------------------------------

class TestCalculateAccelerationRateForDate:
    """
    Weights: engagement=0.40, video_count=0.35, views=0.25
    Engagement = (likes*1 + comments*5) / views
    change_x = (current - prev) / prev  if prev > 0  else (1.0 if current > 0 else 0.0)
    """

    def _make_stats_row(
        self,
        narrative_id,
        current_views, current_likes, current_comments, current_video_count,
        prev_views, prev_likes, prev_comments, prev_video_count,
        paired_video_count=1.0,
    ) -> dict:
        return {
            "narrative_id": narrative_id,
            "current_views": float(current_views),
            "current_likes": float(current_likes),
            "current_comments": float(current_comments),
            "current_video_count": float(current_video_count),
            "prev_views": float(prev_views),
            "prev_likes": float(prev_likes),
            "prev_comments": float(prev_comments),
            "prev_video_count": float(prev_video_count),
            "paired_video_count": float(paired_video_count),
        }

    async def test_acceleration_rate_normal_growth(self, narrative_service: NarrativeService):
        """
        prev:    views=100, likes=10, comments=2, videos=2
        current: views=200, likes=20, comments=4, videos=3

        prev_engagement    = (10*1 + 2*5) / 100 = 20/100 = 0.2
        current_engagement = (20*1 + 4*5) / 200 = 40/200 = 0.2
        change_engagement  = (0.2 - 0.2) / 0.2  = 0.0
        change_video_count = (3 - 2) / 2         = 0.5
        change_views       = (200 - 100) / 100   = 1.0
        acceleration       = 0.0*0.40 + 0.5*0.35 + 1.0*0.25 = 0.425
        """
        narrative_id = uuid.uuid4()
        row = self._make_stats_row(
            narrative_id,
            current_views=200, current_likes=20, current_comments=4, current_video_count=3,
            prev_views=100, prev_likes=10, prev_comments=2, prev_video_count=2,
        )
        mock_repo = AsyncMock()
        mock_repo.get_bulk_narrative_stats_comparison.return_value = [row]

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_acceleration_rate_for_date(date.today())

        inserted = mock_repo.bulk_insert_narrative_analysis_indicators.call_args[0][0]
        _, acceleration, indicator_type, _ = inserted[0]
        assert indicator_type == NarrativeAnalysisIndicatorType.ACCELERATION_RATE
        assert acceleration == pytest.approx(0.425)

    async def test_acceleration_rate_no_prev_data(self, narrative_service: NarrativeService):
        """
        prev all zeros → percent-change is undefined and treated as 0 (the old
        1.0 fallback was dropped to stop conflating "freshly observed" with
        "100% growth"). Each change = 0.0 → acceleration = 0.0.
        """
        narrative_id = uuid.uuid4()
        row = self._make_stats_row(
            narrative_id,
            current_views=100, current_likes=5, current_comments=1, current_video_count=2,
            prev_views=0, prev_likes=0, prev_comments=0, prev_video_count=0,
        )
        mock_repo = AsyncMock()
        mock_repo.get_bulk_narrative_stats_comparison.return_value = [row]

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_acceleration_rate_for_date(date.today())

        inserted = mock_repo.bulk_insert_narrative_analysis_indicators.call_args[0][0]
        _, acceleration, _, _ = inserted[0]
        assert acceleration == pytest.approx(0.0)

    async def test_acceleration_rate_all_zeros(self, narrative_service: NarrativeService):
        """All zeros → no change → acceleration = 0.0"""
        narrative_id = uuid.uuid4()
        row = self._make_stats_row(
            narrative_id,
            current_views=0, current_likes=0, current_comments=0, current_video_count=0,
            prev_views=0, prev_likes=0, prev_comments=0, prev_video_count=0,
        )
        mock_repo = AsyncMock()
        mock_repo.get_bulk_narrative_stats_comparison.return_value = [row]

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_acceleration_rate_for_date(date.today())

        inserted = mock_repo.bulk_insert_narrative_analysis_indicators.call_args[0][0]
        _, acceleration, _, _ = inserted[0]
        assert acceleration == pytest.approx(0.0)

    async def test_comparison_uses_trailing_window_not_calc_date(
        self, narrative_service: NarrativeService
    ):
        """
        The comparison must be anchored to a trailing window, never to calc_date —
        that mismatch is what zeroed acceleration on the just-after-midnight run.
        """
        mock_repo = AsyncMock()
        mock_repo.get_bulk_narrative_stats_comparison.return_value = []

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_acceleration_rate_for_date(date.today(), hours=48)

        mock_repo.get_bulk_narrative_stats_comparison.assert_awaited_once_with(48)

    async def test_percentile_is_stored_in_metadata(self, narrative_service: NarrativeService):
        """
        Acceleration is classified on its rank within the cohort, so every record
        carries its PERCENT_RANK. Postgres semantics: min → 0.0, max → 1.0.
        """
        rows = [
            # change_views drives each: 0.0, 1.0, 3.0 → accel 0.0, 0.25, 0.75
            self._make_stats_row(uuid.uuid4(), 100, 0, 0, 1, 100, 0, 0, 1),
            self._make_stats_row(uuid.uuid4(), 200, 0, 0, 1, 100, 0, 0, 1),
            self._make_stats_row(uuid.uuid4(), 400, 0, 0, 1, 100, 0, 0, 1),
        ]
        mock_repo = AsyncMock()
        mock_repo.get_bulk_narrative_stats_comparison.return_value = rows

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_acceleration_rate_for_date(date.today())

        inserted = mock_repo.bulk_insert_narrative_analysis_indicators.call_args[0][0]
        percentiles = [metadata["percentile"] for _, _, _, metadata in inserted]
        assert percentiles == pytest.approx([0.0, 0.5, 1.0])

    async def test_percentile_ties_share_lowest_rank(self, narrative_service: NarrativeService):
        """Ties get the same rank, as PERCENT_RANK does in SQL."""
        assert narrative_service._percent_ranks([5.0, 1.0, 1.0]) == pytest.approx([1.0, 0.0, 0.0])

    async def test_percentile_single_narrative_is_zero(self, narrative_service: NarrativeService):
        """A lone row scores 0.0 in Postgres, not 1.0. Mirror that."""
        assert narrative_service._percent_ranks([7.3]) == [0.0]
        assert narrative_service._percent_ranks([]) == []


# ---------------------------------------------------------------------------
# Section 4 — update_narrative_alert_levels
# ---------------------------------------------------------------------------

class TestUpdateNarrativeAlertLevels:
    """
    Acceleration is compared by PERCENT_RANK within the run's cohort, not by its raw
    value. Thresholds (from service.py):
        composite > 0.85 AND accel_pct > 0.90   → VIRAL
        composite < 0.65 AND accel_pct > 0.95   → EARLY_SURGE
        composite > 0.70 AND accel_pct > 0.90   → ALERT
        composite > 0.55 AND accel_pct > 0.75   → WATCH
        composite > 0.85                        → WATCH (plateaued-but-popular)
        else                                    → NONE
    Note: conditions are evaluated top-to-bottom (if/elif chain).
    """

    @staticmethod
    def _indicators(composite: float | None, accel_pct: float | None) -> dict:
        """Build one narrative's indicator payload as the repo now returns it."""
        values: dict = {}
        if composite is not None:
            values["composite_virality"] = {"value": composite, "metadata": {}}
        if accel_pct is not None:
            values["acceleration_rate"] = {"value": 0.42, "metadata": {"percentile": accel_pct}}
        return values

    async def _run(self, narrative_service, indicators: dict):
        mock_repo = AsyncMock()
        mock_repo.get_bulk_analysis_indicators_for_date.return_value = indicators

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            count = await narrative_service.update_narrative_alert_levels(date.today())

        if not mock_repo.bulk_update_narrative_alert_levels.called:
            return [], count
        return mock_repo.bulk_update_narrative_alert_levels.call_args[0][0], count

    async def test_alert_level_viral(self, narrative_service: NarrativeService):
        nid = uuid.uuid4()
        records, _ = await self._run(narrative_service, {nid: self._indicators(0.90, 0.95)})
        assert records[0] == (nid, NarrativeAlertLevel.VIRAL)

    async def test_alert_level_early_surge(self, narrative_service: NarrativeService):
        nid = uuid.uuid4()
        records, _ = await self._run(narrative_service, {nid: self._indicators(0.60, 0.97)})
        assert records[0] == (nid, NarrativeAlertLevel.EARLY_SURGE)

    async def test_alert_level_alert(self, narrative_service: NarrativeService):
        nid = uuid.uuid4()
        records, _ = await self._run(narrative_service, {nid: self._indicators(0.75, 0.93)})
        assert records[0] == (nid, NarrativeAlertLevel.ALERT)

    async def test_alert_level_watch(self, narrative_service: NarrativeService):
        nid = uuid.uuid4()
        records, _ = await self._run(narrative_service, {nid: self._indicators(0.60, 0.80)})
        assert records[0] == (nid, NarrativeAlertLevel.WATCH)

    async def test_alert_level_watch_plateaued(self, narrative_service: NarrativeService):
        """High composite, no acceleration: still surfaced rather than lost in NONE."""
        nid = uuid.uuid4()
        records, _ = await self._run(narrative_service, {nid: self._indicators(0.90, 0.10)})
        assert records[0] == (nid, NarrativeAlertLevel.WATCH)

    async def test_alert_level_none(self, narrative_service: NarrativeService):
        nid = uuid.uuid4()
        records, _ = await self._run(narrative_service, {nid: self._indicators(0.40, 0.50)})
        assert records[0] == (nid, NarrativeAlertLevel.NONE)

    async def test_missing_composite_is_not_classified(self, narrative_service: NarrativeService):
        """
        A narrative with no composite used to be read as composite=0.0, which
        satisfies `composite < 0.65` and made it EARLY_SURGE on acceleration alone.
        Absence of data must not be a signal — it is skipped entirely.
        """
        nid = uuid.uuid4()
        records, count = await self._run(narrative_service, {nid: self._indicators(None, 0.99)})
        assert records == []
        assert count == 0

    async def test_missing_acceleration_is_not_classified(self, narrative_service: NarrativeService):
        nid = uuid.uuid4()
        records, count = await self._run(narrative_service, {nid: self._indicators(0.90, None)})
        assert records == []
        assert count == 0

    async def test_acceleration_without_percentile_is_not_classified(
        self, narrative_service: NarrativeService
    ):
        """An acceleration row written before this change has no percentile."""
        nid = uuid.uuid4()
        indicators = {nid: {
            "composite_virality": {"value": 0.90, "metadata": {}},
            "acceleration_rate": {"value": 1.5, "metadata": {}},
        }}
        records, count = await self._run(narrative_service, indicators)
        assert records == []
        assert count == 0

    async def test_unscoreable_narratives_have_their_badge_cleared(
        self, narrative_service: NarrativeService
    ):
        """Narratives outside the scored cohort must not keep yesterday's badge."""
        nid = uuid.uuid4()
        mock_repo = AsyncMock()
        mock_repo.get_bulk_analysis_indicators_for_date.return_value = {
            nid: self._indicators(0.40, 0.50)
        }

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.update_narrative_alert_levels(date.today())

        mock_repo.clear_alert_levels_except.assert_awaited_once_with([nid])

    async def test_returns_count_of_updated_narratives(self, narrative_service: NarrativeService):
        indicators = {uuid.uuid4(): self._indicators(0.3, 0.2) for _ in range(5)}
        _, count = await self._run(narrative_service, indicators)
        assert count == 5

    async def test_no_bulk_update_when_empty(self, narrative_service: NarrativeService):
        mock_repo = AsyncMock()
        mock_repo.get_bulk_analysis_indicators_for_date.return_value = {}

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            count = await narrative_service.update_narrative_alert_levels(date.today())

        mock_repo.bulk_update_narrative_alert_levels.assert_not_called()
        # An empty cohort means the run found nothing, not that every badge is stale.
        mock_repo.clear_alert_levels_except.assert_not_called()
        assert count == 0


# ---------------------------------------------------------------------------
# Section 5 — run_narrative_analysis_indicators_pipeline
# ---------------------------------------------------------------------------

class TestRunNarrativeAnalysisIndicatorsPipeline:
    """
    Patches service sub-methods so the pipeline orchestration is tested
    without triggering any real DB calls.
    """

    def _patch_sub_methods(self, service, narratives_pages: list[list[NarrativeSummary]]):
        """
        Returns a dict of AsyncMock patches to apply via patch.object.
            narratives_pages: list of pages returned by get_prevalent_narratives_summary.
                              An empty list is appended automatically as the terminator.
        """
        pages = narratives_pages + [[]]  # terminator

        get_prevalent = AsyncMock(side_effect=pages)
        get_avg_views = AsyncMock(return_value=1000.0)
        calc_scores = AsyncMock(return_value=(0.1, 0.5, 0.2))
        calc_composite = AsyncMock()
        calc_acceleration = AsyncMock()
        update_alerts = AsyncMock(return_value=len(pages[0]) if pages else 0)

        return {
            "get_prevalent_narratives_summary": get_prevalent,
            "get_average_views_for_all_narratives": get_avg_views,
            "calculate_narrative_virality_scores": calc_scores,
            "calculate_composite_virality_for_date": calc_composite,
            "calculate_acceleration_rate_for_date": calc_acceleration,
            "update_narrative_alert_levels": update_alerts,
        }

    async def test_pipeline_processes_single_batch(self, narrative_service: NarrativeService):
        narratives = NarrativeSummaryFactory.batch(3)
        mocks = self._patch_sub_methods(narrative_service, [narratives])

        with patch.multiple(narrative_service, **mocks):
            total, errors = await narrative_service.run_narrative_analysis_indicators_pipeline(batch_size=10)

        assert total == 3
        assert errors == 0

    async def test_pipeline_processes_multiple_batches(self, narrative_service: NarrativeService):
        batch1 = NarrativeSummaryFactory.batch(5)
        batch2 = NarrativeSummaryFactory.batch(3)
        mocks = self._patch_sub_methods(narrative_service, [batch1, batch2])

        with patch.multiple(narrative_service, **mocks):
            total, errors = await narrative_service.run_narrative_analysis_indicators_pipeline(batch_size=5)

        assert total == 8
        assert errors == 0

    async def test_pipeline_error_does_not_abort(self, narrative_service: NarrativeService):
        narratives = NarrativeSummaryFactory.batch(3)
        mocks = self._patch_sub_methods(narrative_service, [narratives])
        # Second narrative raises
        mocks["calculate_narrative_virality_scores"].side_effect = [
            (0.1, 0.5, 0.2),
            ValueError("boom"),
            (0.1, 0.5, 0.2),
        ]

        with patch.multiple(narrative_service, **mocks):
            total, errors = await narrative_service.run_narrative_analysis_indicators_pipeline()

        assert total == 2
        assert errors == 1

    async def test_pipeline_returns_counts(self, narrative_service: NarrativeService):
        narratives = NarrativeSummaryFactory.batch(4)
        mocks = self._patch_sub_methods(narrative_service, [narratives])
        mocks["calculate_narrative_virality_scores"].side_effect = [
            (0.1, 0.5, 0.2),
            ValueError("boom"),
            (0.1, 0.5, 0.2),
            (0.1, 0.5, 0.2),
        ]

        with patch.multiple(narrative_service, **mocks):
            result = await narrative_service.run_narrative_analysis_indicators_pipeline()

        assert result == (3, 1)

    async def test_pipeline_on_progress_callback(self, narrative_service: NarrativeService):
        batch1 = NarrativeSummaryFactory.batch(2)
        batch2 = NarrativeSummaryFactory.batch(3)
        mocks = self._patch_sub_methods(narrative_service, [batch1, batch2])

        progress_calls = []
        def on_progress(total, errors):
            progress_calls.append((total, errors))

        with patch.multiple(narrative_service, **mocks):
            await narrative_service.run_narrative_analysis_indicators_pipeline(on_progress=on_progress)

        assert len(progress_calls) == 2
        assert progress_calls[0] == (2, 0)
        assert progress_calls[1] == (5, 0)

    async def test_pipeline_phases_2_and_3_always_called(self, narrative_service: NarrativeService):
        """Phases 2 and 3 must run even if all narratives in phase 1 error out."""
        narratives = NarrativeSummaryFactory.batch(2)
        mocks = self._patch_sub_methods(narrative_service, [narratives])
        mocks["calculate_narrative_virality_scores"].side_effect = ValueError("all fail")

        calc_date = date(2026, 5, 11)
        with patch.multiple(narrative_service, **mocks):
            await narrative_service.run_narrative_analysis_indicators_pipeline(calc_date=calc_date)

        mocks["calculate_composite_virality_for_date"].assert_called_once_with(calc_date=calc_date)
        mocks["calculate_acceleration_rate_for_date"].assert_called_once_with(
            calc_date=calc_date, hours=INDICATOR_WINDOW_HOURS
        )
        mocks["update_narrative_alert_levels"].assert_called_once_with(calc_date=calc_date)

    async def test_pipeline_uses_provided_calc_date(self, narrative_service: NarrativeService):
        mocks = self._patch_sub_methods(narrative_service, [[]])
        calc_date = date(2026, 1, 15)

        with patch.multiple(narrative_service, **mocks):
            await narrative_service.run_narrative_analysis_indicators_pipeline(calc_date=calc_date)

        mocks["calculate_composite_virality_for_date"].assert_called_once_with(calc_date=calc_date)
        mocks["calculate_acceleration_rate_for_date"].assert_called_once_with(
            calc_date=calc_date, hours=INDICATOR_WINDOW_HOURS
        )
        mocks["update_narrative_alert_levels"].assert_called_once_with(calc_date=calc_date)

    async def test_pipeline_uses_today_when_no_calc_date(self, narrative_service: NarrativeService):
        mocks = self._patch_sub_methods(narrative_service, [[]])
        today = date.today()

        with patch.multiple(narrative_service, **mocks):
            await narrative_service.run_narrative_analysis_indicators_pipeline()

        mocks["calculate_composite_virality_for_date"].assert_called_once_with(calc_date=today)
        mocks["update_narrative_alert_levels"].assert_called_once_with(calc_date=today)

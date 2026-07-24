"""
Mock-only tests for the virality scoring pipeline in NarrativeService.

No database is used. Repo and service sub-methods are replaced with AsyncMocks so
that every input is fully controlled and every expected output can be derived by hand.

The design under test is docs/narrative-alert-redesign.md. Its two core decisions are
what these tests are really guarding:

    C1  we only rank what we measured — an unmeasured narrative is excluded, never
        ranked as the least-active one
    C2  composite carries the spread STATE, acceleration carries the RATE, and nothing
        straddles

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
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from core.models import NarrativeAlertLevel
from core.narratives.models import (
    NarrativeAnalysisIndicatorType,
    NarrativeViralityScoreType,
)
from core.narratives.service import NarrativeService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_repo_cm(mock_repo: AsyncMock) -> AsyncMock:
    """Wrap a mock repo in an async context manager mock."""
    cm = AsyncMock()
    cm.__aenter__.return_value = mock_repo
    cm.__aexit__.return_value = None
    return cm


def _cohort_row(
    narrative_id=None,
    *,
    engagement_score: float = 0.1,
    reach_score: float = 0.5,
    views: int = 1000,
    likes: int = 50,
    comments: int = 10,
    video_count: int = 3,
) -> dict:
    """One row of get_composite_cohort, which scores in SQL."""
    return {
        "narrative_id": narrative_id or uuid.uuid4(),
        "video_count": video_count,
        "views": views,
        "likes": likes,
        "comments": comments,
        "engagement_score": engagement_score,
        "reach_score": reach_score,
    }


def _accel_row(
    narrative_id=None,
    *,
    cur_videos: float = 2.0,
    prev_videos: float = 2.0,
    refreshed_videos: int = 1,
    daily_view_gain: float = 0.0,
    baseline_views: float = 100.0,
    mean_gap_days: float = 1.0,
    cur_views: float | None = None,
    cur_likes: float | None = None,
    cur_comments: float | None = None,
    prev_likes: float = 10.0,
    prev_comments: float = 2.0,
) -> dict:
    """
    One row of get_acceleration_cohort. The repository has already done the per-day
    division: `daily_view_gain` is the sum of each video's gain divided by that video's
    own elapsed gap, and `baseline_views` is the sum of the same videos' baselines.

    The current-side totals default to whatever keeps the ENGAGEMENT RATIO unchanged,
    so a test that means to vary only view growth does not silently also vary
    engagement. Pass them explicitly to move engagement on purpose.
    """
    if cur_views is None:
        cur_views = baseline_views + daily_view_gain * mean_gap_days
    ratio = cur_views / baseline_views if baseline_views > 0 else 1.0
    if cur_likes is None:
        cur_likes = prev_likes * ratio
    if cur_comments is None:
        cur_comments = prev_comments * ratio
    return {
        "narrative_id": narrative_id or uuid.uuid4(),
        "cur_videos": cur_videos,
        "prev_videos": prev_videos,
        "refreshed_videos": refreshed_videos,
        "daily_view_gain": daily_view_gain,
        "baseline_views": baseline_views,
        "mean_gap_days": mean_gap_days,
        "cur_views": cur_views,
        "cur_likes": cur_likes,
        "cur_comments": cur_comments,
        "prev_likes": prev_likes,
        "prev_comments": prev_comments,
    }


# ---------------------------------------------------------------------------
# Section 1 — calculate_narrative_virality_scores (the spread-state axis)
# ---------------------------------------------------------------------------

class TestCalculateNarrativeViralityScores:
    """
    Phase 1 is now one bulk query. The scores themselves are computed in SQL, so what
    the service is responsible for is writing exactly two score rows per narrative —
    and, critically, no longer writing a velocity score.
    """

    async def _run(self, narrative_service, cohort: list[dict]):
        mock_repo = AsyncMock()
        mock_repo.get_composite_cohort.return_value = cohort
        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            count = await narrative_service.calculate_narrative_virality_scores(date.today())
        return mock_repo, count

    async def test_writes_engagement_and_reach_per_narrative(
        self, narrative_service: NarrativeService
    ):
        nid = uuid.uuid4()
        row = _cohort_row(nid, engagement_score=0.1, reach_score=0.5)

        mock_repo, count = await self._run(narrative_service, [row])

        records = mock_repo.bulk_insert_narrative_virality_scores.call_args[0][0]
        by_type = {score_type: value for _id, value, score_type, _meta in records}
        assert by_type[NarrativeViralityScoreType.ENGAGEMENT_SCORE] == pytest.approx(0.1)
        assert by_type[NarrativeViralityScoreType.REACH_SCORE] == pytest.approx(0.5)
        assert count == 1

    async def test_velocity_score_is_no_longer_written(self, narrative_service: NarrativeService):
        """
        Velocity is a *change* measure. C2 puts every change signal on the acceleration
        axis, and keeping this one on the level axis double-counted growth — which is
        what made the two axes look independent when they were not.
        """
        mock_repo, _ = await self._run(narrative_service, [_cohort_row()])

        records = mock_repo.bulk_insert_narrative_virality_scores.call_args[0][0]
        written = {score_type for _id, _v, score_type, _m in records}
        assert "velocity_score" not in {s.value for s in written}
        assert len(records) == 2

    async def test_scores_are_stamped_with_the_day_they_describe(
        self, narrative_service: NarrativeService
    ):
        calc_date = date(2026, 7, 16)
        mock_repo = AsyncMock()
        mock_repo.get_composite_cohort.return_value = [_cohort_row()]
        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_narrative_virality_scores(calc_date)

        mock_repo.get_composite_cohort.assert_awaited_once_with(calc_date)
        assert mock_repo.bulk_insert_narrative_virality_scores.call_args.kwargs["calc_date"] == calc_date

    async def test_empty_cohort_writes_nothing(self, narrative_service: NarrativeService):
        mock_repo, count = await self._run(narrative_service, [])
        mock_repo.bulk_insert_narrative_virality_scores.assert_not_called()
        assert count == 0


# ---------------------------------------------------------------------------
# Section 2 — calculate_composite_virality_for_date
# ---------------------------------------------------------------------------

class TestCalculateCompositeViralityForDate:
    """Weights: engagement=0.625, reach=0.375 — velocity evicted, the rest rescaled."""

    async def _composite(self, narrative_service, percentiles: dict) -> list:
        mock_repo = AsyncMock()
        mock_repo.get_all_virality_percentiles_for_date.return_value = percentiles
        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_composite_virality_for_date(date.today())
        return mock_repo.bulk_insert_narrative_analysis_indicators.call_args[0][0]

    async def test_composite_all_ones(self, narrative_service: NarrativeService):
        nid = uuid.uuid4()
        inserted = await self._composite(narrative_service, {
            nid: {
                NarrativeViralityScoreType.ENGAGEMENT_SCORE: 1.0,
                NarrativeViralityScoreType.REACH_SCORE: 1.0,
            }
        })
        _id, composite, indicator_type, _meta = inserted[0]
        assert _id == nid
        assert composite == pytest.approx(1.0)
        assert indicator_type == NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY

    async def test_composite_weighted_combination(self, narrative_service: NarrativeService):
        # 0.6*0.625 + 0.4*0.375 = 0.375 + 0.15 = 0.525
        inserted = await self._composite(narrative_service, {
            uuid.uuid4(): {
                NarrativeViralityScoreType.ENGAGEMENT_SCORE: 0.6,
                NarrativeViralityScoreType.REACH_SCORE: 0.4,
            }
        })
        _, composite, _, _ = inserted[0]
        assert composite == pytest.approx(0.525)

    async def test_a_velocity_percentile_cannot_move_composite(
        self, narrative_service: NarrativeService
    ):
        """
        Historical rows still carry velocity_score, and the percentile query keys on
        the raw column, so it still arrives as a plain string. It must not reach the
        level axis: two narratives identical but for velocity have to score identically.
        """
        without = await self._composite(narrative_service, {
            uuid.uuid4(): {
                NarrativeViralityScoreType.ENGAGEMENT_SCORE: 0.6,
                NarrativeViralityScoreType.REACH_SCORE: 0.4,
            }
        })
        with_velocity = await self._composite(narrative_service, {
            uuid.uuid4(): {
                NarrativeViralityScoreType.ENGAGEMENT_SCORE: 0.6,
                NarrativeViralityScoreType.REACH_SCORE: 0.4,
                "velocity_score": 1.0,
            }
        })
        assert without[0][1] == pytest.approx(with_velocity[0][1])

    async def test_composite_partial_percentiles(self, narrative_service: NarrativeService):
        # engagement=0.8 alone → 0.8 * 0.625 = 0.5
        inserted = await self._composite(narrative_service, {
            uuid.uuid4(): {NarrativeViralityScoreType.ENGAGEMENT_SCORE: 0.8}
        })
        _, composite, _, _ = inserted[0]
        assert composite == pytest.approx(0.5)

    async def test_composite_empty_percentiles(self, narrative_service: NarrativeService):
        assert await self._composite(narrative_service, {}) == []


# ---------------------------------------------------------------------------
# Section 3 — calculate_acceleration_rate_for_date (the change-in-spread axis)
# ---------------------------------------------------------------------------

class TestCalculateAccelerationRateForDate:
    """
    Weights: engagement=0.10, video_count=0.35, views=0.55.

    The one hard constraint is engagement < views: the old 0.40/0.35/0.25 let a dip in
    the engagement ratio cancel real view growth, which floored 679 genuine growers to
    zero on 2026-07-16.
    """

    async def _accel(self, narrative_service, rows: list[dict]) -> list:
        mock_repo = AsyncMock()
        mock_repo.get_acceleration_cohort.return_value = rows
        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_acceleration_rate_for_date(date.today())
        return mock_repo.bulk_insert_narrative_analysis_indicators.call_args[0][0]

    async def test_acceleration_normal_growth(self, narrative_service: NarrativeService):
        """
        A narrative whose refreshed videos gained 100 views/day over a 100-view
        baseline, and which gained one video on top of two:

            change_views       = 100 / 100        = 1.0
            change_video_count = (3 - 2) / 2      = 0.5
            change_engagement  = 0 (engagement ratio unchanged)
            acceleration       = 0*0.10 + 0.5*0.35 + 1.0*0.55 = 0.725
        """
        inserted = await self._accel(narrative_service, [_accel_row(
            daily_view_gain=100.0, baseline_views=100.0,
            cur_videos=3.0, prev_videos=2.0,
            cur_views=200.0, cur_likes=20.0, cur_comments=4.0,
            prev_likes=10.0, prev_comments=2.0,
        )])
        _, acceleration, indicator_type, _ = inserted[0]
        assert indicator_type == NarrativeAnalysisIndicatorType.ACCELERATION_RATE
        assert acceleration == pytest.approx(0.725)

    async def test_growth_is_divided_by_the_elapsed_gap(
        self, narrative_service: NarrativeService
    ):
        """
        D4: a rate is per unit time or it is not a rate. The same total gain spread over
        four days must rank a quarter as high as one earned in a day — before this, a
        video ranked high for having gone *unmeasured*.

        The repository does the division per video; what this asserts is that the
        service consumes an already-per-day number rather than re-deriving a total.
        """
        one_day = await self._accel(narrative_service, [_accel_row(
            daily_view_gain=100.0, baseline_views=100.0, mean_gap_days=1.0,
        )])
        four_days = await self._accel(narrative_service, [_accel_row(
            daily_view_gain=25.0, baseline_views=100.0, mean_gap_days=4.0,
        )])
        assert one_day[0][1] == pytest.approx(0.55)
        assert four_days[0][1] == pytest.approx(0.1375)
        assert four_days[0][1] == pytest.approx(one_day[0][1] / 4)

    async def test_no_baseline_views_yields_no_view_growth(
        self, narrative_service: NarrativeService
    ):
        """A rate with no denominator is undefined, not infinite — and not 100%."""
        inserted = await self._accel(narrative_service, [_accel_row(
            daily_view_gain=500.0, baseline_views=0.0, refreshed_videos=0,
            cur_videos=2.0, prev_videos=2.0,
        )])
        _, acceleration, _, meta = inserted[0]
        assert meta["change_views"] == 0.0
        assert acceleration == pytest.approx(0.0)

    async def test_engagement_cannot_outweigh_views(self, narrative_service: NarrativeService):
        """
        The defect this design exists to fix: a narrative whose views grew while its
        engagement ratio dipped must still register as growing.
        """
        inserted = await self._accel(narrative_service, [_accel_row(
            daily_view_gain=100.0, baseline_views=100.0,
            cur_views=200.0, cur_likes=10.0, cur_comments=2.0,   # engagement halved
            prev_likes=10.0, prev_comments=2.0,
        )])
        _, acceleration, _, meta = inserted[0]
        assert meta["change_engagement"] < 0, "engagement ratio fell"
        assert meta["change_views"] == pytest.approx(1.0)
        assert acceleration > 0, "real view growth must survive an engagement dip"

    async def test_flat_narrative_stays_in_the_cohort_at_zero(
        self, narrative_service: NarrativeService
    ):
        """
        A visited-but-flat narrative is ranked at an honest zero, not excluded.
        `consolidated` means big *and flat*, so this population must be able to reach
        it. What gets excluded is the unmeasured, which never reaches the service.
        """
        inserted = await self._accel(narrative_service, [_accel_row(
            daily_view_gain=0.0, baseline_views=1000.0,
            cur_videos=2.0, prev_videos=2.0,
        )])
        assert len(inserted) == 1
        assert inserted[0][1] == pytest.approx(0.0)

    async def test_decline_is_floored_at_zero(self, narrative_service: NarrativeService):
        """A shrinking narrative is not 'negatively accelerating', it is not surging."""
        inserted = await self._accel(narrative_service, [_accel_row(
            daily_view_gain=-500.0, baseline_views=1000.0,
        )])
        assert inserted[0][1] == pytest.approx(0.0)

    async def test_there_is_no_magnitude_cap(self, narrative_service: NarrativeService):
        """
        The old ACCELERATION_CHANGE_CAP clamped every component at 5.0. Per-day
        normalisation plus ranking removes the need for a magic clamp, and the clamp
        cost real signal: everything above it was tied.
        """
        inserted = await self._accel(narrative_service, [_accel_row(
            daily_view_gain=10_000.0, baseline_views=100.0,   # 100x in a day
            cur_videos=2.0, prev_videos=2.0,
        )])
        _, acceleration, _, meta = inserted[0]
        assert meta["change_views"] == pytest.approx(100.0)
        assert acceleration == pytest.approx(55.0)

    async def test_the_cohort_is_asked_for_by_date_alone(
        self, narrative_service: NarrativeService
    ):
        """
        Nothing narrows the cohort but the date. There is no baseline-age bound: growth
        per day is the whole normalisation, and an old baseline is divided by its own
        gap rather than discarded.
        """
        mock_repo = AsyncMock()
        mock_repo.get_acceleration_cohort.return_value = []
        calc_date = date(2026, 7, 16)
        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_acceleration_rate_for_date(calc_date)

        args, kwargs = mock_repo.get_acceleration_cohort.call_args
        assert args == (calc_date,)
        assert kwargs == {}


# ---------------------------------------------------------------------------
# Section 4 — update_narrative_alert_levels
# ---------------------------------------------------------------------------

class TestUpdateNarrativeAlertLevels:
    """
    The four labels are RECTANGLES on the percentile plane, not quadrants:

        viral         composite >= 0.80  and  accel >= 0.80
        early_surge   composite <= 0.40  and  accel >= 0.50
        consolidated  composite >= 0.50  and  accel <= 0.40
        trending      composite >= 0.40  and  accel >= 0.40
        (no badge)    everything else — small AND flat
    """

    @staticmethod
    def _indicators(composite: float | None, acceleration: float | None) -> dict:
        values: dict = {}
        if composite is not None:
            values["composite_virality"] = {"value": 0.5, "metadata": {"percentile": composite}}
        if acceleration is not None:
            values["acceleration_rate"] = {"value": 0.42, "metadata": {"percentile": acceleration}}
        return values

    async def _run(self, narrative_service, indicators: dict):
        mock_repo = AsyncMock()
        mock_repo.get_bulk_analysis_indicators_for_date.return_value = indicators

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            count = await narrative_service.update_narrative_alert_levels(date.today())

        if not mock_repo.bulk_update_narrative_alert_levels.called:
            return [], count, mock_repo
        return mock_repo.bulk_update_narrative_alert_levels.call_args[0][0], count, mock_repo

    async def _level(self, narrative_service, composite, acceleration):
        nid = uuid.uuid4()
        records, _, _ = await self._run(
            narrative_service, {nid: self._indicators(composite, acceleration)}
        )
        return records[0][1] if records else None

    async def test_viral_is_big_and_still_climbing(self, narrative_service: NarrativeService):
        assert await self._level(narrative_service, 0.90, 0.90) == NarrativeAlertLevel.VIRAL

    async def test_viral_requires_both_axes(self, narrative_service: NarrativeService):
        """
        `viral` is a conjunction. A narrative that is large but no longer growing is
        precisely `consolidated` — that is what gives the new label a reason to exist.
        """
        assert await self._level(narrative_service, 0.90, 0.10) == NarrativeAlertLevel.CONSOLIDATED

    async def test_early_surge_is_for_small_narratives_only(
        self, narrative_service: NarrativeService
    ):
        """
        The composite ceiling is the whole point of the label. A *large* climber that
        misses the viral floor is `trending`, never `early_surge`.
        """
        assert await self._level(narrative_service, 0.20, 0.90) == NarrativeAlertLevel.EARLY_SURGE
        assert await self._level(narrative_service, 0.70, 0.60) == NarrativeAlertLevel.TRENDING

    async def test_consolidated_is_big_and_flat(self, narrative_service: NarrativeService):
        assert await self._level(narrative_service, 0.85, 0.05) == NarrativeAlertLevel.CONSOLIDATED

    async def test_trending_is_the_broad_middle(self, narrative_service: NarrativeService):
        assert await self._level(narrative_service, 0.60, 0.60) == NarrativeAlertLevel.TRENDING

    async def test_small_and_flat_gets_no_badge(self, narrative_service: NarrativeService):
        """
        The four labels do not tile the plane. The bottom-left is an absence, not a
        level — it must come back as no record at all rather than as NONE.
        """
        assert await self._level(narrative_service, 0.20, 0.20) is None

    async def test_viral_is_carved_out_of_trending(self, narrative_service: NarrativeService):
        """
        The regions overlap by construction, so evaluation order decides the answer.
        A point inside both boxes must resolve to the more specific one.
        """
        assert await self._level(narrative_service, 0.95, 0.95) == NarrativeAlertLevel.VIRAL

    async def test_boundaries_are_inclusive_where_documented(
        self, narrative_service: NarrativeService
    ):
        assert await self._level(narrative_service, 0.80, 0.80) == NarrativeAlertLevel.VIRAL
        assert await self._level(narrative_service, 0.40, 0.50) == NarrativeAlertLevel.EARLY_SURGE
        assert await self._level(narrative_service, 0.50, 0.40) == NarrativeAlertLevel.CONSOLIDATED
        assert await self._level(narrative_service, 0.40, 0.40) == NarrativeAlertLevel.TRENDING
        # just under the viral floor on one axis, still large and moving -> trending
        assert await self._level(narrative_service, 0.7999, 0.99) == NarrativeAlertLevel.TRENDING

    async def test_the_grid_agrees_with_the_geometry(self, narrative_service: NarrativeService):
        expectations = [
            (0.99, 0.99, NarrativeAlertLevel.VIRAL),
            (0.85, 0.85, NarrativeAlertLevel.VIRAL),
            (0.10, 0.99, NarrativeAlertLevel.EARLY_SURGE),
            (0.35, 0.55, NarrativeAlertLevel.EARLY_SURGE),
            (0.99, 0.00, NarrativeAlertLevel.CONSOLIDATED),
            (0.55, 0.35, NarrativeAlertLevel.CONSOLIDATED),
            (0.60, 0.60, NarrativeAlertLevel.TRENDING),
            (0.45, 0.99, NarrativeAlertLevel.TRENDING),
            (0.10, 0.10, None),   # small and flat: the no-badge region
            (0.20, 0.45, None),   # small, moving a little, but under the surge floor
            (0.45, 0.20, None),   # middling size, flat, under the consolidated floor
        ]
        for composite, acceleration, expected in expectations:
            assert await self._level(narrative_service, composite, acceleration) == expected, (
                f"composite={composite} accel={acceleration}"
            )

    async def test_classifier_never_emits_a_retired_label(
        self, narrative_service: NarrativeService
    ):
        """`alert` and `watch` survive in the enum for consumers, not for the pipeline."""
        retired = {NarrativeAlertLevel.ALERT, NarrativeAlertLevel.WATCH, NarrativeAlertLevel.NONE}
        for i in range(0, 101, 5):
            for j in range(0, 101, 5):
                level = narrative_service._classify(i / 100, j / 100)
                assert level not in retired, f"composite={i / 100} accel={j / 100} -> {level}"

    async def test_every_boundary_is_axis_aligned(self, narrative_service: NarrativeService):
        """
        No region may compare one axis to the other — that is what lets the two axes
        rank over different cohorts without a shared denominator. Holding one axis
        fixed and sweeping the other must therefore produce contiguous runs of labels.
        """
        for fixed in (0.0, 0.25, 0.5, 0.75, 1.0):
            seen: list = []
            for i in range(101):
                level = narrative_service._classify(fixed, i / 100)
                if not seen or seen[-1] != level:
                    seen.append(level)
            assert len(seen) == len(set(seen)), (
                f"composite={fixed} revisits a label: {seen}"
            )

    async def test_missing_composite_is_not_classified(self, narrative_service: NarrativeService):
        """Absence of data must not be a signal (C1)."""
        nid = uuid.uuid4()
        records, count, _ = await self._run(narrative_service, {nid: self._indicators(None, 0.99)})
        assert records == []
        assert count == 0

    async def test_missing_acceleration_is_not_classified(
        self, narrative_service: NarrativeService
    ):
        nid = uuid.uuid4()
        records, count, _ = await self._run(narrative_service, {nid: self._indicators(0.99, None)})
        assert records == []
        assert count == 0

    async def test_indicator_without_percentile_is_not_classified(
        self, narrative_service: NarrativeService
    ):
        nid = uuid.uuid4()
        indicators = {nid: {
            "composite_virality": {"value": 0.90, "metadata": {}},
            "acceleration_rate": {"value": 1.5, "metadata": {"percentile": 0.99}},
        }}
        records, count, _ = await self._run(narrative_service, indicators)
        assert records == []
        assert count == 0

    async def test_no_badge_region_clears_a_stale_badge(
        self, narrative_service: NarrativeService
    ):
        """
        A narrative that drops into the bottom-left must lose whatever badge it had,
        not keep yesterday's.
        """
        nid = uuid.uuid4()
        records, count, mock_repo = await self._run(
            narrative_service, {nid: self._indicators(0.10, 0.10)}
        )
        assert records == []
        assert count == 1, "it was scored, it just earned no badge"
        mock_repo.clear_alert_levels_except.assert_awaited_once_with([])

    async def test_badged_narratives_are_kept_and_the_rest_cleared(
        self, narrative_service: NarrativeService
    ):
        badged = uuid.uuid4()
        unbadged = uuid.uuid4()
        _, count, mock_repo = await self._run(narrative_service, {
            badged: self._indicators(0.90, 0.90),
            unbadged: self._indicators(0.10, 0.10),
        })
        assert count == 2
        mock_repo.clear_alert_levels_except.assert_awaited_once_with([badged])

    async def test_returns_count_of_scored_narratives(self, narrative_service: NarrativeService):
        indicators = {uuid.uuid4(): self._indicators(0.60, 0.60) for _ in range(5)}
        records, count, _ = await self._run(narrative_service, indicators)
        assert count == 5
        assert len(records) == 5

    async def test_no_bulk_update_when_empty(self, narrative_service: NarrativeService):
        mock_repo = AsyncMock()
        mock_repo.get_bulk_analysis_indicators_for_date.return_value = {}

        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            count = await narrative_service.update_narrative_alert_levels(date.today())

        mock_repo.bulk_update_narrative_alert_levels.assert_not_called()
        # An empty cohort means the run found nothing, not that every badge is stale.
        mock_repo.clear_alert_levels_except.assert_not_called()
        assert count == 0


class TestPercentileRanking:
    """Both axes carry their PERCENT_RANK within their own cohort."""

    async def test_percent_ranks_match_postgres_semantics(
        self, narrative_service: NarrativeService
    ):
        assert narrative_service._percent_ranks([10.0, 20.0, 30.0]) == pytest.approx([0.0, 0.5, 1.0])
        assert narrative_service._percent_ranks([5.0, 1.0, 1.0]) == pytest.approx([1.0, 0.0, 0.0])
        assert narrative_service._percent_ranks([7.3]) == [0.0]
        assert narrative_service._percent_ranks([]) == []

    async def test_acceleration_records_carry_a_percentile(
        self, narrative_service: NarrativeService
    ):
        rows = [
            _accel_row(daily_view_gain=gain, baseline_views=100.0)
            for gain in (0.0, 50.0, 200.0)
        ]
        mock_repo = AsyncMock()
        mock_repo.get_acceleration_cohort.return_value = rows
        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_acceleration_rate_for_date(date.today())

        inserted = mock_repo.bulk_insert_narrative_analysis_indicators.call_args[0][0]
        assert [m["percentile"] for _, _, _, m in inserted] == pytest.approx([0.0, 0.5, 1.0])

    async def test_composite_records_carry_a_percentile(
        self, narrative_service: NarrativeService
    ):
        percentiles = {
            uuid.uuid4(): {NarrativeViralityScoreType.ENGAGEMENT_SCORE: p,
                           NarrativeViralityScoreType.REACH_SCORE: p}
            for p in (0.1, 0.5, 0.9)
        }
        mock_repo = AsyncMock()
        mock_repo.get_all_virality_percentiles_for_date.return_value = percentiles
        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            await narrative_service.calculate_composite_virality_for_date(date.today())

        inserted = mock_repo.bulk_insert_narrative_analysis_indicators.call_args[0][0]
        assert [m["percentile"] for _, _, _, m in inserted] == pytest.approx([0.0, 0.5, 1.0])


# ---------------------------------------------------------------------------
# Section 5 — run_narrative_analysis_indicators_pipeline
# ---------------------------------------------------------------------------

class TestRunNarrativeAnalysisIndicatorsPipeline:
    """
    Patches service sub-methods so the pipeline orchestration is tested
    without triggering any real DB calls.
    """

    def _patch_sub_methods(self, scored: int = 3):
        return {
            "calculate_narrative_virality_scores": AsyncMock(return_value=scored),
            "calculate_composite_virality_for_date": AsyncMock(),
            "calculate_acceleration_rate_for_date": AsyncMock(),
            "update_narrative_alert_levels": AsyncMock(return_value=scored),
        }

    async def test_pipeline_returns_the_scored_count(self, narrative_service: NarrativeService):
        mocks = self._patch_sub_methods(scored=22_398)
        with patch.multiple(narrative_service, **mocks):
            total, errors = await narrative_service.run_narrative_analysis_indicators_pipeline()

        assert total == 22_398
        assert errors == 0

    async def test_phase_one_no_longer_paginates(self, narrative_service: NarrativeService):
        """
        The per-narrative loop is gone. Composite ranks over every measured narrative,
        so the dashboard pagination query — with its NOW() anchor and 24-hour window,
        which made the cohort a function of when the job ran — is out of the pipeline.
        """
        mocks = self._patch_sub_methods()
        get_prevalent = AsyncMock(return_value=[])
        with patch.multiple(narrative_service, **mocks), \
             patch.object(narrative_service, "get_prevalent_narratives_summary", get_prevalent):
            await narrative_service.run_narrative_analysis_indicators_pipeline()

        get_prevalent.assert_not_called()
        mocks["calculate_narrative_virality_scores"].assert_awaited_once()

    async def test_phase_one_failure_does_not_abort_the_run(
        self, narrative_service: NarrativeService
    ):
        """Phases 2 and 3 must still run — yesterday's badges are worse than none."""
        mocks = self._patch_sub_methods()
        mocks["calculate_narrative_virality_scores"].side_effect = ValueError("boom")

        calc_date = date(2026, 5, 11)
        with patch.multiple(narrative_service, **mocks):
            total, errors = await narrative_service.run_narrative_analysis_indicators_pipeline(
                calc_date=calc_date
            )

        assert (total, errors) == (0, 1)
        mocks["calculate_composite_virality_for_date"].assert_called_once_with(calc_date=calc_date)
        mocks["calculate_acceleration_rate_for_date"].assert_called_once_with(calc_date=calc_date)
        mocks["update_narrative_alert_levels"].assert_called_once_with(calc_date=calc_date)

    async def test_pipeline_on_progress_callback(self, narrative_service: NarrativeService):
        mocks = self._patch_sub_methods(scored=7)
        progress_calls: list[tuple[int, int]] = []

        with patch.multiple(narrative_service, **mocks):
            await narrative_service.run_narrative_analysis_indicators_pipeline(
                on_progress=lambda total, errors: progress_calls.append((total, errors))
            )

        assert progress_calls == [(7, 0)]

    async def test_pipeline_uses_provided_calc_date(self, narrative_service: NarrativeService):
        mocks = self._patch_sub_methods()
        calc_date = date(2026, 1, 15)

        with patch.multiple(narrative_service, **mocks):
            await narrative_service.run_narrative_analysis_indicators_pipeline(calc_date=calc_date)

        mocks["calculate_narrative_virality_scores"].assert_called_once_with(calc_date=calc_date)
        mocks["calculate_composite_virality_for_date"].assert_called_once_with(calc_date=calc_date)
        mocks["calculate_acceleration_rate_for_date"].assert_called_once_with(calc_date=calc_date)
        mocks["update_narrative_alert_levels"].assert_called_once_with(calc_date=calc_date)

    async def test_pipeline_uses_yesterday_when_no_calc_date(
        self, narrative_service: NarrativeService
    ):
        mocks = self._patch_sub_methods()
        # Defaults to yesterday — the last completed scraping day (see pipeline docstring).
        yesterday = date.today() - timedelta(days=1)

        with patch.multiple(narrative_service, **mocks):
            await narrative_service.run_narrative_analysis_indicators_pipeline()

        mocks["calculate_composite_virality_for_date"].assert_called_once_with(calc_date=yesterday)
        mocks["update_narrative_alert_levels"].assert_called_once_with(calc_date=yesterday)


# ---------------------------------------------------------------------------
# Section 6 — get_narrative_analysis_indicators (the read side)
# ---------------------------------------------------------------------------

class TestGetNarrativeAnalysisIndicators:
    """
    The response is what a consumer sees, so C1 has to survive the serialisation too.

    The two axes answer to different evidence and so arrive independently: composite
    covers every narrative measured at least once (~22k), acceleration only those
    visited on the day (~2k). Requiring both — which the endpoint used to — returned
    null for roughly nine narratives in ten, telling a client "nothing measured" about
    a narrative whose size is perfectly well known.
    """

    def _rows(self, narrative_id, *, with_acceleration: bool):
        stamped = datetime(2026, 7, 16, 3, 0)
        rows = [{
            "id": uuid.uuid4(),
            "narrative_id": narrative_id,
            "indicator_type": NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY,
            "indicator_value": 0.62,
            "calculated_at": stamped,
            "metadata": {
                "engagement_percentile": 0.7,
                "reach_percentile": 0.5,
                "engagement_weight": 0.625,
                "reach_weight": 0.375,
                "percentile": 0.83,
            },
        }]
        if with_acceleration:
            rows.append({
                "id": uuid.uuid4(),
                "narrative_id": narrative_id,
                "indicator_type": NarrativeAnalysisIndicatorType.ACCELERATION_RATE,
                "indicator_value": 0.41,
                "calculated_at": stamped,
                "metadata": {
                    "change_engagement": -0.02,
                    "change_video_count": 0.5,
                    "change_views": 0.3,
                    "engagement_weight": 0.10,
                    "video_volume_weight": 0.35,
                    "views_weight": 0.55,
                    "percentile": 0.91,
                    "refreshed_videos": 4,
                    "mean_gap_days": 1.5,
                },
            })
        return rows

    async def _get(self, narrative_service, rows):
        mock_repo = AsyncMock()
        mock_repo.get_narrative_analysis_indicators.return_value = rows
        with patch.object(narrative_service, "repo", return_value=_make_repo_cm(mock_repo)):
            return await narrative_service.get_narrative_analysis_indicators(uuid.uuid4())

    async def test_composite_metadata_survives_without_velocity(
        self, narrative_service: NarrativeService
    ):
        """
        D6 evicted velocity from the level axis, so the pipeline stopped writing
        velocity_percentile/velocity_weight. While the response model still required
        them, every indicator computed by the new pipeline failed validation.
        """
        response = await self._get(
            narrative_service, self._rows(uuid.uuid4(), with_acceleration=True)
        )

        assert response is not None
        assert response.composite_virality.metadata.engagement_percentile == pytest.approx(0.7)
        assert response.composite_virality.metadata.reach_percentile == pytest.approx(0.5)

    async def test_percentile_reaches_the_consumer_on_both_axes(
        self, narrative_service: NarrativeService
    ):
        """
        The percentile is the number the classifier reads and the only one a client can
        place on the region plane; indicator_value is a weighted blend of two ranks and
        is not itself a rank. Undeclared on the model, pydantic silently dropped it.
        """
        response = await self._get(
            narrative_service, self._rows(uuid.uuid4(), with_acceleration=True)
        )

        assert response.composite_virality.metadata.percentile == pytest.approx(0.83)
        assert response.acceleration_rate.metadata.percentile == pytest.approx(0.91)
        assert response.acceleration_rate.metadata.refreshed_videos == 4
        assert response.acceleration_rate.metadata.mean_gap_days == pytest.approx(1.5)

    async def test_composite_is_returned_when_the_narrative_was_not_visited(
        self, narrative_service: NarrativeService
    ):
        """
        The ~91% case: a level we know, a rate we could not compute today. The composite
        must come back, and acceleration must be *absent* rather than zero — a zero
        would read as "flat", which is the conflation D0 forbids.
        """
        response = await self._get(
            narrative_service, self._rows(uuid.uuid4(), with_acceleration=False)
        )

        assert response is not None
        assert response.composite_virality.indicator_value == pytest.approx(0.62)
        assert response.acceleration_rate is None

    async def test_returns_none_when_nothing_was_ever_measured(
        self, narrative_service: NarrativeService
    ):
        """Composite stays required: with no level at all there is nothing to report."""
        assert await self._get(narrative_service, []) is None

"""DB-backed tests for how the narrative stats queries handle sparse scraping.

video_stats is scraped sparsely: a video is not guaranteed to have a row on every
calendar day. These tests build tiny narratives with deliberately gappy snapshots.

Cumulative *totals* must carry each video's last-known snapshot forward, rather than
treating a missing day as zero engagement (the bug raised in PR review).

*Rates* must not: acceleration compares a video's current snapshot against its last
known one, records how many days actually elapsed, and divides — so a video untouched
for weeks is still comparable, but its accumulated growth is not booked as one day's
surge. A video with no baseline at all yields no row, so acceleration is absent rather
than a confident zero.
"""

import math
from uuid import UUID, uuid4

import pytest

from core.narratives.repo import NarrativeRepository


async def _insert_video(cur, *, views_by_date: dict[str, int]) -> UUID:
    """Insert one video plus a video_stats row per (date -> views) entry.

    likes/comments are set equal to views to keep the fixture compact; the queries
    treat the three columns identically.
    """
    video_id = uuid4()
    await cur.execute(
        """
        INSERT INTO videos (id, title, description, platform, source_url, destination_path)
        VALUES (%(id)s, 'v', 'v', 'youtube', 'http://x', '/tmp/x')
        """,
        {"id": video_id},
    )
    for recorded_at, views in views_by_date.items():
        await cur.execute(
            """
            INSERT INTO video_stats (video_id, views, likes, comments, recorded_at)
            VALUES (%(video_id)s, %(views)s, %(views)s, %(views)s, %(recorded_at)s)
            """,
            {"video_id": video_id, "views": views, "recorded_at": recorded_at},
        )
    return video_id


async def _link_videos_to_narrative(cur, narrative_id: UUID, video_ids: list[UUID]) -> None:
    for video_id in video_ids:
        claim_id = uuid4()
        await cur.execute(
            """
            INSERT INTO video_claims (id, video_id, claim, start_time_s)
            VALUES (%(id)s, %(video_id)s, 'c', 0)
            """,
            {"id": claim_id, "video_id": video_id},
        )
        await cur.execute(
            """
            INSERT INTO claim_narratives (claim_id, narrative_id)
            VALUES (%(claim_id)s, %(narrative_id)s)
            """,
            {"claim_id": claim_id, "narrative_id": narrative_id},
        )


async def _make_narrative(cur) -> UUID:
    narrative_id = uuid4()
    await cur.execute(
        "INSERT INTO narratives (id, title, description) VALUES (%(id)s, 'n', 'n')",
        {"id": narrative_id},
    )
    return narrative_id


async def test_get_narrative_stats_carries_forward_across_gap_days(conn_factory):
    """Video A: snapshots on day 1 and 5. Video B: snapshot on day 3 only.

    Cumulative engagement must stay monotonic — on day 5 B still contributes its
    day-3 value, and on day 3 A still contributes its day-1 value. The buggy version
    (one row per video only on days it was scraped) would drop B on day 5 and A on
    day 3, producing a non-monotonic series [100, 50, 300] instead of [100, 150, 350].
    """
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        a = await _insert_video(cur, views_by_date={"2025-01-01": 100, "2025-01-05": 300})
        b = await _insert_video(cur, views_by_date={"2025-01-03": 50})
        await _link_videos_to_narrative(cur, narrative_id, [a, b])

        repo = NarrativeRepository(conn.cursor())
        stats = await repo.get_narrative_stats(narrative_id)

    assert stats is not None
    cumulative = [(p.date.isoformat()[:10], p.cumulative_views) for p in stats.time_series]
    assert cumulative == [
        ("2025-01-01", 100),
        ("2025-01-03", 150),  # A (carried 100) + B (50)
        ("2025-01-05", 350),  # A (300) + B (carried 50)
    ]
    # Per-day deltas derived via LAG stay non-negative and sum to the final total.
    deltas = [p.views for p in stats.time_series]
    assert deltas == [100, 50, 200]
    assert stats.totals.views == 350


async def _insert_stat(cur, video_id: UUID, views: int, hours_ago: int) -> None:
    await cur.execute(
        "INSERT INTO video_stats (video_id, views, likes, comments, recorded_at) "
        "VALUES (%(v)s, %(views)s, %(views)s, %(views)s, NOW() - make_interval(hours => %(h)s))",
        {"v": video_id, "views": views, "h": hours_ago},
    )


async def test_bulk_comparison_counts_a_brand_new_video_as_growth(conn_factory):
    """A video the narrative did not have before brings genuinely new engagement.

    A is comparable (1000 -> 1100). N has never been seen before this window, so its
    baseline really is 0 and its 5000 views are growth — not merely a bump in video
    count. Summing at the narrative level is what lets that register.
    """
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        a = await _insert_video(cur, views_by_date={})
        n = await _insert_video(cur, views_by_date={})
        await _insert_stat(cur, a, 1000, hours_ago=36)
        await _insert_stat(cur, a, 1100, hours_ago=2)
        await _insert_stat(cur, n, 5000, hours_ago=2)   # first sighting ever
        await _link_videos_to_narrative(cur, narrative_id, [a, n])

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_bulk_narrative_stats_comparison(hours=24)

    row = next(r for r in rows if r["narrative_id"] == narrative_id)
    assert row["paired_video_count"] == 1
    assert row["new_video_count"] == 1
    assert row["prev_views"] == 1000
    assert row["current_views"] == 6100          # 1100 + 5000
    gap = 34 / 24
    assert row["growth_views"] == pytest.approx(math.log(6101 / 1001) / gap, rel=0.02)


async def test_bulk_comparison_drops_an_unobserved_video_rather_than_calling_it_new(conn_factory):
    """A video with an old snapshot is not new — its baseline is unknown, not zero.

    U was last seen 40 days ago, beyond the baseline age limit. Treating it as new
    would invent unbounded growth from a baseline of 0; carrying its last value
    forward would report growth of exactly 0 for a video nobody looked at. It is
    dropped, and only the comparable video decides the rate.
    """
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        a = await _insert_video(cur, views_by_date={})
        u = await _insert_video(cur, views_by_date={})
        await _insert_stat(cur, a, 1000, hours_ago=36)
        await _insert_stat(cur, a, 1100, hours_ago=2)
        await _insert_stat(cur, u, 900, hours_ago=24 * 40)   # beyond the age limit
        await _insert_stat(cur, u, 9000, hours_ago=2)
        await _link_videos_to_narrative(cur, narrative_id, [a, u])

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_bulk_narrative_stats_comparison(hours=24)

    row = next(r for r in rows if r["narrative_id"] == narrative_id)
    assert row["paired_video_count"] == 1
    assert row["new_video_count"] == 0
    assert row["current_views"] == 1100      # U's 9000 views are not counted
    assert row["prev_views"] == 1000


async def test_bulk_comparison_growth_is_dominated_by_the_views_a_video_carries(conn_factory):
    """A 2-view video must not outvote a 500k-view one.

    Growth is summed across the narrative and only then turned into a ratio, so a
    video's influence is proportional to the views it actually carries.
    """
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        big = await _insert_video(cur, views_by_date={})
        tiny = await _insert_video(cur, views_by_date={})
        await _insert_stat(cur, big, 500_000, hours_ago=36)
        await _insert_stat(cur, big, 505_000, hours_ago=2)    # +1%
        await _insert_stat(cur, tiny, 2, hours_ago=36)
        await _insert_stat(cur, tiny, 2000, hours_ago=2)      # +100000%
        await _link_videos_to_narrative(cur, narrative_id, [big, tiny])

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_bulk_narrative_stats_comparison(hours=24)

    row = next(r for r in rows if r["narrative_id"] == narrative_id)
    assert row["paired_video_count"] == 2
    assert row["growth_views"] < 0.05


async def test_bulk_comparison_accepts_a_baseline_older_than_the_previous_window(conn_factory):
    """A video untouched for weeks that suddenly jumps must still be comparable.

    Requiring the baseline to sit inside the immediately-preceding window discarded
    exactly this narrative — the one the metric most wants to catch. The gap is
    reported so the caller can turn the change into a per-day rate.
    """
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        v = await _insert_video(cur, views_by_date={})
        await _insert_stat(cur, v, 1000, hours_ago=24 * 20)   # 20 days ago
        await _insert_stat(cur, v, 8000, hours_ago=2)         # just re-scraped
        await _link_videos_to_narrative(cur, narrative_id, [v])

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_bulk_narrative_stats_comparison(hours=24)

    row = next(r for r in rows if r["narrative_id"] == narrative_id)
    assert row["prev_views"] == 1000
    assert row["current_views"] == 8000
    gap = 20 - 2 / 24
    assert row["max_gap_days"] == pytest.approx(gap, abs=0.05)
    # 8x growth spread over ~20 days, not booked as one day's surge
    assert row["growth_views"] == pytest.approx(math.log(8001 / 1001) / gap, rel=0.02)


async def test_bulk_comparison_baseline_at_the_age_limit_is_excluded(conn_factory):
    """The age bound is strict: a baseline exactly `max_baseline_age_hours` old is out."""
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        v = await _insert_video(cur, views_by_date={})
        await _insert_stat(cur, v, 1000, hours_ago=720)   # exactly the limit
        await _insert_stat(cur, v, 8000, hours_ago=2)
        await _link_videos_to_narrative(cur, narrative_id, [v])

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_bulk_narrative_stats_comparison(hours=24, max_baseline_age_hours=720)

    assert all(r["narrative_id"] != narrative_id for r in rows)


async def test_bulk_comparison_rejects_baselines_beyond_the_age_limit(conn_factory):
    """Past the age limit the narrative is unscoreable rather than wrong."""
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        v = await _insert_video(cur, views_by_date={})
        await _insert_stat(cur, v, 1000, hours_ago=24 * 40)   # 40 days — beyond 30
        await _insert_stat(cur, v, 8000, hours_ago=2)
        await _link_videos_to_narrative(cur, narrative_id, [v])

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_bulk_narrative_stats_comparison(hours=24, max_baseline_age_hours=720)

    assert all(r["narrative_id"] != narrative_id for r in rows)


async def test_bulk_comparison_rejects_tiny_baselines(conn_factory):
    """2 -> 12 views is not a 500% surge; it is noise."""
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        v = await _insert_video(cur, views_by_date={})
        await _insert_stat(cur, v, 2, hours_ago=36)
        await _insert_stat(cur, v, 12, hours_ago=2)
        await _link_videos_to_narrative(cur, narrative_id, [v])

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_bulk_narrative_stats_comparison(hours=24, min_baseline_views=100)

    assert all(r["narrative_id"] != narrative_id for r in rows)


async def test_bulk_comparison_omits_narratives_with_no_comparable_video(conn_factory):
    """A narrative made only of brand-new videos has nothing to compare against.

    Its views are all growth, but from an unmeasured past — there is no rate. It gets
    no row, so acceleration is absent rather than a confident zero, and it cannot be
    classified on data that was never gathered.
    """
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        fresh = await _insert_video(cur, views_by_date={})
        await _insert_stat(cur, fresh, 5000, hours_ago=2)   # only ever seen once
        await _link_videos_to_narrative(cur, narrative_id, [fresh])

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_bulk_narrative_stats_comparison(hours=24)

    assert all(r["narrative_id"] != narrative_id for r in rows)


async def test_bulk_comparison_window_is_relative_to_now_not_a_calendar_day(conn_factory):
    """Snapshots 2h and 36h old pair up even when they straddle midnight.

    Under the calendar-day comparison this narrative scored exactly 0 whenever the
    job ran shortly after midnight: both sides resolved to the same carried-forward
    row.
    """
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        v = await _insert_video(cur, views_by_date={})
        await _insert_stat(cur, v, 1000, hours_ago=36)
        await _insert_stat(cur, v, 2000, hours_ago=2)
        await _link_videos_to_narrative(cur, narrative_id, [v])

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_bulk_narrative_stats_comparison(hours=24)

    row = next(r for r in rows if r["narrative_id"] == narrative_id)
    assert row["current_views"] == 2000
    assert row["prev_views"] == 1000
    assert row["growth_views"] > 0


async def test_delta_for_period_baselines_on_pre_window_snapshot(conn_factory):
    """Video C grows 100 -> 500 across the window boundary; video D is stale.

    C's only in-window snapshot is the 500 one, so the old "first vs last snapshot
    inside the window" logic would report a delta of 0. The fix baselines on the last
    snapshot *before* the window (100), yielding a delta of 400. Stale video D (single
    old snapshot) carries forward to current == baseline, contributing 0.
    """
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        # recorded_at relative to NOW() so the days_back window is meaningful.
        c = await _insert_video(cur, views_by_date={})
        d = await _insert_video(cur, views_by_date={})
        await cur.execute(
            "INSERT INTO video_stats (video_id, views, likes, comments, recorded_at) "
            "VALUES (%(v)s, 100, 100, 100, NOW() - INTERVAL '10 days')",
            {"v": c},
        )
        await cur.execute(
            "INSERT INTO video_stats (video_id, views, likes, comments, recorded_at) "
            "VALUES (%(v)s, 500, 500, 500, NOW() - INTERVAL '1 day')",
            {"v": c},
        )
        await cur.execute(
            "INSERT INTO video_stats (video_id, views, likes, comments, recorded_at) "
            "VALUES (%(v)s, 200, 200, 200, NOW() - INTERVAL '10 days')",
            {"v": d},
        )
        await _link_videos_to_narrative(cur, narrative_id, [c, d])

        repo = NarrativeRepository(conn.cursor())
        totals = await repo.get_narrative_stats_delta_for_period(narrative_id, days_back=2)

    assert totals.views == 400  # C: 500 - 100; D: 200 - 200 = 0
    assert totals.video_count == 2

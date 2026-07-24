"""DB-backed tests for the two cohort queries behind the alert system.

video_stats is scraped sparsely: a video is not guaranteed to have a row on every
calendar day. These tests build tiny narratives with deliberately gappy snapshots and
assert two different things about that sparseness, one per axis:

  - composite (a LEVEL) carries each video's last-known snapshot forward, because "how
    big is this" does not become unknown just because we did not look today;
  - acceleration (a RATE) refuses to answer at all unless we provably visited the
    narrative on calc_date, and divides each video's gain by that video's own elapsed
    gap so a long gap cannot masquerade as a fast day.
"""

from datetime import date
from uuid import UUID, uuid4

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


async def test_composite_cohort_carries_forward_to_calc_date(conn_factory):
    """The same gappy fixture, scored for day 5.

    B was not scraped on day 5. The exact-day filter (`recorded_at::date = day`) would
    drop it and make the narrative look like it shrank; carry-forward must count B's
    day-3 value, giving 350 across 2 videos.

    A level does not become unknown just because we did not look today — which is the
    whole reason composite may rank over narratives nobody visited.
    """
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        a = await _insert_video(cur, views_by_date={"2025-01-01": 100, "2025-01-05": 300})
        b = await _insert_video(cur, views_by_date={"2025-01-03": 50})
        await _link_videos_to_narrative(cur, narrative_id, [a, b])

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_composite_cohort(date(2025, 1, 5))

    row = next(r for r in rows if r["narrative_id"] == narrative_id)
    assert row["views"] == 350  # A (300) + B (carried 50)
    assert row["video_count"] == 2


async def test_composite_cohort_excludes_the_never_measured(conn_factory):
    """A narrative with videos but no snapshot on or before calc_date is *unmeasured*.

    It must be absent rather than present at zero: ranking it at the bottom would claim
    we looked and found nothing, when we never looked at all.
    """
    async with conn_factory() as conn:
        cur = conn.cursor()
        measured = await _make_narrative(cur)
        unmeasured = await _make_narrative(cur)
        a = await _insert_video(cur, views_by_date={"2025-01-01": 100})
        # snapshot lands *after* calc_date
        b = await _insert_video(cur, views_by_date={"2025-01-09": 500})
        await _link_videos_to_narrative(cur, measured, [a])
        await _link_videos_to_narrative(cur, unmeasured, [b])

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_composite_cohort(date(2025, 1, 5))

    ids = {r["narrative_id"] for r in rows}
    assert measured in ids
    assert unmeasured not in ids


async def test_acceleration_divides_each_video_by_its_own_gap(conn_factory):
    """Two videos gain 100 views each, but over gaps of 1 and 4 days.

    A rate is per unit time or it is not a rate: the four-day video must contribute a
    quarter as much per day, not the same. Before this, a video ranked high for having
    gone *unmeasured* — the longer the gap, the bigger the apparent jump.

        A: 100 -> 200 across 1 day   -> 100/day
        B: 100 -> 200 across 4 days  ->  25/day
        daily_view_gain = 125 over a baseline of 200
    """
    calc_date = date(2025, 1, 5)
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        a = await _insert_video(cur, views_by_date={"2025-01-04": 100, "2025-01-05": 200})
        b = await _insert_video(cur, views_by_date={"2025-01-01": 100, "2025-01-05": 200})
        await _link_videos_to_narrative(cur, narrative_id, [a, b])
        # the visit predicate reads videos.updated_at as well as video_stats
        await cur.execute(
            "UPDATE videos SET updated_at = %(d)s WHERE id = ANY(%(ids)s)",
            {"d": calc_date, "ids": [a, b]},
        )

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_acceleration_cohort(calc_date, max_baseline_age_days=0)

    row = next(r for r in rows if r["narrative_id"] == narrative_id)
    assert row["refreshed_videos"] == 2
    assert row["daily_view_gain"] == 125.0
    assert row["baseline_views"] == 200.0


async def test_acceleration_drops_a_baseline_older_than_the_bound(conn_factory):
    """The same fixture with a 2-day bound: B's 4-day-old baseline cannot anchor a rate.

    Per-day division would otherwise launder an old surge into today's number.
    """
    calc_date = date(2025, 1, 5)
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        a = await _insert_video(cur, views_by_date={"2025-01-04": 100, "2025-01-05": 200})
        b = await _insert_video(cur, views_by_date={"2025-01-01": 100, "2025-01-05": 200})
        await _link_videos_to_narrative(cur, narrative_id, [a, b])
        await cur.execute(
            "UPDATE videos SET updated_at = %(d)s WHERE id = ANY(%(ids)s)",
            {"d": calc_date, "ids": [a, b]},
        )

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_acceleration_cohort(calc_date, max_baseline_age_days=2)

    row = next(r for r in rows if r["narrative_id"] == narrative_id)
    assert row["refreshed_videos"] == 1
    assert row["daily_view_gain"] == 100.0
    assert row["baseline_views"] == 100.0


async def test_acceleration_excludes_narratives_born_on_calc_date(conn_factory):
    """A narrative whose videos all appeared today has no previous-day state.

    That is birth, not acceleration, and there is no baseline to divide by. It must be
    absent from the cohort entirely rather than arrive with an invented rate.
    """
    calc_date = date(2025, 1, 5)
    async with conn_factory() as conn:
        cur = conn.cursor()
        established = await _make_narrative(cur)
        newborn = await _make_narrative(cur)
        a = await _insert_video(cur, views_by_date={"2025-01-04": 100, "2025-01-05": 200})
        b = await _insert_video(cur, views_by_date={"2025-01-05": 900})
        await _link_videos_to_narrative(cur, established, [a])
        await _link_videos_to_narrative(cur, newborn, [b])
        await cur.execute(
            "UPDATE videos SET updated_at = %(d)s WHERE id = ANY(%(ids)s)",
            {"d": calc_date, "ids": [a, b]},
        )

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_acceleration_cohort(calc_date, max_baseline_age_days=0)

    ids = {r["narrative_id"] for r in rows}
    assert established in ids
    assert newborn not in ids


async def test_acceleration_excludes_the_unvisited(conn_factory):
    """A narrative nobody looked at on calc_date cannot have a rate computed.

    Its carried-forward snapshot is identical on both days, so it would arrive wearing
    a zero that means "unmeasured" rather than "flat" — exactly the conflation that put
    93% of the axis at zero.
    """
    calc_date = date(2025, 1, 5)
    async with conn_factory() as conn:
        cur = conn.cursor()
        visited = await _make_narrative(cur)
        stale = await _make_narrative(cur)
        a = await _insert_video(cur, views_by_date={"2025-01-04": 100, "2025-01-05": 200})
        b = await _insert_video(cur, views_by_date={"2025-01-01": 100, "2025-01-02": 150})
        await _link_videos_to_narrative(cur, visited, [a])
        await _link_videos_to_narrative(cur, stale, [b])
        await cur.execute(
            "UPDATE videos SET updated_at = %(d)s WHERE id = %(id)s",
            {"d": calc_date, "id": a},
        )
        await cur.execute(
            "UPDATE videos SET updated_at = %(d)s WHERE id = %(id)s",
            {"d": date(2025, 1, 2), "id": b},
        )

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_acceleration_cohort(calc_date, max_baseline_age_days=0)

    ids = {r["narrative_id"] for r in rows}
    assert visited in ids
    assert stale not in ids


async def test_visited_but_flat_stays_in_the_cohort(conn_factory):
    """Visited, nothing changed: an honest zero, and it must survive.

    `consolidated` means big *and flat*, so a large narrative that genuinely stopped
    growing has to be able to reach it. Excluding zeros would delete that population.
    """
    calc_date = date(2025, 1, 5)
    async with conn_factory() as conn:
        cur = conn.cursor()
        narrative_id = await _make_narrative(cur)
        a = await _insert_video(cur, views_by_date={"2025-01-04": 100, "2025-01-05": 100})
        await _link_videos_to_narrative(cur, narrative_id, [a])
        await cur.execute(
            "UPDATE videos SET updated_at = %(d)s WHERE id = %(id)s",
            {"d": calc_date, "id": a},
        )

        repo = NarrativeRepository(conn.cursor())
        rows = await repo.get_acceleration_cohort(calc_date, max_baseline_age_days=0)

    row = next(r for r in rows if r["narrative_id"] == narrative_id)
    assert row["daily_view_gain"] == 0.0
    assert row["baseline_views"] == 100.0

"""DB-backed tests for the analysis-indicator write/read round-trip.

Phase 2 of the pipeline writes composite/acceleration indicators and phase 3 reads
them back to classify spread levels. They agree on one thing only: `calculated_at`
holds the day the indicator *describes*, which is what
get_bulk_analysis_indicators_for_date filters on.

Stamping the write with NOW() instead broke that agreement as soon as the pipeline
started scoring the last completed day: a 00:05 run wrote rows dated today, then
asked for rows dated yesterday and classified against the *previous* run's output.
Nothing raised — the badges were simply derived from stale numbers.
"""

from datetime import date, timedelta
from uuid import UUID, uuid4

from core.narratives.models import NarrativeAnalysisIndicatorType
from core.narratives.repo import NarrativeRepository

CALC_DATE = date(2025, 1, 5)


async def _make_narrative(cur) -> UUID:
    narrative_id = uuid4()
    await cur.execute(
        "INSERT INTO narratives (id, title, description) VALUES (%(id)s, 'n', 'n')",
        {"id": narrative_id},
    )
    return narrative_id


async def test_indicators_are_read_back_for_the_date_they_describe(conn_factory):
    """The phase 2 -> phase 3 round-trip: what a run writes for a date, it reads for that date.

    CALC_DATE is in the past, so a NOW() stamp lands on today and this lookup comes
    back empty — which is exactly how the pipeline silently classified narratives
    against the previous run's indicators.
    """
    async with conn_factory() as conn:
        narrative_id = await _make_narrative(conn.cursor())

        repo = NarrativeRepository(conn.cursor())
        await repo.bulk_insert_narrative_analysis_indicators(
            [
                (narrative_id, 0.90, NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY, {"percentile": 0.42}),
                (narrative_id, 1.50, NarrativeAnalysisIndicatorType.ACCELERATION_RATE, {"percentile": 0.99}),
            ],
            calc_date=CALC_DATE,
        )

        indicators = await repo.get_bulk_analysis_indicators_for_date(CALC_DATE)

    assert indicators[narrative_id]["composite_virality"] == {"value": 0.90, "metadata": {"percentile": 0.42}}
    assert indicators[narrative_id]["acceleration_rate"] == {"value": 1.50, "metadata": {"percentile": 0.99}}


async def test_indicators_are_not_visible_on_the_day_the_run_executes(conn_factory):
    """The other half: a row describing CALC_DATE must not surface under the wall-clock day.

    Without this, the off-by-one hides — a row could satisfy both dates and the
    round-trip test above would pass against a NOW() stamp too.
    """
    async with conn_factory() as conn:
        narrative_id = await _make_narrative(conn.cursor())

        repo = NarrativeRepository(conn.cursor())
        await repo.bulk_insert_narrative_analysis_indicators(
            [(narrative_id, 0.90, NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY, {"percentile": 0.42})],
            calc_date=CALC_DATE,
        )

        indicators = await repo.get_bulk_analysis_indicators_for_date(CALC_DATE + timedelta(days=1))

    assert narrative_id not in indicators


async def test_rerun_on_the_same_date_supersedes_the_earlier_row(conn_factory):
    """Why the stamp keeps the wall-clock time instead of being a bare date.

    Reruns for one calc_date are ordered by `calculated_at DESC` and the newest wins.
    Stamping midnight would tie every rerun and make which row is 'latest' arbitrary.
    """
    async with conn_factory() as conn:
        narrative_id = await _make_narrative(conn.cursor())
        repo = NarrativeRepository(conn.cursor())

        await repo.bulk_insert_narrative_analysis_indicators(
            [(narrative_id, 0.10, NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY, {"percentile": 0.11})],
            calc_date=CALC_DATE,
        )
        await repo.bulk_insert_narrative_analysis_indicators(
            [(narrative_id, 0.90, NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY, {"percentile": 0.99})],
            calc_date=CALC_DATE,
        )

        indicators = await repo.get_bulk_analysis_indicators_for_date(CALC_DATE)

    assert indicators[narrative_id]["composite_virality"]["value"] == 0.90


async def test_defaults_to_today_when_no_calc_date_is_given(conn_factory):
    """calc_date is optional and falls back to the current day, as it did before."""
    async with conn_factory() as conn:
        narrative_id = await _make_narrative(conn.cursor())

        repo = NarrativeRepository(conn.cursor())
        await repo.bulk_insert_narrative_analysis_indicators(
            [(narrative_id, 0.90, NarrativeAnalysisIndicatorType.COMPOSITE_VIRALITY, {"percentile": 0.42})]
        )

        indicators = await repo.get_bulk_analysis_indicators_for_date(date.today())

    assert indicators[narrative_id]["composite_virality"]["value"] == 0.90

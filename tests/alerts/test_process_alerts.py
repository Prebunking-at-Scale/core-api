"""The alerts job reports a narrative once per alert.

Topic and keyword alerts record their matches with threshold_crossed = NULL, and
alerts_triggered's UNIQUE (alert_id, narrative_id, threshold_crossed) never conflicts
on NULL. Every time a matched narrative's videos were updated (stats refreshes do it
all the time), the next run recorded and e-mailed the same narrative again.
"""

from typing import Any
from uuid import UUID, uuid4

from pytest import fixture

from core.alerts.service import AlertService
from core.auth.service import AuthService
from tests.auth.conftest import create_organisation, create_user

HEALTH = UUID("bb52f622-b9ee-4d5b-9b70-5fd05046528b")


@fixture
def tables_to_truncate() -> list[str]:
    return [
        "alerts_triggered",
        "alert_executions",
        "alerts",
        "claim_narratives",
        "narrative_topics",
        "video_claims",
        "videos",
        "narratives",
    ]


async def add_narrative(conn: Any, title: str, views: int) -> tuple[UUID, UUID]:
    narrative_id, video_id, claim_id = uuid4(), uuid4(), uuid4()
    await conn.execute(
        "INSERT INTO narratives (id, title, description) VALUES (%s, %s, '')", (narrative_id, title)
    )
    await conn.execute(
        """
        INSERT INTO videos (id, title, description, platform, source_url, destination_path, views, metadata)
        VALUES (%s, %s, '', 'youtube', 'https://example.org', '', %s, '{}')
        """,
        (video_id, title, views),
    )
    await conn.execute(
        "INSERT INTO video_claims (id, video_id, claim, start_time_s, metadata) VALUES (%s, %s, %s, 1, '{}')",
        (claim_id, video_id, title),
    )
    await conn.execute(
        "INSERT INTO claim_narratives (claim_id, narrative_id) VALUES (%s, %s)", (claim_id, narrative_id)
    )
    await conn.execute(
        "INSERT INTO narrative_topics (narrative_id, topic_id) VALUES (%s, %s)", (narrative_id, HEALTH)
    )
    return narrative_id, video_id


async def test_each_alert_reports_a_narrative_once(conn_factory: Any) -> None:
    auth = AuthService(conn_factory)
    organisation = await create_organisation(auth, short_name=f"org-{uuid4().hex[:6]}")
    user = await create_user(auth, organisation, False)
    async with conn_factory() as conn:
        _, video_id = await add_narrative(conn, "Vaccines contain microchips", views=5000)
        for alert_type, column, value in (
            ("narrative_with_topic", "topic_id", HEALTH),
            ("keyword", "keyword", "microchip"),
            ("narrative_views", "threshold", 1000),
        ):
            await conn.execute(
                f"""
                INSERT INTO alerts (user_id, organisation_id, name, alert_type, scope, {column})
                VALUES (%s, %s, %s, %s, 'general', %s)
                """,
                (user.id, organisation.id, alert_type, alert_type, value),
            )

    service = AlertService(conn_factory)
    first = await service.process_alerts()
    assert first.alerts_triggered == 3

    # A stats refresh touches the narrative's video: the narrative is looked at again
    async with conn_factory() as conn:
        await conn.execute("UPDATE videos SET views = 6000, updated_at = now() WHERE id = %s", (video_id,))
    second = await service.process_alerts()
    assert second.alerts_triggered == 0

    async with conn_factory() as conn:
        cur = await conn.execute("SELECT count(*) FROM alerts_triggered")
        row = await cur.fetchone()
    assert row is not None and row["count"] == 3

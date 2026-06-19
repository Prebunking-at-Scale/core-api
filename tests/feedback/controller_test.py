"""End-to-end tests for the narrative feedback-summary endpoint.

Exercises GET /api/feedback/narratives/{id}/summary, which aggregates the
narrative_feedback rows (one upserted row per user) into a count + average.

Notes:
- A single AsyncTestClient (api_key_client) is used throughout. X-API-TOKEN auth
  resolves to the super-admin api@pas user, which satisfies the endpoint's
  `user` requirement, so a second authenticated client is unnecessary (and two
  clients on the same app double-close the connection pool at teardown).
- Feedback rows are seeded directly through FeedbackRepository rather than via
  the POST endpoint: in this workspace the external narratives API is
  configured, so POST feedback would attempt a real outbound call. The summary
  read path does no outbound call, which is what we test here in isolation.
"""

from uuid import uuid4

from litestar import Litestar
from litestar.testing import AsyncTestClient

from core.auth.models import Organisation
from core.auth.service import AuthService
from core.feedback.repo import FeedbackRepository
from core.uow import uow
from tests.auth.controller_test import create_user_with_password
from tests.narratives.conftest import create_narrative


async def test_summary_aggregates_count_and_average(
    api_key_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
    conn_factory,
) -> None:
    narrative = await create_narrative(api_key_client)

    # Two distinct users rate the same narrative: 0.25 and 0.75 -> avg 0.5
    async with uow(FeedbackRepository, conn_factory) as repo:
        for score in (0.25, 0.75):
            user, _ = await create_user_with_password(auth_service, organisation)
            await repo.submit_narrative_feedback(user.id, narrative.id, score)

    response = await api_key_client.get(
        f"/api/feedback/narratives/{narrative.id}/summary"
    )
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["score_count"] == 2
    assert data["average_score"] == 0.5


async def test_narratives_list_includes_rating_aggregate(
    api_key_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
    conn_factory,
) -> None:
    """The narratives list endpoint exposes score_count + average_score per item."""
    narrative = await create_narrative(api_key_client)

    async with uow(FeedbackRepository, conn_factory) as repo:
        for score in (0.5, 1.0):
            user, _ = await create_user_with_password(auth_service, organisation)
            await repo.submit_narrative_feedback(user.id, narrative.id, score)

    response = await api_key_client.get("/api/narratives/")
    assert response.status_code == 200, response.text
    items = {item["id"]: item for item in response.json()["data"]}
    assert str(narrative.id) in items
    rated = items[str(narrative.id)]
    assert rated["score_count"] == 2
    assert rated["average_score"] == 0.75


async def test_narratives_list_unrated_has_null_average(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    """A narrative with no feedback reports score_count 0 / average_score null."""
    narrative = await create_narrative(api_key_client)

    response = await api_key_client.get("/api/narratives/")
    assert response.status_code == 200, response.text
    items = {item["id"]: item for item in response.json()["data"]}
    rated = items[str(narrative.id)]
    assert rated["score_count"] == 0
    assert rated["average_score"] is None


async def test_summary_no_feedback_returns_zero_and_none(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    narrative = await create_narrative(api_key_client)

    response = await api_key_client.get(
        f"/api/feedback/narratives/{narrative.id}/summary"
    )
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["score_count"] == 0
    assert data["average_score"] is None


async def test_summary_unknown_narrative_returns_404(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    response = await api_key_client.get(
        f"/api/feedback/narratives/{uuid4()}/summary"
    )
    assert response.status_code == 404

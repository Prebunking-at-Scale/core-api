from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from core.narratives import api as api_module


@pytest.fixture
def configured_api(monkeypatch: pytest.MonkeyPatch) -> api_module.NarrativesApiClient:
    monkeypatch.setattr(api_module, "NARRATIVES_BASE_URL", "https://example.test")
    monkeypatch.setattr(api_module, "NARRATIVES_API_KEY", "test-token")
    return api_module.NarrativesApiClient()


def _mock_async_client() -> tuple[MagicMock, AsyncMock]:
    """Build a context-manager mock for httpx.AsyncClient with an awaitable .post."""
    client_instance = MagicMock()
    post_mock = AsyncMock(return_value=MagicMock(status_code=200))
    client_instance.post = post_mock
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client_instance)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm, post_mock


async def test_send_feedback_includes_comment_and_user_id(
    configured_api: api_module.NarrativesApiClient,
) -> None:
    narrative_id = uuid4()
    user_id = uuid4()
    content_id = uuid4()
    cm, post_mock = _mock_async_client()

    with patch("core.narratives.api.httpx.AsyncClient", return_value=cm):
        await configured_api.send_feedback(
            narrative_id=narrative_id,
            feedback_score=0.8,
            content_id=content_id,
            comment="muy útil",
            user_id=user_id,
        )

    assert post_mock.await_count == 1
    _, kwargs = post_mock.call_args
    assert kwargs["json"] == {
        "narrative_id": str(narrative_id),
        "feedback_score": 0.8,
        "content_id": str(content_id),
        "comment": "muy útil",
        "user_id": str(user_id),
    }
    assert kwargs["headers"]["X-API-TOKEN"] == "test-token"


async def test_send_feedback_omits_optional_fields_when_missing(
    configured_api: api_module.NarrativesApiClient,
) -> None:
    narrative_id = uuid4()
    cm, post_mock = _mock_async_client()

    with patch("core.narratives.api.httpx.AsyncClient", return_value=cm):
        await configured_api.send_feedback(
            narrative_id=narrative_id,
            feedback_score=0.5,
        )

    _, kwargs = post_mock.call_args
    assert kwargs["json"] == {
        "narrative_id": str(narrative_id),
        "feedback_score": 0.5,
    }


async def test_send_feedback_omits_empty_comment(
    configured_api: api_module.NarrativesApiClient,
) -> None:
    narrative_id = uuid4()
    cm, post_mock = _mock_async_client()

    with patch("core.narratives.api.httpx.AsyncClient", return_value=cm):
        await configured_api.send_feedback(
            narrative_id=narrative_id,
            feedback_score=0.0,
            comment="",
        )

    _, kwargs = post_mock.call_args
    assert "comment" not in kwargs["json"]

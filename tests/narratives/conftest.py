from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

from litestar import Litestar
from litestar.testing import AsyncTestClient
from polyfactory.factories.pydantic_factory import ModelFactory
from pytest import fixture

from core.models import Narrative
from core.narratives.models import NarrativeInput, NarrativeSummary
from core.narratives.service import NarrativeService


class NarrativeInputFactory(ModelFactory[NarrativeInput]):
    __check_model__ = True

    @classmethod
    def build(cls, **kwargs):

        instance = super().build(**kwargs)
        if "claim_ids" not in kwargs:
            instance.claim_ids = []
        if "topic_ids" not in kwargs:
            instance.topic_ids = []
        return instance


@fixture
def tables_to_truncate() -> list[str]:
    return [
        "narratives",
        "entities",
        "narrative_topics",
        "narrative_entities",
        "claim_narratives",
        "claim_entities",
        "claim_topics",
        "videos",
    ]


async def create_narrative(
    api_key_client: AsyncTestClient[Litestar], **kwargs: dict[str, Any]
) -> Narrative:
    narrative_input = NarrativeInputFactory.build(kwargs=kwargs)
    response = await api_key_client.post(
        "/api/narratives/",
        json=narrative_input.model_dump(mode="json"),
    )
    assert response.status_code == 201
    return Narrative(**response.json()["data"])


@fixture
async def narrative(api_key_client: AsyncTestClient[Litestar]) -> Narrative:
    return await create_narrative(api_key_client)


class NarrativeSummaryFactory(ModelFactory[NarrativeSummary]):
    __check_model__ = True

    @classmethod
    def build(cls, **kwargs):
        instance = super().build(**kwargs)
        if "id" not in kwargs:
            instance.id = uuid4()
        return instance


@fixture
def narrative_service() -> NarrativeService:
    """NarrativeService wired with a dummy connection_factory — never touches the DB."""
    return NarrativeService(connection_factory=MagicMock())
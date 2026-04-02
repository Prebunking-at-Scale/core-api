from typing import Any
from uuid import UUID

from litestar import Litestar
from litestar.testing import AsyncTestClient
from polyfactory.factories.pydantic_factory import ModelFactory
from pytest import fixture

from core.auth.models import Organisation
from core.auth.service import AuthService
from core.media_feeds.models import ChannelFeed, KeywordFeed
from tests.auth.conftest import create_organisation

CLIMATE_TOPIC_ID = UUID("db3d996b-e691-4ce5-8c46-e35a82a9b28c")
HEALTH_TOPIC_ID = UUID("bb52f622-b9ee-4d5b-9b70-5fd05046528b")


class ChannelFeedFactory(ModelFactory[ChannelFeed]):
    __check_model__ = True


class KeywordFeedFactory(ModelFactory[KeywordFeed]):
    __check_model__ = True


@fixture
def tables_to_truncate() -> list[str]:
    return [
        "channel_feeds",
        "keyword_feeds",
        "media_feed_cursors",
    ]


@fixture
async def auth_service(conn_factory) -> AuthService:
    return AuthService(conn_factory)


@fixture
async def organisation(auth_service: AuthService) -> Organisation:
    return await create_organisation(auth_service)


async def create_channel_feed(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
    **kwargs: dict[str, Any],
) -> ChannelFeed:
    channel_feed = ChannelFeedFactory.build(kwargs=kwargs)
    response = await api_key_client.post(
        "/api/media_feeds/channels",
        params={"organisation_id": str(organisation.id)},
        json=channel_feed.model_dump(mode="json"),
    )
    assert response.status_code == 201
    return ChannelFeed(**response.json()["data"])


async def create_keyword_feed(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
    topic_id: UUID | None = None,
    **kwargs: dict[str, Any],
) -> KeywordFeed:
    feed = KeywordFeedFactory.build(
        topic_id=topic_id or CLIMATE_TOPIC_ID,
        kwargs=kwargs,
    )
    response = await api_key_client.post(
        "/api/media_feeds/keywords",
        params={"organisation_id": str(organisation.id)},
        json=feed.model_dump(mode="json"),
    )
    assert response.status_code == 201
    return KeywordFeed(**response.json()["data"])


@fixture
async def channel_feed(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> ChannelFeed:
    return await create_channel_feed(api_key_client, organisation)


@fixture
async def keyword_feed(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> KeywordFeed:
    return await create_keyword_feed(api_key_client, organisation)

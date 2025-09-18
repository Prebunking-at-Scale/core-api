from typing import Any

from litestar import Litestar
from litestar.testing import AsyncTestClient
from polyfactory.factories.pydantic_factory import ModelFactory
from pytest import fixture

from core.auth.models import Organisation
from core.auth.service import AuthService
from core.media_feeds.models import ChannelFeed, KeywordFeed
from tests.auth.conftest import create_organisation


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
    **kwargs: dict[str, Any],
) -> KeywordFeed:
    keyword_feed = KeywordFeedFactory.build(kwargs=kwargs)
    response = await api_key_client.post(
        "/api/media_feeds/keywords",
        params={"organisation_id": str(organisation.id)},
        json=keyword_feed.model_dump(mode="json"),
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

import uuid
from uuid import uuid4

from litestar import Litestar
from litestar.testing import AsyncTestClient

from core.auth.models import Organisation
from core.auth.service import AuthService
from core.media_feeds.models import ChannelFeed, KeywordFeed
from tests.auth.conftest import create_organisation
from tests.media_feeds.conftest import (
    ChannelFeedFactory,
    KeywordFeedFactory,
    create_channel_feed,
    create_keyword_feed,
)


async def test_create_channel_feed(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    channel_feed = ChannelFeedFactory.build()
    channel_feed_json = channel_feed.model_dump(mode="json")
    response = await api_key_client.post(
        "/api/media_feeds/channels",
        params={"organisation_id": str(organisation.id)},
        json=channel_feed_json,
    )
    assert response.status_code == 201
    response_data = response.json()["data"]
    assert response_data["organisation_id"] == str(organisation.id)
    assert response_data["channel"] == channel_feed.channel
    assert response_data["platform"] == channel_feed.platform
    assert response_data["is_archived"] is False


async def test_create_channel_feed_from_url_youtube(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    url_data = {"url": "https://www.youtube.com/@testchannel"}
    response = await api_key_client.post(
        "/api/media_feeds/channels/from-url",
        params={"organisation_id": str(organisation.id)},
        json=url_data,
    )
    assert response.status_code == 201
    response_data = response.json()["data"]
    assert response_data["organisation_id"] == str(organisation.id)
    assert response_data["channel"] == "testchannel"
    assert response_data["platform"] == "youtube"
    assert response_data["is_archived"] is False


async def test_create_channel_feed_from_url_instagram(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    url_data = {"url": "https://www.instagram.com/testuser"}
    response = await api_key_client.post(
        "/api/media_feeds/channels/from-url",
        params={"organisation_id": str(organisation.id)},
        json=url_data,
    )
    assert response.status_code == 201
    response_data = response.json()["data"]
    assert response_data["organisation_id"] == str(organisation.id)
    assert response_data["channel"] == "testuser"
    assert response_data["platform"] == "instagram"


async def test_create_channel_feed_from_url_tiktok(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    url_data = {"url": "https://www.tiktok.com/@testuser"}
    response = await api_key_client.post(
        "/api/media_feeds/channels/from-url",
        params={"organisation_id": str(organisation.id)},
        json=url_data,
    )
    assert response.status_code == 201
    response_data = response.json()["data"]
    assert response_data["organisation_id"] == str(organisation.id)
    assert response_data["channel"] == "testuser"
    assert response_data["platform"] == "tiktok"


async def test_create_channel_feed_from_url_invalid(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    url_data = {"url": "https://www.example.com/invalid"}
    response = await api_key_client.post(
        "/api/media_feeds/channels/from-url",
        params={"organisation_id": str(organisation.id)},
        json=url_data,
    )
    assert response.status_code == 400


async def test_create_channel_feed_conflict(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    channel_feed_data = {
        "channel": "test_channel",
        "platform": "youtube",
    }

    response1 = await api_key_client.post(
        "/api/media_feeds/channels",
        params={"organisation_id": str(organisation.id)},
        json=channel_feed_data,
    )
    assert response1.status_code == 201

    response2 = await api_key_client.post(
        "/api/media_feeds/channels",
        params={"organisation_id": str(organisation.id)},
        json=channel_feed_data,
    )
    assert response2.status_code == 409


async def test_get_channel_feeds(
    api_key_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
) -> None:
    org1 = await create_organisation(auth_service)
    org2 = await create_organisation(auth_service)

    await create_channel_feed(api_key_client, organisation=org1)
    await create_channel_feed(api_key_client, organisation=org1)
    await create_channel_feed(api_key_client, organisation=org2)

    response = await api_key_client.get(
        "/api/media_feeds/channels",
        params={"organisation_id": str(org1.id)},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) == 2
    for feed in data:
        assert feed["organisation_id"] == str(org1.id)


async def test_update_channel_feed(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
    channel_feed: ChannelFeed,
) -> None:
    updated_data = {
        "channel": "updated_channel",
        "platform": "tiktok",
    }

    response = await api_key_client.patch(
        f"/api/media_feeds/channels/{channel_feed.id}",
        params={"organisation_id": str(organisation.id)},
        json=updated_data,
    )
    assert response.status_code == 200
    response_data = response.json()["data"]
    assert response_data["channel"] == "updated_channel"
    assert response_data["platform"] == "tiktok"


async def test_update_channel_feed_not_found(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    non_existent_id = uuid.uuid4()
    updated_data = {
        "channel": "updated_channel",
        "platform": "tiktok",
        "is_archived": False,
    }

    response = await api_key_client.patch(
        f"/api/media_feeds/channels/{non_existent_id}",
        params={"organisation_id": str(organisation.id)},
        json=updated_data,
    )
    assert response.status_code == 404


async def test_archive_channel_feed(
    api_key_client: AsyncTestClient[Litestar],
    channel_feed: ChannelFeed,
    organisation: Organisation,
) -> None:
    response = await api_key_client.delete(
        f"/api/media_feeds/channels/{channel_feed.id}",
        params={"organisation_id": str(organisation.id)},
    )
    assert response.status_code == 204


async def test_archive_channel_feed_not_found(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    non_existent_id = uuid.uuid4()
    response = await api_key_client.delete(
        f"/api/media_feeds/channels/{non_existent_id}",
        params={"organisation_id": str(organisation.id)},
    )
    assert response.status_code == 404


async def test_create_keyword_feed(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    keyword_feed = KeywordFeedFactory.build()
    keyword_feed_json = keyword_feed.model_dump(
        mode="json", exclude={"id", "created_at", "updated_at"}
    )
    response = await api_key_client.post(
        "/api/media_feeds/keywords",
        params={"organisation_id": str(organisation.id)},
        json=keyword_feed_json,
    )
    assert response.status_code == 201
    response_data = response.json()["data"]
    assert response_data["organisation_id"] == str(organisation.id)
    assert response_data["topic"] == keyword_feed.topic
    assert response_data["keywords"] == keyword_feed.keywords
    assert response_data["is_archived"] is False


async def test_create_keyword_feed_conflict(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    keyword_feed_data = {
        "topic": "test_topic",
        "keywords": ["keyword1", "keyword2"],
        "is_archived": False,
    }

    response1 = await api_key_client.post(
        "/api/media_feeds/keywords",
        params={"organisation_id": str(organisation.id)},
        json=keyword_feed_data,
    )
    assert response1.status_code == 201

    response2 = await api_key_client.post(
        "/api/media_feeds/keywords",
        params={"organisation_id": str(organisation.id)},
        json=keyword_feed_data,
    )
    assert response2.status_code == 409


async def test_get_keyword_feeds_by_organisation(
    api_key_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
) -> None:
    org1 = await create_organisation(auth_service)
    org2 = await create_organisation(auth_service)

    await create_keyword_feed(api_key_client, organisation=org1)
    await create_keyword_feed(api_key_client, organisation=org1)
    await create_keyword_feed(api_key_client, organisation=org2)

    response = await api_key_client.get(
        "/api/media_feeds/keywords",
        params={"organisation_id": str(org1.id)},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) == 2
    for feed in data:
        assert feed["organisation_id"] == str(org1.id)


async def test_update_keyword_feed(
    api_key_client: AsyncTestClient[Litestar],
    keyword_feed: KeywordFeed,
    organisation: Organisation,
) -> None:
    updated_data = {
        "topic": "updated_topic",
        "keywords": ["new_keyword1", "new_keyword2"],
    }

    response = await api_key_client.patch(
        f"/api/media_feeds/keywords/{keyword_feed.id}",
        params={"organisation_id": str(organisation.id)},
        json=updated_data,
    )
    assert response.status_code == 200
    response_data = response.json()["data"]
    assert response_data["topic"] == "updated_topic"
    assert response_data["keywords"] == ["new_keyword1", "new_keyword2"]


async def test_update_keyword_feed_not_found(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    non_existent_id = uuid.uuid4()
    updated_data = {
        "topic": "updated_topic",
        "keywords": ["new_keyword1", "new_keyword2"],
    }

    response = await api_key_client.patch(
        f"/api/media_feeds/keywords/{non_existent_id}",
        params={"organisation_id": str(organisation.id)},
        json=updated_data,
    )
    assert response.status_code == 404


async def test_archive_keyword_feed(
    api_key_client: AsyncTestClient[Litestar],
    keyword_feed: KeywordFeed,
    organisation: Organisation,
) -> None:
    response = await api_key_client.delete(
        f"/api/media_feeds/keywords/{keyword_feed.id}",
        params={"organisation_id": str(organisation.id)},
    )
    assert response.status_code == 204


async def test_archive_keyword_feed_not_found(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    non_existent_id = uuid.uuid4()
    response = await api_key_client.delete(
        f"/api/media_feeds/keywords/{non_existent_id}",
        params={"organisation_id": str(organisation.id)},
    )
    assert response.status_code == 404


async def test_get_organisation_feeds(
    api_key_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
) -> None:
    org1 = await create_organisation(auth_service)
    org2 = await create_organisation(auth_service)

    await create_channel_feed(api_key_client, organisation=org1)
    await create_keyword_feed(api_key_client, organisation=org1)
    await create_channel_feed(api_key_client, organisation=org2)

    response = await api_key_client.get(
        "/api/media_feeds/",
        params={"organisation_id": str(org1.id)},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert "channel_feeds" in data
    assert "keyword_feeds" in data
    assert len(data["channel_feeds"]) == 1
    assert len(data["keyword_feeds"]) == 1
    assert data["channel_feeds"][0]["organisation_id"] == str(org1.id)
    assert data["keyword_feeds"][0]["organisation_id"] == str(org1.id)


async def test_get_organisation_feeds_empty(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    response = await api_key_client.get(
        "/api/media_feeds/",
        params={"organisation_id": str(organisation.id)},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert "channel_feeds" in data
    assert "keyword_feeds" in data
    assert len(data["channel_feeds"]) == 0
    assert len(data["keyword_feeds"]) == 0


async def test_get_all_feeds(
    api_key_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
) -> None:
    org1 = await create_organisation(auth_service)
    org2 = await create_organisation(auth_service)

    await create_channel_feed(api_key_client, organisation=org1)
    await create_keyword_feed(api_key_client, organisation=org1)
    await create_channel_feed(api_key_client, organisation=org2)
    await create_keyword_feed(api_key_client, organisation=org2)

    response = await api_key_client.get("/api/media_feeds/all")
    assert response.status_code == 200
    data = response.json()["data"]
    assert "channel_feeds" in data
    assert "keyword_feeds" in data
    assert len(data["channel_feeds"]) == 2
    assert len(data["keyword_feeds"]) == 2

    org_ids = {feed["organisation_id"] for feed in data["channel_feeds"]}
    assert str(org1.id) in org_ids
    assert str(org2.id) in org_ids

    org_ids_keywords = {feed["organisation_id"] for feed in data["keyword_feeds"]}
    assert str(org1.id) in org_ids_keywords
    assert str(org2.id) in org_ids_keywords


async def test_get_all_feeds_empty(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    response = await api_key_client.get("/api/media_feeds/all")
    assert response.status_code == 200
    data = response.json()["data"]
    assert "channel_feeds" in data
    assert "keyword_feeds" in data
    assert len(data["channel_feeds"]) == 0
    assert len(data["keyword_feeds"]) == 0


async def test_get_cursor_not_found(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    response = await api_key_client.get(
        "/api/media_feeds/cursors/test_target/youtube",
    )
    assert response.status_code == 404


async def test_set_cursor(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    cursor_data = {"page": 1, "offset": 100}
    response = await api_key_client.post(
        "/api/media_feeds/cursors/test_target/youtube",
        json=cursor_data,
    )
    assert response.status_code == 201
    response_data = response.json()["data"]
    assert response_data["cursor"] == {"page": 1, "offset": 100}


async def test_get_cursor(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    cursor_data = {"page": 2, "offset": 200}
    await api_key_client.post(
        "/api/media_feeds/cursors/test_channel/instagram",
        json=cursor_data,
    )

    response = await api_key_client.get(
        "/api/media_feeds/cursors/test_channel/instagram",
    )
    assert response.status_code == 200
    response_data = response.json()["data"]
    assert response_data["cursor"] == {"page": 2, "offset": 200}


async def test_update_cursor(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    initial_cursor_data = {"page": 1, "offset": 100}
    await api_key_client.post(
        "/api/media_feeds/cursors/update_test/tiktok",
        json=initial_cursor_data,
    )

    updated_cursor_data = {"page": 5, "offset": 500}
    response = await api_key_client.post(
        "/api/media_feeds/cursors/update_test/tiktok",
        json=updated_cursor_data,
    )
    assert response.status_code == 201
    response_data = response.json()["data"]
    assert response_data["cursor"] == {"page": 5, "offset": 500}


async def test_delete_cursor(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    cursor_data = {"cursor": {"page": 1, "offset": 100}}
    await api_key_client.post(
        "/api/media_feeds/cursors/delete_test/youtube",
        json=cursor_data,
    )

    response = await api_key_client.delete(
        "/api/media_feeds/cursors/delete_test/youtube",
    )
    assert response.status_code == 204

    get_response = await api_key_client.get(
        "/api/media_feeds/cursors/delete_test/youtube",
    )
    assert get_response.status_code == 404


async def test_delete_cursor_not_found(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    response = await api_key_client.delete(
        "/api/media_feeds/cursors/nonexistent/youtube",
    )
    assert response.status_code == 404

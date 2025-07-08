from typing import Any
import uuid
from litestar import Litestar
from litestar.testing import AsyncTestClient
from polyfactory.factories.pydantic_factory import ModelFactory
from pytest import fixture

from core.videos.models import Video


class VideoFactory(ModelFactory[Video]):
    # Creates fake data for testing purposes
    ...


async def create_video(
    api_key_client: AsyncTestClient[Litestar], **args: dict[str, Any]
) -> Video:
    video = VideoFactory.build()
    video_json = video.model_dump(mode="json")
    response = await api_key_client.post(
        "/api/videos/",
        json=video_json,
    )
    assert response.status_code == 201
    return video


@fixture
async def video(api_key_client: AsyncTestClient[Litestar]) -> Video:
    return await create_video(api_key_client)


async def test_add_video(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    video = VideoFactory.build()
    video_json = video.model_dump(mode="json")
    response = await api_key_client.post(
        "/api/videos/",
        json=video_json,
    )
    assert response.status_code == 201
    assert response.json() == {"data": video_json}


async def test_get_video(
    api_key_client: AsyncTestClient[Litestar],
    video: Video,
) -> None:
    response = await api_key_client.get(f"/api/videos/{video.id}")
    assert response.status_code == 200
    assert response.json() == {"data": video.model_dump(mode="json")}


async def test_get_video_does_not_exist(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    response = await api_key_client.get(f"/api/videos/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_delete_video(
    api_key_client: AsyncTestClient[Litestar], video: Video
) -> None:
    response = await api_key_client.get(f"/api/videos/{video.id}")
    assert response.status_code == 200
    assert response.json() == {"data": video.model_dump(mode="json")}

    delete = await api_key_client.delete(f"/api/videos/{video.id}")
    assert delete.status_code == 204

    deleted = await api_key_client.get(f"/api/videos/{video.id}")
    assert deleted.status_code == 404


async def test_update_video(
    api_key_client: AsyncTestClient[Litestar], video: Video
) -> None:
    updated_fields = {
        "title": "Updated title",
        "description": "updated description",
        "views": 56,
        "likes": 78,
        "comments": 128,
        "channel_followers": 95464,
    }
    update_response = await api_key_client.patch(
        f"/api/videos/{video.id}",
        json=updated_fields,
    )
    assert update_response.status_code == 200
    assert update_response.json() == {
        "data": video.model_dump(mode="json") | updated_fields
    }


async def test_empty_filter_response(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    response = await api_key_client.post(
        "/api/videos/filter",
        json={},
    )
    assert response.status_code == 200
    assert response.json() == {"data": [], "cursor": None}


async def test_filter_cursor(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    videos = [
        (await create_video(api_key_client)).model_dump(mode="json") for _ in range(50)
    ][::-1]

    response_videos = []
    cursor = None
    while True:
        response = await api_key_client.post(
            "/api/videos/filter",
            json={
                "limit": 10,
                "cursor": cursor,
            },
        )
        assert response.status_code == 200
        cursor = response.json().get("cursor")
        new_videos = response.json().get("data")
        response_videos.extend(new_videos)

        if not new_videos or not cursor:
            break

    assert response_videos == videos

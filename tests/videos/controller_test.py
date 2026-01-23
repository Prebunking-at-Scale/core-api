import uuid
from unittest.mock import ANY

from litestar import Litestar
from litestar.testing import AsyncTestClient

from core.models import Video
from tests.videos.conftest import VideoFactory, create_video


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
    assert response.json() == {
        "data": video.model_dump(mode="json")
        | {
            "transcript": ANY,
            "claims": ANY,
            "narratives": ANY,
            "stats_history": ANY,
        }
    }


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
    videos = []
    for _ in range(5):
        video = (await create_video(api_key_client)).model_dump(mode="json")
        videos.append(video)
    videos = videos[::-1]

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


async def test_stats_history_created_on_video_add(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    video = VideoFactory.build()
    video_json = video.model_dump(mode="json")

    add_response = await api_key_client.post(
        "/api/videos/",
        json=video_json,
    )
    assert add_response.status_code == 201

    get_response = await api_key_client.get(f"/api/videos/{video.id}")
    assert get_response.status_code == 200

    data = get_response.json()["data"]
    stats_history = data["stats_history"]

    assert len(stats_history) == 1
    assert stats_history[0]["video_id"] == str(video.id)
    assert stats_history[0]["views"] == video.views
    assert stats_history[0]["likes"] == video.likes
    assert stats_history[0]["comments"] == video.comments
    assert stats_history[0]["channel_followers"] == video.channel_followers
    assert "recorded_at" in stats_history[0]


async def test_stats_history_updated_on_video_patch(
    api_key_client: AsyncTestClient[Litestar],
    video: Video,
) -> None:
    initial_response = await api_key_client.get(f"/api/videos/{video.id}")
    assert initial_response.status_code == 200
    initial_stats_history = initial_response.json()["data"]["stats_history"]
    assert len(initial_stats_history) == 1

    updated_stats = {
        "views": 1000,
        "likes": 200,
        "comments": 50,
        "channel_followers": 5000,
    }

    patch_response = await api_key_client.patch(
        f"/api/videos/{video.id}",
        json=updated_stats,
    )
    assert patch_response.status_code == 200

    updated_response = await api_key_client.get(f"/api/videos/{video.id}")
    assert updated_response.status_code == 200

    data = updated_response.json()["data"]
    stats_history = data["stats_history"]

    assert len(stats_history) == 2

    assert stats_history[0]["views"] == updated_stats["views"]
    assert stats_history[0]["likes"] == updated_stats["likes"]
    assert stats_history[0]["comments"] == updated_stats["comments"]
    assert stats_history[0]["channel_followers"] == updated_stats["channel_followers"]

    assert stats_history[1]["views"] == video.views
    assert stats_history[1]["likes"] == video.likes
    assert stats_history[1]["comments"] == video.comments
    assert stats_history[1]["channel_followers"] == video.channel_followers


async def test_stats_history_multiple_updates(
    api_key_client: AsyncTestClient[Litestar],
    video: Video,
) -> None:
    updates = [
        {"views": 100, "likes": 10, "comments": 5, "channel_followers": 1000},
        {"views": 200, "likes": 20, "comments": 10, "channel_followers": 2000},
        {"views": 300, "likes": 30, "comments": 15, "channel_followers": 3000},
    ]

    for update in updates:
        response = await api_key_client.patch(
            f"/api/videos/{video.id}",
            json=update,
        )
        assert response.status_code == 200

    final_response = await api_key_client.get(f"/api/videos/{video.id}")
    assert final_response.status_code == 200

    stats_history = final_response.json()["data"]["stats_history"]

    assert len(stats_history) == 4

    for i, update in enumerate(reversed(updates)):
        assert stats_history[i]["views"] == update["views"]
        assert stats_history[i]["likes"] == update["likes"]
        assert stats_history[i]["comments"] == update["comments"]
        assert stats_history[i]["channel_followers"] == update["channel_followers"]


async def test_stats_history_preserves_current_stats(
    api_key_client: AsyncTestClient[Litestar],
    video: Video,
) -> None:
    new_stats = {
        "views": 9999,
        "likes": 888,
        "comments": 77,
        "channel_followers": 12345,
    }

    await api_key_client.patch(
        f"/api/videos/{video.id}",
        json=new_stats,
    )

    response = await api_key_client.get(f"/api/videos/{video.id}")
    assert response.status_code == 200

    data = response.json()["data"]

    assert data["views"] == new_stats["views"]
    assert data["likes"] == new_stats["likes"]
    assert data["comments"] == new_stats["comments"]
    assert data["channel_followers"] == new_stats["channel_followers"]


async def test_get_videos_by_expected_views_default_limit(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    response = await api_key_client.get("/api/videos/by-expected-views")
    assert response.status_code == 200
    assert "data" in response.json()
    assert isinstance(response.json()["data"], list)


async def test_get_videos_by_expected_views_custom_limit(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    for i in range(10):
        video = VideoFactory.build(views=100 * (i + 1))
        await api_key_client.post("/api/videos/", json=video.model_dump(mode="json"))

    response = await api_key_client.get("/api/videos/by-expected-views?limit=5")
    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) <= 5


async def test_get_videos_by_expected_views_ordering(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    import asyncio

    video1 = VideoFactory.build(views=100)
    video1_response = await api_key_client.post(
        "/api/videos/", json=video1.model_dump(mode="json")
    )
    assert video1_response.status_code == 201

    await asyncio.sleep(0.1)

    await api_key_client.patch(
        f"/api/videos/{video1.id}",
        json={"views": 200},
    )

    video2 = VideoFactory.build(views=100)
    video2_response = await api_key_client.post(
        "/api/videos/", json=video2.model_dump(mode="json")
    )
    assert video2_response.status_code == 201

    await asyncio.sleep(0.1)

    await api_key_client.patch(
        f"/api/videos/{video2.id}",
        json={"views": 500},
    )

    await asyncio.sleep(0.1)

    response = await api_key_client.get("/api/videos/by-expected-views?min_age_hours=0")
    assert response.status_code == 200

    videos = response.json()["data"]
    assert len(videos) >= 2

    video_ids = [v["id"] for v in videos]
    video2_index = video_ids.index(str(video2.id))
    video1_index = video_ids.index(str(video1.id))

    assert video2_index < video1_index


async def test_get_videos_by_expected_views_min_age_filter(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    import asyncio
    from datetime import datetime, timedelta

    old_video = VideoFactory.build(views=1000)
    old_video_response = await api_key_client.post(
        "/api/videos/", json=old_video.model_dump(mode="json")
    )
    assert old_video_response.status_code == 201

    await asyncio.sleep(2)

    new_video = VideoFactory.build(views=1000)
    new_video_response = await api_key_client.post(
        "/api/videos/", json=new_video.model_dump(mode="json")
    )
    assert new_video_response.status_code == 201

    response_no_filter = await api_key_client.get(
        "/api/videos/by-expected-views?min_age_hours=0"
    )
    assert response_no_filter.status_code == 200
    videos_no_filter = response_no_filter.json()["data"]
    video_ids_no_filter = [v["id"] for v in videos_no_filter]

    assert str(old_video.id) in video_ids_no_filter
    assert str(new_video.id) in video_ids_no_filter

    response_with_filter = await api_key_client.get(
        "/api/videos/by-expected-views?min_age_hours=0.0005"
    )
    assert response_with_filter.status_code == 200
    videos_with_filter = response_with_filter.json()["data"]
    video_ids_with_filter = [v["id"] for v in videos_with_filter]

    assert str(old_video.id) in video_ids_with_filter
    assert str(new_video.id) not in video_ids_with_filter


async def test_get_videos_by_expected_views_platform_filter(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    youtube_video = VideoFactory.build(platform="youtube", views=1000)
    await api_key_client.post(
        "/api/videos/", json=youtube_video.model_dump(mode="json")
    )

    tiktok_video = VideoFactory.build(platform="tiktok", views=1000)
    await api_key_client.post(
        "/api/videos/", json=tiktok_video.model_dump(mode="json")
    )

    response = await api_key_client.get(
        "/api/videos/by-expected-views?min_age_hours=0&platform=youtube"
    )
    assert response.status_code == 200
    videos = response.json()["data"]
    video_ids = [v["id"] for v in videos]

    assert str(youtube_video.id) in video_ids
    assert str(tiktok_video.id) not in video_ids


async def test_get_videos_by_expected_views_platform_filter_case_insensitive(
    api_key_client: AsyncTestClient[Litestar],
) -> None:
    video = VideoFactory.build(platform="YouTube", views=1000)
    await api_key_client.post("/api/videos/", json=video.model_dump(mode="json"))

    response = await api_key_client.get(
        "/api/videos/by-expected-views?min_age_hours=0&platform=youtube"
    )
    assert response.status_code == 200
    videos = response.json()["data"]
    video_ids = [v["id"] for v in videos]

    assert str(video.id) in video_ids

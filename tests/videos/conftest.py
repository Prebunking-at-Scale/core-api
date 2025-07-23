from typing import Any

from litestar import Litestar
from litestar.testing import AsyncTestClient
from polyfactory.factories.pydantic_factory import ModelFactory
from pytest import fixture

from core.models import Transcript, Video
from core.videos.claims.models import VideoClaims


class VideoFactory(ModelFactory[Video]):
    __check_model__ = True


class TranscriptFactory(ModelFactory[Transcript]):
    __check_model__ = True


class ClaimsFactory(ModelFactory[VideoClaims]):
    __check_model__ = True


@fixture
def tables_to_truncate() -> list[str]:
    return ["videos"]


async def create_video(
    api_key_client: AsyncTestClient[Litestar], **kwargs: dict[str, Any]
) -> Video:
    video = VideoFactory.build(kwargs=kwargs)
    response = await api_key_client.post(
        "/api/videos/",
        json=video.model_dump(mode="json"),
    )
    assert response.status_code == 201
    return video


@fixture
async def video(api_key_client: AsyncTestClient[Litestar]) -> Video:
    return await create_video(api_key_client)


async def create_transcript(
    api_key_client: AsyncTestClient[Litestar],
    video: Video,
    **kwargs: dict[str, Any],
) -> Transcript:
    transcript = TranscriptFactory.build(video_id=video.id, kwargs=kwargs)
    response = await api_key_client.post(
        f"/api/videos/{video.id}/transcript",
        json=transcript.model_dump(mode="json"),
    )
    assert response.status_code == 201
    return transcript


@fixture
async def transcript(
    api_key_client: AsyncTestClient[Litestar], video: Video
) -> Transcript:
    return await create_transcript(api_key_client, video)


async def create_video_claims(
    api_key_client: AsyncTestClient[Litestar],
    video: Video,
    **kwargs: dict[str, Any],
) -> VideoClaims:
    video_claims = ClaimsFactory.build(video_id=video.id, kwargs=kwargs)
    response = await api_key_client.post(
        f"/api/videos/{video.id}/claims",
        json=video_claims.model_dump(mode="json"),
    )
    assert response.status_code == 201
    return video_claims


@fixture
async def video_claims(
    api_key_client: AsyncTestClient[Litestar], video: Video
) -> VideoClaims:
    return await create_video_claims(api_key_client, video)

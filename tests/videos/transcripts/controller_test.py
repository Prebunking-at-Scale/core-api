from unittest.mock import ANY

from litestar import Litestar
from litestar.testing import AsyncTestClient

from core.videos.models import Video
from core.videos.transcripts.models import Transcript
from tests.videos.conftest import TranscriptFactory


async def test_add_transcript(
    api_key_client: AsyncTestClient[Litestar], video: Video
) -> None:
    print(video.id)
    transcript = TranscriptFactory.build(video_id=video.id)
    transcript_json = transcript.model_dump(mode="json")
    response = await api_key_client.post(
        f"/api/videos/{video.id}/transcript",
        json=transcript_json,
    )
    assert response.status_code == 201
    transcript_json["sentences"][0]["embedding"] = ANY
    assert response.json() | {"data": transcript_json} == response.json()


async def test_get_empty_transcript(
    api_key_client: AsyncTestClient[Litestar], video: Video
) -> None:
    response = await api_key_client.get(f"/api/videos/{video.id}/transcript")
    assert response.status_code == 200
    assert response.json() == {"data": {"sentences": [], "video_id": str(video.id)}}


async def test_get_transcript(
    api_key_client: AsyncTestClient[Litestar], video: Video, transcript: Transcript
) -> None:
    response = await api_key_client.get(f"/api/videos/{video.id}/transcript")
    assert response.status_code == 200
    transcript_json = transcript.model_dump(mode="json")
    transcript_json["sentences"][0]["embedding"] = ANY
    assert response.json() == {"data": transcript_json}


async def test_delete_transcript(
    api_key_client: AsyncTestClient[Litestar], video: Video, transcript: Transcript
) -> None:
    response = await api_key_client.get(f"/api/videos/{video.id}/transcript")
    assert response.status_code == 200
    assert len(response.json()["data"]["sentences"]) > 0

    response = await api_key_client.delete(f"/api/videos/{video.id}/transcript")
    assert response.status_code == 204

    response = await api_key_client.get(f"/api/videos/{video.id}/transcript")
    assert response.status_code == 200
    assert response.json() == {"data": {"sentences": [], "video_id": str(video.id)}}


async def test_delete_video_also_deletes_transcript(
    api_key_client: AsyncTestClient[Litestar], video: Video, transcript: Transcript
) -> None:
    response = await api_key_client.get(f"/api/videos/{video.id}/transcript")
    assert response.status_code == 200

    response = await api_key_client.delete(f"/api/videos/{video.id}")
    assert response.status_code == 204

    response = await api_key_client.get(f"/api/videos/{video.id}/transcript")
    assert response.status_code == 404


async def test_delete_sentence(
    api_key_client: AsyncTestClient[Litestar], video: Video, transcript: Transcript
) -> None:
    response = await api_key_client.get(f"/api/videos/{video.id}/transcript")
    assert response.status_code == 200

    sentences = response.json().get("data").get("sentences")
    assert len(sentences) == 1
    sentence = sentences[0]

    response = await api_key_client.delete(
        f"/api/videos/{video.id}/transcript/{sentence['id']}"
    )
    assert response.status_code == 204

    response = await api_key_client.get(f"/api/videos/{video.id}/transcript")
    assert response.status_code == 200
    assert response.json() == {"data": {"sentences": [], "video_id": str(video.id)}}


async def test_update_sentence_metadata(
    api_key_client: AsyncTestClient[Litestar], video: Video, transcript: Transcript
) -> None:
    sentences = transcript.sentences
    sentence = sentences[0]

    updated_metadata = {"new": "test"}
    update_response = await api_key_client.patch(
        f"/api/videos/{video.id}/transcript/{sentence.id}/metadata",
        json=updated_metadata,
    )
    assert update_response.status_code == 200
    assert update_response.json() == {"data": sentence.metadata | updated_metadata}

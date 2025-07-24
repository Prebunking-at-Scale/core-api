from unittest.mock import ANY

from litestar import Litestar
from litestar.testing import AsyncTestClient

from core.models import Video
from core.videos.claims.models import VideoClaims
from tests.videos.conftest import ClaimsFactory


async def test_add_claims(
    api_key_client: AsyncTestClient[Litestar], video: Video
) -> None:
    claims = ClaimsFactory.build(video_id=video.id)
    claims_json = claims.model_dump(mode="json")
    response = await api_key_client.post(
        f"/api/videos/{video.id}/claims",
        json=claims_json,
    )
    claims_json["claims"][0] = claims_json["claims"][0] | {
        "created_at": ANY,
        "updated_at": ANY,
        "video_id": str(video.id),
    }
    assert response.status_code == 201
    assert response.json() == {"data": claims_json}


async def test_get_empty_claims(
    api_key_client: AsyncTestClient[Litestar], video: Video
) -> None:
    response = await api_key_client.get(f"/api/videos/{video.id}/claims")
    assert response.status_code == 200
    assert response.json() == {"data": {"claims": [], "video_id": str(video.id)}}


async def test_get_claims(
    api_key_client: AsyncTestClient[Litestar], video: Video, video_claims: VideoClaims
) -> None:
    response = await api_key_client.get(f"/api/videos/{video.id}/claims")
    assert response.status_code == 200
    claims_json = video_claims.model_dump(mode="json")
    claims_json["claims"][0] = claims_json["claims"][0] | {
        "created_at": ANY,
        "updated_at": ANY,
        "video_id": str(video.id),
    }
    assert response.json() == {"data": claims_json}


async def test_delete_claims(
    api_key_client: AsyncTestClient[Litestar],
    video: Video,
    video_claims: VideoClaims,  # side effects
) -> None:
    response = await api_key_client.get(f"/api/videos/{video.id}/claims")
    assert response.status_code == 200
    assert len(response.json()["data"]["claims"]) > 0

    response = await api_key_client.delete(f"/api/videos/{video.id}/claims")
    assert response.status_code == 204

    response = await api_key_client.get(f"/api/videos/{video.id}/claims")
    assert response.status_code == 200
    assert response.json() == {"data": {"claims": [], "video_id": str(video.id)}}


async def test_delete_video_also_deletes_claims(
    api_key_client: AsyncTestClient[Litestar],
    video: Video,
    video_claims: VideoClaims,  # side effects
) -> None:
    response = await api_key_client.get(f"/api/videos/{video.id}/claims")
    assert response.status_code == 200

    response = await api_key_client.delete(f"/api/videos/{video.id}")
    assert response.status_code == 204

    response = await api_key_client.get(f"/api/videos/{video.id}/claims")
    assert response.status_code == 404


async def test_delete_claim(
    api_key_client: AsyncTestClient[Litestar],
    video: Video,
    video_claims: VideoClaims,  # side effects
) -> None:
    response = await api_key_client.get(f"/api/videos/{video.id}/claims")
    assert response.status_code == 200

    claims = response.json().get("data").get("claims")
    assert len(claims) == 1
    claim = claims[0]

    response = await api_key_client.delete(
        f"/api/videos/{video.id}/claims/{claim['id']}"
    )
    assert response.status_code == 204

    response = await api_key_client.get(f"/api/videos/{video.id}/claims")
    assert response.status_code == 200
    assert response.json() == {"data": {"claims": [], "video_id": str(video.id)}}


async def test_update_claim_metadata(
    api_key_client: AsyncTestClient[Litestar], video: Video, video_claims: VideoClaims
) -> None:
    claim = video_claims.claims[0]
    updated_metadata = {"new": "test"}
    update_response = await api_key_client.patch(
        f"/api/videos/{video.id}/claims/{claim.id}/metadata",
        json=updated_metadata,
    )
    assert update_response.status_code == 200
    assert update_response.json() == {"data": claim.metadata | updated_metadata}

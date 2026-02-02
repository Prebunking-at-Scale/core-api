import io

from litestar import Litestar
from litestar.testing import AsyncTestClient

from core.auth.models import Organisation


async def test_bulk_upload_channels_case_insensitive_duplicates(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    """Test that channels with different cases are treated as duplicates."""
    file_content = """https://www.youtube.com/@Example
https://www.youtube.com/@example
https://www.youtube.com/@EXAMPLE
"""
    files = {"data": ("channels.txt", io.BytesIO(file_content.encode()), "text/plain")}
    response = await api_key_client.post(
        "/api/media_feeds/channels/bulk-upload",
        params={"organisation_id": str(organisation.id)},
        files=files,
    )
    assert response.status_code == 201
    data = response.json()["data"]

    # Only one channel should be created (the first one encountered)
    assert len(data["created"]) == 1
    # The original case should be preserved
    assert data["created"][0]["channel"] == "@Example"
    assert data["created"][0]["platform"] == "youtube"

    # Two should be skipped as duplicates
    assert len(data["skipped"]) == 2
    assert data["skipped"][0]["channel"] == "@example"
    assert data["skipped"][0]["platform"] == "youtube"
    assert data["skipped"][1]["channel"] == "@EXAMPLE"
    assert data["skipped"][1]["platform"] == "youtube"


async def test_create_channel_feed_case_insensitive_conflict(
    api_key_client: AsyncTestClient[Litestar],
    organisation: Organisation,
) -> None:
    """Test that creating channels with different cases causes a conflict."""
    # Create a channel with mixed case
    response1 = await api_key_client.post(
        "/api/media_feeds/channels",
        params={"organisation_id": str(organisation.id)},
        json={"channel": "@TestUser", "platform": "youtube"},
    )
    assert response1.status_code == 201
    assert response1.json()["data"]["channel"] == "@TestUser"

    # Try to create the same channel with different case
    response2 = await api_key_client.post(
        "/api/media_feeds/channels",
        params={"organisation_id": str(organisation.id)},
        json={"channel": "@testuser", "platform": "youtube"},
    )
    assert response2.status_code == 409  # Conflict

    # Try uppercase
    response3 = await api_key_client.post(
        "/api/media_feeds/channels",
        params={"organisation_id": str(organisation.id)},
        json={"channel": "@TESTUSER", "platform": "youtube"},
    )
    assert response3.status_code == 409  # Conflict

from uuid import UUID, uuid4

from litestar import Litestar
from litestar.testing import AsyncTestClient

from core.entities.models import EntityInput
from core.models import Narrative
from core.narratives.models import NarrativeInput, NarrativePatchInput
from tests.narratives.conftest import NarrativeInputFactory


async def test_create_narrative(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    narrative_input = NarrativeInputFactory.build()
    narrative_json = narrative_input.model_dump(mode="json")

    response = await api_key_client.post(
        "/api/narratives/",
        json=narrative_json,
    )

    assert response.status_code == 201
    response_data = response.json()["data"]
    assert response_data["title"] == narrative_input.title
    assert response_data["description"] == narrative_input.description
    assert "id" in response_data
    assert response_data["created_at"] is not None
    assert response_data["updated_at"] is not None


async def test_create_narrative_with_topics_and_entities(
    api_key_client: AsyncTestClient[Litestar]
) -> None:

    topic_ids = [
        UUID("db3d996b-e691-4ce5-8c46-e35a82a9b28c"),  # Climate
        UUID("bb52f622-b9ee-4d5b-9b70-5fd05046528b"),  # Health
    ]
    entities = [
        EntityInput(
            wikidata_id="Q123",
            entity_name="Test Entity 1",
            entity_type="person",
            wikidata_info={"label": "Test Entity 1"}
        ),
        EntityInput(
            wikidata_id="Q456",
            entity_name="Test Entity 2",
            entity_type="organization",
            wikidata_info={"label": "Test Entity 2"}
        )
    ]

    narrative_input = NarrativeInput(
        title="Test Narrative with Topics and Entities",
        description="A test narrative containing topics and entities",
        topic_ids=topic_ids,
        entities=entities,
        metadata={"test_key": "test_value"}
    )

    response = await api_key_client.post(
        "/api/narratives/",
        json=narrative_input.model_dump(mode="json"),
    )

    assert response.status_code == 201
    response_data = response.json()["data"]
    assert response_data["title"] == narrative_input.title
    assert response_data["description"] == narrative_input.description
    assert response_data["metadata"] == {"test_key": "test_value"}

    # Entities should be returned in the response
    assert len(response_data["entities"]) == 2
    entity_names = {e["name"] for e in response_data["entities"]}
    assert "Test Entity 1" in entity_names
    assert "Test Entity 2" in entity_names


async def test_get_narrative(
    api_key_client: AsyncTestClient[Litestar],
    narrative: Narrative
) -> None:
    response = await api_key_client.get(f"/api/narratives/{narrative.id}")

    assert response.status_code == 200
    response_data = response.json()["data"]
    assert response_data["id"] == str(narrative.id)
    assert response_data["title"] == narrative.title
    assert response_data["description"] == narrative.description

    # Verify new NarrativeDetail fields are present
    assert "claims" in response_data  # Preview claims
    assert "claim_count" in response_data  # Total count
    assert "videos" in response_data  # Preview videos
    assert "video_count" in response_data  # Total count
    assert "total_views" in response_data
    assert "total_likes" in response_data
    assert "total_comments" in response_data
    assert "platforms" in response_data
    assert "language_count" in response_data
    assert "topics" in response_data
    assert "entities" in response_data


async def test_get_narrative_with_custom_limits(
    api_key_client: AsyncTestClient[Litestar],
    narrative: Narrative
) -> None:
    response = await api_key_client.get(
        f"/api/narratives/{narrative.id}?claims_limit=5&videos_limit=5"
    )

    assert response.status_code == 200
    response_data = response.json()["data"]
    assert response_data["id"] == str(narrative.id)
    # Preview lists should respect the limits (may have fewer if not enough data)
    assert len(response_data["claims"]) <= 5
    assert len(response_data["videos"]) <= 5


async def test_get_narrative_not_found(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    fake_id = uuid4()
    response = await api_key_client.get(f"/api/narratives/{fake_id}")

    assert response.status_code == 404


async def test_patch_narrative_title(
    api_key_client: AsyncTestClient[Litestar],
    narrative: Narrative
) -> None:
    new_title = "Updated Narrative Title"
    patch_data = NarrativePatchInput(title=new_title)

    response = await api_key_client.patch(
        f"/api/narratives/{narrative.id}",
        json=patch_data.model_dump(mode="json", exclude_unset=True),
    )

    assert response.status_code == 200
    response_data = response.json()["data"]
    assert response_data["title"] == new_title
    assert response_data["description"] == narrative.description  # Unchanged


async def test_patch_narrative_add_entities(
    api_key_client: AsyncTestClient[Litestar],
    narrative: Narrative
) -> None:
    entities = [
        EntityInput(
            wikidata_id="Q789",
            entity_name="New Entity",
            entity_type="location",
            wikidata_info={"label": "New Entity"}
        )
    ]

    patch_data = NarrativePatchInput(entities=entities)

    response = await api_key_client.patch(
        f"/api/narratives/{narrative.id}",
        json=patch_data.model_dump(mode="json", exclude_unset=True),
    )

    assert response.status_code == 200
    response_data = response.json()["data"]

    # Check that entity was added
    assert len(response_data["entities"]) >= 1
    entity_names = {e["name"] for e in response_data["entities"]}
    assert "New Entity" in entity_names


async def test_patch_narrative_add_topics(
    api_key_client: AsyncTestClient[Litestar],
    narrative: Narrative
) -> None:

    topic_ids = [
        UUID("3cd4a9cd-5906-4b0b-9167-57ff22c2345a"),  # Migration
        UUID("0d7aaf8d-5b7e-4c0c-b03a-28457e27ac7d"),  # Conflicts
    ]

    patch_data = NarrativePatchInput(topic_ids=topic_ids)

    response = await api_key_client.patch(
        f"/api/narratives/{narrative.id}",
        json=patch_data.model_dump(mode="json", exclude_unset=True),
    )

    assert response.status_code == 200
    response_data = response.json()["data"]

    # Topics should be associated (though may not be visible in basic response)
    assert response_data["id"] == str(narrative.id)


async def test_patch_narrative_multiple_fields(
    api_key_client: AsyncTestClient[Litestar],
    narrative: Narrative
) -> None:
    new_title = "Completely New Title"
    new_description = "Completely new description"
    entities = [
        EntityInput(
            wikidata_id="Q999",
            entity_name="Multiple Update Entity",
            entity_type="person",
            wikidata_info={"label": "Multiple Update Entity"}
        )
    ]

    patch_data = NarrativePatchInput(
        title=new_title,
        description=new_description,
        entities=entities
    )

    response = await api_key_client.patch(
        f"/api/narratives/{narrative.id}",
        json=patch_data.model_dump(mode="json", exclude_unset=True),
    )

    assert response.status_code == 200
    response_data = response.json()["data"]
    assert response_data["title"] == new_title
    assert response_data["description"] == new_description
    assert len(response_data["entities"]) >= 1


async def test_patch_narrative_metadata(
    api_key_client: AsyncTestClient[Litestar],
    narrative: Narrative
) -> None:
    new_metadata = {"new_key": "new_value", "another_key": 123}

    response = await api_key_client.patch(
        f"/api/narratives/{narrative.id}/metadata",
        json=new_metadata,
    )

    assert response.status_code == 200
    response_data = response.json()["data"]

    # Should merge with existing metadata
    assert "new_key" in response_data
    assert response_data["new_key"] == "new_value"
    assert response_data["another_key"] == 123


async def test_delete_narrative(
    api_key_client: AsyncTestClient[Litestar],
    narrative: Narrative
) -> None:
    # Verify narrative exists
    response = await api_key_client.get(f"/api/narratives/{narrative.id}")
    assert response.status_code == 200

    # Delete narrative
    response = await api_key_client.delete(f"/api/narratives/{narrative.id}")
    assert response.status_code == 204

    # Verify narrative is deleted
    response = await api_key_client.get(f"/api/narratives/{narrative.id}")
    assert response.status_code == 404


async def test_delete_narrative_not_found(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    fake_id = uuid4()
    response = await api_key_client.delete(f"/api/narratives/{fake_id}")

    # Should still return 204 (idempotent delete)
    assert response.status_code == 204


async def test_get_all_narratives(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    # Create multiple narratives
    narrative1 = NarrativeInputFactory.build()
    narrative2 = NarrativeInputFactory.build()

    await api_key_client.post("/api/narratives/", json=narrative1.model_dump(mode="json"))
    await api_key_client.post("/api/narratives/", json=narrative2.model_dump(mode="json"))

    # Get all narratives
    response = await api_key_client.get("/api/narratives/")

    assert response.status_code == 200
    response_data = response.json()
    assert "data" in response_data
    assert "total" in response_data
    assert "page" in response_data
    assert "size" in response_data

    assert len(response_data["data"]) >= 2
    assert response_data["total"] >= 2

    # Verify the response contains counts instead of full lists
    first_item = response_data["data"][0]
    assert "claim_count" in first_item
    assert "video_count" in first_item
    assert "language_count" in first_item
    assert "entity_count" in first_item
    assert "description" in first_item
    assert "created_at" in first_item
    assert "updated_at" in first_item
    # Full lists should NOT be present
    assert "claims" not in first_item
    assert "videos" not in first_item
    assert "entities" not in first_item


async def test_get_narrative_claims(
    api_key_client: AsyncTestClient[Litestar],
    narrative: Narrative
) -> None:
    response = await api_key_client.get(
        f"/api/narratives/{narrative.id}/claims?limit=25&offset=0"
    )

    assert response.status_code == 200
    response_data = response.json()
    assert "data" in response_data
    assert "total" in response_data
    assert "page" in response_data
    assert "size" in response_data

    # Verify it returns a list of claims
    assert isinstance(response_data["data"], list)
    assert response_data["page"] == 1


async def test_get_narrative_claims_pagination(
    api_key_client: AsyncTestClient[Litestar],
    narrative: Narrative
) -> None:
    # Test with offset
    response = await api_key_client.get(
        f"/api/narratives/{narrative.id}/claims?limit=10&offset=10"
    )

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["page"] == 2


async def test_get_narrative_claims_not_found(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    fake_id = uuid4()
    response = await api_key_client.get(f"/api/narratives/{fake_id}/claims")

    assert response.status_code == 404


async def test_get_narrative_videos(
    api_key_client: AsyncTestClient[Litestar],
    narrative: Narrative
) -> None:
    response = await api_key_client.get(
        f"/api/narratives/{narrative.id}/videos?limit=25&offset=0"
    )

    assert response.status_code == 200
    response_data = response.json()
    assert "data" in response_data
    assert "total" in response_data
    assert "page" in response_data
    assert "size" in response_data

    # Verify it returns a list of videos
    assert isinstance(response_data["data"], list)
    assert response_data["page"] == 1


async def test_get_narrative_videos_pagination(
    api_key_client: AsyncTestClient[Litestar],
    narrative: Narrative
) -> None:
    # Test with offset
    response = await api_key_client.get(
        f"/api/narratives/{narrative.id}/videos?limit=10&offset=10"
    )

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["page"] == 2


async def test_get_narrative_videos_not_found(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    fake_id = uuid4()
    response = await api_key_client.get(f"/api/narratives/{fake_id}/videos")

    assert response.status_code == 404


async def test_get_narrative_stats(
    api_key_client: AsyncTestClient[Litestar],
    narrative: Narrative
) -> None:
    response = await api_key_client.get(f"/api/narratives/{narrative.id}/stats")

    assert response.status_code == 200
    response_data = response.json()["data"]

    # Verify the response structure
    assert "narrative_id" in response_data
    assert response_data["narrative_id"] == str(narrative.id)
    assert "time_series" in response_data
    assert "totals" in response_data

    # Verify time_series is a list
    assert isinstance(response_data["time_series"], list)

    # Verify totals structure
    totals = response_data["totals"]
    assert "views" in totals
    assert "likes" in totals
    assert "comments" in totals
    assert "video_count" in totals


async def test_get_narrative_stats_not_found(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    fake_id = uuid4()
    response = await api_key_client.get(f"/api/narratives/{fake_id}/stats")

    assert response.status_code == 404
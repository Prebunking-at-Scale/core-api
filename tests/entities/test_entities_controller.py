from uuid import UUID

from litestar import Litestar
from litestar.testing import AsyncTestClient

from core.entities.models import EntityInput
from core.narratives.models import NarrativeInput, NarrativePatchInput
from tests.entities.conftest import create_entity


async def test_get_all_entities_empty(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    """Test getting entities when none exist"""
    response = await api_key_client.get("/api/entities/")

    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "total" in data
    assert "page" in data
    assert "size" in data
    assert data["data"] == []
    assert data["total"] == 0


async def test_get_all_entities(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    """Test getting all entities after creating some"""
    # Create a narrative with entities
    entities = [
        EntityInput(
            wikidata_id="Q1",
            entity_name="Universe",
            entity_type="concept",
            wikidata_info={"label": "Universe", "description": "Everything that exists"}
        ),
        EntityInput(
            wikidata_id="Q2",
            entity_name="Earth",
            entity_type="planet",
            wikidata_info={"label": "Earth", "description": "Third planet from the Sun"}
        ),
        EntityInput(
            wikidata_id="Q5",
            entity_name="Human",
            entity_type="species",
            wikidata_info={"label": "Human", "description": "Homo sapiens"}
        )
    ]

    narrative_input = NarrativeInput(
        title="Entities Test Narrative",
        description="Narrative with multiple entities",
        entities=entities,
        claim_ids=[],
        topic_ids=[]
    )

    narrative_response = await api_key_client.post(
        "/api/narratives/",
        json=narrative_input.model_dump(mode="json"),
    )
    assert narrative_response.status_code == 201

    # Now get all entities
    response = await api_key_client.get("/api/entities/")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 3
    assert len(data["data"]) == 3

    # Check entity names
    entity_names = {entity["name"] for entity in data["data"]}
    assert "Universe" in entity_names
    assert "Earth" in entity_names
    assert "Human" in entity_names


async def test_get_entities_with_text_filter(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    """Test filtering entities by text search"""
    # Create entities with different names
    entities = [
        EntityInput(
            wikidata_id="Q76",
            entity_name="Barack Obama",
            entity_type="person",
            wikidata_info={"label": "Barack Obama"}
        ),
        EntityInput(
            wikidata_id="Q22686",
            entity_name="Donald Trump",
            entity_type="person",
            wikidata_info={"label": "Donald Trump"}
        ),
        EntityInput(
            wikidata_id="Q6279",
            entity_name="Joe Biden",
            entity_type="person",
            wikidata_info={"label": "Joe Biden"}
        )
    ]

    narrative_input = NarrativeInput(
        title="Presidents",
        description="US Presidents",
        entities=entities,
        claim_ids=[]
    )

    await api_key_client.post(
        "/api/narratives/",
        json=narrative_input.model_dump(mode="json"),
    )

    # Search for "Obama"
    response = await api_key_client.get("/api/entities/?text=Obama")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 1
    assert any("Obama" in entity["name"] for entity in data["data"])

    # Search for "Trump"
    response = await api_key_client.get("/api/entities/?text=Trump")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 1
    assert any("Trump" in entity["name"] for entity in data["data"])


async def test_get_entities_with_pagination(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    """Test pagination of entities"""
    # Create multiple entities
    entities = []
    for i in range(5):
        entities.append(
            EntityInput(
                wikidata_id=f"Q{100+i}",
                entity_name=f"Entity {i}",
                entity_type="test",
                wikidata_info={"label": f"Entity {i}"}
            )
        )

    narrative_input = NarrativeInput(
        title="Many Entities",
        description="Narrative with many entities for pagination test",
        entities=entities,
        claim_ids=[]
    )

    await api_key_client.post(
        "/api/narratives/",
        json=narrative_input.model_dump(mode="json"),
    )

    # Get first page (limit 2)
    response = await api_key_client.get("/api/entities/?limit=2&offset=0")
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]) == 2
    assert data["total"] == 5
    assert data["page"] == 1

    # Get second page
    response = await api_key_client.get("/api/entities/?limit=2&offset=2")
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]) == 2
    assert data["page"] == 2

    # Get third page
    response = await api_key_client.get("/api/entities/?limit=2&offset=4")
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]) == 1  # Only one entity left
    assert data["page"] == 3


async def test_get_specific_entity(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    """Test getting a specific entity by ID"""
    # Create a narrative with an entity
    entity_input = EntityInput(
        wikidata_id="Q42",
        entity_name="Douglas Adams",
        entity_type="author",
        wikidata_info={
            "label": "Douglas Adams",
            "description": "English author",
            "birth_date": "1952-03-11"
        }
    )

    narrative_input = NarrativeInput(
        title="Literature",
        description="About authors",
        entities=[entity_input],
        claim_ids=[]
    )

    narrative_response = await api_key_client.post(
        "/api/narratives/",
        json=narrative_input.model_dump(mode="json"),
    )
    assert narrative_response.status_code == 201

    # Get entity ID from narrative response
    entity_id = narrative_response.json()["data"]["entities"][0]["id"]

    # Get the specific entity
    response = await api_key_client.get(f"/api/entities/{entity_id}")

    assert response.status_code == 200
    entity_data = response.json()["data"]
    assert entity_data["id"] == entity_id
    assert entity_data["name"] == "Douglas Adams"
    assert entity_data["wikidata_id"] == "Q42"
    assert entity_data["metadata"]["entity_type"] == "author"


async def test_get_entity_not_found(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    """Test getting a non-existent entity"""
    fake_id = "00000000-0000-0000-0000-000000000001"
    response = await api_key_client.get(f"/api/entities/{fake_id}")

    assert response.status_code == 404


async def test_get_narratives_by_entity(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    """Test getting all narratives that reference a specific entity"""
    # Create an entity that will be shared
    shared_entity = EntityInput(
        wikidata_id="Q7942",
        entity_name="Climate Change",
        entity_type="concept",
        wikidata_info={"label": "Climate Change"}
    )

    # Create first narrative with the entity
    narrative1 = NarrativeInput(
        title="Climate Science",
        description="About climate change science",
        entities=[shared_entity],
        claim_ids=[]
    )

    response1 = await api_key_client.post(
        "/api/narratives/",
        json=narrative1.model_dump(mode="json"),
    )
    assert response1.status_code == 201
    narrative1_id = response1.json()["data"]["id"]

    # Get entity ID from first narrative
    entity_id = response1.json()["data"]["entities"][0]["id"]

    # Create second narrative with the same entity (using existing entity ID)
    narrative2 = NarrativeInput(
        title="Climate Policy",
        description="About climate change policies",
        entities=[shared_entity],  # Will reuse existing entity
        claim_ids=[]
    )

    response2 = await api_key_client.post(
        "/api/narratives/",
        json=narrative2.model_dump(mode="json"),
    )
    assert response2.status_code == 201
    narrative2_id = response2.json()["data"]["id"]

    # Get narratives for this entity
    response = await api_key_client.get(f"/api/entities/{entity_id}/narratives")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    assert len(data["data"]) == 2

    # Check both narratives are returned
    narrative_ids = {n["id"] for n in data["data"]}
    assert narrative1_id in narrative_ids
    assert narrative2_id in narrative_ids

    narrative_titles = {n["title"] for n in data["data"]}
    assert "Climate Science" in narrative_titles
    assert "Climate Policy" in narrative_titles


async def test_get_narratives_by_entity_with_pagination(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    """Test pagination when getting narratives for an entity"""
    shared_entity = EntityInput(
        wikidata_id="Q100",
        entity_name="Shared Entity",
        entity_type="test",
        wikidata_info={"label": "Shared"}
    )

    # Create multiple narratives with the same entity
    entity_id = None
    for i in range(3):
        narrative_input = NarrativeInput(
            title=f"Narrative {i}",
            description=f"Description {i}",
            entities=[shared_entity],
            claim_ids=[]
        )

        response = await api_key_client.post(
            "/api/narratives/",
            json=narrative_input.model_dump(mode="json"),
        )
        assert response.status_code == 201

        if entity_id is None:
            entity_id = response.json()["data"]["entities"][0]["id"]

    # Get first page
    response = await api_key_client.get(f"/api/entities/{entity_id}/narratives?limit=2&offset=0")
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]) == 2
    assert data["total"] == 3
    assert data["page"] == 1

    # Get second page
    response = await api_key_client.get(f"/api/entities/{entity_id}/narratives?limit=2&offset=2")
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]) == 1
    assert data["page"] == 2


async def test_get_narratives_by_entity_empty(
    api_key_client: AsyncTestClient[Litestar]
) -> None:
    """Test getting narratives for an entity with no narratives"""
    # Create an entity without any narratives referencing it later
    entity_input = EntityInput(
        wikidata_id="Q999",
        entity_name="Lonely Entity",
        entity_type="test",
        wikidata_info={"label": "Lonely"}
    )

    narrative_input = NarrativeInput(
        title="Temporary",
        description="Will be deleted",
        entities=[entity_input],
        claim_ids=[]
    )

    response = await api_key_client.post(
        "/api/narratives/",
        json=narrative_input.model_dump(mode="json"),
    )
    entity_id = response.json()["data"]["entities"][0]["id"]
    narrative_id = response.json()["data"]["id"]

    # Delete the narrative (entity should still exist)
    await api_key_client.delete(f"/api/narratives/{narrative_id}")

    # Now the entity exists but has no narratives
    response = await api_key_client.get(f"/api/entities/{entity_id}/narratives")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 0
    assert data["data"] == []
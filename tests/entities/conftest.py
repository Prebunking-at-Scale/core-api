from typing import Any

from litestar import Litestar
from litestar.testing import AsyncTestClient
from pytest import fixture

from core.entities.models import EntityInput
from core.models import Entity


@fixture
def tables_to_truncate() -> list[str]:
    return [
        "entities",
        "narrative_entities",
        "claim_entities",
        "narratives",
        "video_claims",
        "claim_narratives",
    ]


async def create_entity(
    api_key_client: AsyncTestClient[Litestar],
    narrative_id: str,
    wikidata_id: str = "Q42",
    entity_name: str = "Test Entity",
    entity_type: str = "person",
    **kwargs: dict[str, Any]
) -> Entity:
    """Helper to create an entity by adding it to a narrative"""
    from core.narratives.models import NarrativePatchInput

    entity_input = EntityInput(
        wikidata_id=wikidata_id,
        entity_name=entity_name,
        entity_type=entity_type,
        wikidata_info=kwargs.get("wikidata_info", {"label": entity_name}),
    )

    # Add entity to narrative via patch
    patch_data = NarrativePatchInput(entities=[entity_input])
    response = await api_key_client.patch(
        f"/api/narratives/{narrative_id}",
        json=patch_data.model_dump(mode="json", exclude_unset=True),
    )
    assert response.status_code == 200

    # Find and return the entity
    entities = response.json()["data"]["entities"]
    for entity in entities:
        if entity["wikidata_id"] == wikidata_id:
            return Entity(**entity)

    raise ValueError(f"Entity with wikidata_id {wikidata_id} not found")
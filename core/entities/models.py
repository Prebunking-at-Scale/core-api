from typing import Any

from pydantic import BaseModel

from core.models import Entity


class EntityInput(BaseModel):
    """Model for incoming entity data from PATCH requests"""
    wikidata_id: str
    entity_name: str
    entity_type: str | None = None
    wikidata_info: dict[str, Any] = {}


class EntityUpdate(BaseModel):
    """Model for updating entities in claims/narratives"""
    entities: list[EntityInput] = []


class EnrichedEntity(Entity):
    """Entity model enriched with usage statistics"""
    total_claims: int = 0
    total_videos: int = 0
    linked_narratives: int = 0
    platforms: list[str] = []
    languages: list[str] = []

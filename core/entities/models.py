from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class EntityInput(BaseModel):
    """Model for incoming entity data from PATCH requests"""
    wikidata_id: str
    entity_name: str 
    entity_type: str | None = None 
    wikidata_info: dict[str, Any] = {}


class EntityUpdate(BaseModel):
    """Model for updating entities in claims/narratives"""
    entities: list[EntityInput] = []


class EnrichedEntity(BaseModel):
    """Entity model enriched with statistics"""
    id: UUID
    wikidata_id: str
    name: str
    metadata: dict[str, Any] = {}
    created_at: datetime | None = None
    updated_at: datetime | None = None
    total_claims: int = 0
    total_videos: int = 0
    linked_narratives: int = 0
    platforms: list[str] = []
    languages: list[str] = []
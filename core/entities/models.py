from typing import Any

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
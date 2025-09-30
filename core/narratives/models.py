from typing import Any
from uuid import UUID

from pydantic import BaseModel

from core.entities.models import EntityInput


class NarrativeInput(BaseModel):
    title: str
    description: str
    claim_ids: list[UUID] = []
    topic_ids: list[UUID] = []
    entities: list[EntityInput] | None = None
    metadata: dict[str, Any] = {}


class NarrativePatchInput(BaseModel):
    title: str | None = None
    description: str | None = None
    claim_ids: list[UUID] | None = None
    topic_ids: list[UUID] | None = None
    entities: list[EntityInput] | None = None
    metadata: dict[str, Any] | None = None

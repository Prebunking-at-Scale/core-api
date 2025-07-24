from typing import Any
from uuid import UUID

from pydantic import BaseModel


class NarrativeInput(BaseModel):
    title: str
    description: str
    claim_ids: list[UUID] = []
    topic_ids: list[UUID] = []
    metadata: dict[str, Any] = {}

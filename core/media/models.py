from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Channel(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    organisation_id: UUID
    platform: Literal["youtube", "tiktok", "instagram"]
    name: str

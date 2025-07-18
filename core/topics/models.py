from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from pydantic import BaseModel, Field


class Topic(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    topic: str
    metadata: dict[str, Any] = {}
    created_at: datetime | None = None
    updated_at: datetime | None = None


class TopicWithStats(Topic):
    narrative_count: int = 0
    claim_count: int = 0


class TopicDTO(PydanticDTO[Topic]):
    config = DTOConfig(
        exclude={
            "id",
            "created_at",
            "updated_at",
        },
    )

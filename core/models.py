from datetime import datetime
import uuid

from pydantic import BaseModel, Field


class IDAuditModel(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    created_at: datetime | None = None
    updated_at: datetime | None = None

from datetime import datetime
from typing import Literal

from litestar.exceptions import HTTPException
from pydantic import BaseModel

SavedSelectionKind = Literal["keyword", "entity_id", "channel"]

NAME_MAX_LENGTH = 60


class SavedSelection(BaseModel):
    """A named set of filter values. Defaults are the organisation's, built from its
    feeds (id "default-…"); the rest belong to the person who saved them."""

    id: str
    kind: SavedSelectionKind
    name: str
    values: list[str]
    is_default: bool = False
    created_at: datetime | None = None


class SavedSelectionInput(BaseModel):
    kind: str
    name: str
    values: list[str]


class SavedSelectionError(HTTPException):
    """422 with one of: invalid_kind, name_required, name_too_long, values_required,
    name_taken."""

    status_code = 422

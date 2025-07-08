from typing import Generic, TypeVar
from uuid import UUID

from pydantic.dataclasses import dataclass

T = TypeVar("T")


@dataclass
class JSON(Generic[T]):
    """Base response structure for all API responses."""

    data: T


@dataclass
class CursorJSON(JSON[T]):
    """Base response structure for paginated API responses."""

    cursor: UUID | None = None


@dataclass
class Error:
    status: int
    message: str
    detail: str


@dataclass
class JSONError:
    """Base response structure for API errors."""

    error: Error

from typing import Generic, TypeVar

from pydantic.dataclasses import dataclass


T = TypeVar("T")


@dataclass
class JSON(Generic[T]):
    """Base response structure for all API responses."""

    data: T


@dataclass
class PaginatedJSON(JSON[T]):
    """Base response structure for paginated API responses."""

    next_cursor: str | None = None
    previous_cursor: str | None = None


@dataclass
class Error:
    status: int
    message: str
    detail: str


@dataclass
class JSONError:
    """Base response structure for API errors."""

    error: Error

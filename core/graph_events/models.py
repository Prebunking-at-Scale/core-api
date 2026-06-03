"""Graph-event types and payload shapes.

A graph event describes a Postgres-side mutation that needs to be reflected
in the Neo4j entity graph (lives in the narratives service). Events are
written to the `graph_events` outbox in the same transaction as the
mutation, then dispatched asynchronously.

Payload models live here so the dispatcher can revalidate them on read
(catching corrupt rows early) and so narratives gets the same contract
when it parses incoming events at `POST /graph/events`.
"""

from datetime import datetime
from enum import Enum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, Field


class GraphEventType(str, Enum):
    NARRATIVE_DELETED = "narrative.deleted"
    NARRATIVE_MERGED = "narrative.merged"


class NarrativeDeletedPayload(BaseModel):
    """The narrative was hard-deleted from Postgres. Neo4j must remove
    this id from every `narrative_ids` array and drop nodes/edges that
    end up empty."""

    narrative_id: UUID


class NarrativeMergedPayload(BaseModel):
    """The source narrative was merged into target. Its claims now belong
    to target, the source row is gone from Postgres. Neo4j must rewrite
    every occurrence of source_id to target_id, dedupe, and not leave
    orphans."""

    source_id: UUID
    target_id: UUID


# Validated union: which payload shape goes with which event_type.
EventPayload = Annotated[
    NarrativeDeletedPayload | NarrativeMergedPayload,
    Field(discriminator=None),
]


PAYLOAD_MODELS: dict[GraphEventType, type[BaseModel]] = {
    GraphEventType.NARRATIVE_DELETED: NarrativeDeletedPayload,
    GraphEventType.NARRATIVE_MERGED: NarrativeMergedPayload,
}


def validate_payload(event_type: GraphEventType, raw: dict) -> BaseModel:
    """Pick the right payload model for an event_type and validate `raw`
    against it. Raises pydantic.ValidationError on mismatch."""
    model = PAYLOAD_MODELS[event_type]
    return model.model_validate(raw)


class GraphEvent(BaseModel):
    """One row of the graph_events outbox, as the dispatcher sees it."""

    id: UUID
    event_type: GraphEventType
    payload: dict
    created_at: datetime
    processed_at: datetime | None = None
    attempts: int = 0
    last_error: str | None = None
    next_retry_at: datetime | None = None

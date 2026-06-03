"""Outbox writer + dispatcher state machine for graph events.

The two halves use the same module on purpose: keeping `publish` next to
the SQL the dispatcher reads makes it harder to drift the schema across
call sites.

`publish` is meant to be called *inside the caller's transaction*. It
takes the caller's cursor rather than opening its own, so the INSERT
either commits with the mutation that triggered it or rolls back with it.
That coupling is the entire atomicity guarantee the outbox pattern buys
you — don't refactor it into a method that opens a fresh connection.
"""

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.graph_events.models import (
    GraphEvent,
    GraphEventType,
    validate_payload,
)


async def publish(
    session: psycopg.AsyncCursor[DictRow],
    event_type: GraphEventType,
    payload: dict[str, Any],
) -> UUID:
    """Append an event to the outbox using the caller's cursor.

    Returns the event id so the caller can correlate logs / tests. The
    INSERT lives in the caller's transaction — commit/rollback of the
    surrounding unit of work decides whether the event survives.

    Payload is validated against the event_type's schema before the
    INSERT so we never persist garbage that the dispatcher would have
    to re-validate and fail on later.
    """
    validate_payload(event_type, payload)
    await session.execute(
        """
        INSERT INTO graph_events (event_type, payload)
        VALUES (%(event_type)s, %(payload)s)
        RETURNING id
        """,
        {"event_type": event_type.value, "payload": Jsonb(payload)},
    )
    row = await session.fetchone()
    assert row is not None  # RETURNING always yields a row on success
    return row["id"]


async def claim_batch(
    session: psycopg.AsyncCursor[DictRow], limit: int = 50
) -> list[GraphEvent]:
    """Lock and return up to `limit` pending events for processing.

    Uses `FOR UPDATE SKIP LOCKED` so multiple dispatcher replicas can run
    in parallel without colliding — each claims a disjoint slice. Skips
    events that have a `next_retry_at` set in the future.
    """
    await session.execute(
        """
        SELECT id, event_type, payload, created_at, processed_at,
               attempts, last_error, next_retry_at
        FROM graph_events
        WHERE processed_at IS NULL
          AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
        ORDER BY created_at ASC
        LIMIT %(limit)s
        FOR UPDATE SKIP LOCKED
        """,
        {"limit": limit},
    )
    rows = await session.fetchall()
    return [GraphEvent.model_validate(dict(r)) for r in rows]


async def mark_processed(
    session: psycopg.AsyncCursor[DictRow], event_id: UUID
) -> None:
    await session.execute(
        """
        UPDATE graph_events
        SET processed_at = CURRENT_TIMESTAMP,
            last_error   = NULL,
            next_retry_at = NULL
        WHERE id = %(id)s
        """,
        {"id": event_id},
    )


async def mark_failed(
    session: psycopg.AsyncCursor[DictRow],
    event_id: UUID,
    error: str,
    backoff: timedelta,
) -> None:
    """Increment attempts and schedule next retry with the given backoff."""
    await session.execute(
        """
        UPDATE graph_events
        SET attempts      = attempts + 1,
            last_error    = %(err)s,
            next_retry_at = CURRENT_TIMESTAMP + %(backoff)s
        WHERE id = %(id)s
        """,
        {
            "id": event_id,
            "err": error[:2000],  # cap so a verbose traceback doesn't bloat the row
            "backoff": backoff,
        },
    )


def compute_backoff(attempts: int, base_seconds: float, max_seconds: float) -> timedelta:
    """Exponential backoff capped at `max_seconds`.

    attempt 0 → base, 1 → 2*base, 2 → 4*base, …, capped. `attempts` is the
    count after the failure (i.e. the value already incremented on the row).
    """
    delay = min(base_seconds * (2 ** max(attempts - 1, 0)), max_seconds)
    return timedelta(seconds=delay)


async def pending_count(
    session: psycopg.AsyncCursor[DictRow],
) -> int:
    """Observability hook — how many events are waiting to be dispatched."""
    await session.execute(
        "SELECT COUNT(*) AS c FROM graph_events WHERE processed_at IS NULL"
    )
    row = await session.fetchone()
    return row["c"] if row else 0


# ── String helpers exposed for the dispatcher's logging ──────────────
def event_summary(event: GraphEvent) -> str:
    """One-line, log-friendly summary of an event."""
    return f"{event.event_type.value}[{event.id}] payload={event.payload}"


def is_in_terminal_state(event: GraphEvent, max_attempts: int) -> bool:
    """True if the event has exhausted retries — the dispatcher escalates
    these to a dead-letter log line so ops can react. We don't auto-drop:
    the row stays in the table with processed_at=NULL, attempts>=max,
    visible to the drift detector / runbook."""
    return event.attempts >= max_attempts


def now() -> datetime:
    """Indirection so tests can monkeypatch time."""
    return datetime.utcnow()

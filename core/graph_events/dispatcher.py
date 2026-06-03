"""Outbox dispatcher — moves pending graph_events into narratives.

Runs as its own process (see Procfile entry `graph-events-dispatcher`).
Polls the outbox in a loop, claims a batch with FOR UPDATE SKIP LOCKED
so we can run multiple replicas if throughput ever calls for it, POSTs
each event to `POST /graph/events` on narratives, and marks the row
processed or scheduled for retry. The actual Neo4j Cypher runs on the
narratives side via a Celery task — the dispatcher only needs a 202
back to consider its job done.

A note on retries vs giving up: we keep retrying as long as the receiver
returns something that *might* be transient (5xx, timeout, network). On
a 4xx we still retry up to `GRAPH_EVENTS_MAX_ATTEMPTS`, but each backoff
doubles, so the event spends time waiting for human intervention rather
than spinning. After max attempts we stop touching the row; the drift
detector surfaces it and ops decides whether to fix the payload, replay,
or drop.
"""

import asyncio
import logging
import signal

import httpx
import structlog
from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from core import config
from core.graph_events import service
from core.graph_events.models import GraphEvent
from core.narratives.api import NarrativesApiClient

logger = structlog.get_logger(__name__)


_shutdown = asyncio.Event()


def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    """Clean stop on SIGTERM / SIGINT so an in-flight batch finishes
    before the process exits — avoids leaving claimed rows locked behind
    a hard kill."""

    def _request_shutdown(_signum: int) -> None:
        logger.info("graph-events.dispatcher.shutdown_requested")
        _shutdown.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_shutdown, sig)


async def dispatch_one(
    client: NarrativesApiClient, event: GraphEvent
) -> tuple[bool, str | None]:
    """Send a single event. Returns (success, error_message).

    Treats network/timeout/5xx as failures eligible for backoff retry.
    A 4xx is also retried (in case it was caused by transient state on
    the receiver) but the operator will see it stuck after a handful of
    attempts; that's by design — 4xx usually means schema mismatch and
    deserves a human."""
    try:
        response = await client.post_graph_event(
            event_id=str(event.id),
            event_type=event.event_type.value,
            payload=event.payload,
        )
    except (httpx.TimeoutException, httpx.NetworkError) as exc:
        return False, f"transport: {type(exc).__name__}: {exc}"

    if 200 <= response.status_code < 300:
        return True, None
    try:
        detail = response.json().get("detail")
    except Exception:
        detail = None
    return False, f"http {response.status_code}: {detail or response.text[:300]}"


async def process_batch(pool: AsyncConnectionPool[AsyncConnection[DictRow]]) -> int:
    """Claim, dispatch and mark a single batch.

    Returns the number of events actually attempted, so the caller can
    decide whether to poll again immediately (full batch → more work
    waiting) or sleep (empty batch → idle).
    """
    if not NarrativesApiClient.is_configured():
        # Not configured → don't spin retries. Log once and bail; let the
        # process loop sleep until config catches up.
        logger.warning("graph-events.dispatcher.narratives_unconfigured")
        return 0

    client = NarrativesApiClient()

    # Each batch lives in its own transaction. The SELECT FOR UPDATE
    # holds row-level locks until COMMIT, so the dispatch HTTP calls
    # have to happen *inside* the transaction. That keeps slow HTTP
    # work coupled to a held connection but guarantees a row can't be
    # double-claimed by another replica mid-flight.
    async with pool.connection() as conn:
        async with conn.cursor() as session:
            events = await service.claim_batch(
                session, limit=config.GRAPH_EVENTS_BATCH_SIZE
            )
            if not events:
                await conn.commit()
                return 0

            for event in events:
                success, error = await dispatch_one(client, event)
                if success:
                    await service.mark_processed(session, event.id)
                    logger.info(
                        "graph-events.dispatcher.dispatched",
                        event_id=str(event.id),
                        event_type=event.event_type.value,
                    )
                else:
                    new_attempts = event.attempts + 1
                    backoff = service.compute_backoff(
                        attempts=new_attempts,
                        base_seconds=config.GRAPH_EVENTS_BACKOFF_BASE_SEC,
                        max_seconds=config.GRAPH_EVENTS_BACKOFF_MAX_SEC,
                    )
                    await service.mark_failed(session, event.id, error or "", backoff)
                    log_method = (
                        logger.error
                        if new_attempts >= config.GRAPH_EVENTS_MAX_ATTEMPTS
                        else logger.warning
                    )
                    log_method(
                        "graph-events.dispatcher.failed",
                        event_id=str(event.id),
                        event_type=event.event_type.value,
                        attempts=new_attempts,
                        backoff_sec=backoff.total_seconds(),
                        error=error,
                    )
            await conn.commit()
            return len(events)


async def run_forever(pool: AsyncConnectionPool[AsyncConnection[DictRow]]) -> None:
    logger.info("graph-events.dispatcher.start")
    while not _shutdown.is_set():
        try:
            processed = await process_batch(pool)
        except Exception:
            # Don't let a bug here take the process down — log and back
            # off. The row-level locks are released on the rollback that
            # happens when the connection context exits.
            logger.exception("graph-events.dispatcher.cycle_error")
            processed = 0
        if processed < config.GRAPH_EVENTS_BATCH_SIZE:
            # Either the batch wasn't full or nothing was claimed — idle.
            try:
                await asyncio.wait_for(
                    _shutdown.wait(),
                    timeout=config.GRAPH_EVENTS_POLL_INTERVAL_SEC,
                )
            except asyncio.TimeoutError:
                pass
    logger.info("graph-events.dispatcher.stopped")


async def _main() -> None:
    postgres_url = (
        f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}"
        f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    )
    pool: AsyncConnectionPool[AsyncConnection[DictRow]] = AsyncConnectionPool(
        postgres_url,
        open=False,
        max_size=4,
        connection_class=AsyncConnection[DictRow],
        kwargs={"row_factory": dict_row},
    )
    await pool.open()
    _install_signal_handlers(asyncio.get_running_loop())
    try:
        await run_forever(pool)
    finally:
        await pool.close()


def main() -> None:
    """Entry point for `python -m core.graph_events.dispatcher`."""
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_main())


if __name__ == "__main__":
    main()

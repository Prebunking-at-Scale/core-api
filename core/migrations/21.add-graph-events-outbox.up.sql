BEGIN;

-- Outbox for Postgres→Neo4j synchronisation events.
--
-- Mutations that affect entity-graph state in narratives (delete narrative,
-- merge narratives, …) write a row here in the same transaction as the
-- Postgres mutation. A dispatcher process polls pending rows and POSTs them
-- to the narratives /graph/events endpoint, which queues the actual Cypher
-- work on the narratives Celery worker. This decouples Postgres
-- mutations from Neo4j availability without losing atomicity guarantees.

CREATE TABLE IF NOT EXISTS graph_events (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type    text NOT NULL,             -- 'narrative.deleted' | 'narrative.merged' | …
    payload       jsonb NOT NULL,             -- event-specific body, validated on dispatch
    created_at    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at  timestamp,                  -- NULL = pending
    attempts      integer NOT NULL DEFAULT 0,
    last_error    text,
    next_retry_at timestamp                   -- back-off target; NULL = retry immediately
);

-- Partial index for the dispatcher's hot path: it only ever reads pending rows.
CREATE INDEX IF NOT EXISTS idx_graph_events_pending
    ON graph_events (created_at)
    WHERE processed_at IS NULL;

COMMIT;

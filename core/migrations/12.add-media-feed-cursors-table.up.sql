BEGIN;

CREATE TABLE IF NOT EXISTS media_feed_cursors (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    target TEXT NOT NULL,
    platform TEXT NOT NULL,
    cursor JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX media_feed_cursors_unique
ON media_feed_cursors (target, platform);

COMMIT;
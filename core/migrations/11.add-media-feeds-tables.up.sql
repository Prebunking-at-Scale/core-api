BEGIN;

CREATE TABLE IF NOT EXISTS keyword_feeds (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organisation_id uuid NOT NULL REFERENCES organisations (id) ON DELETE CASCADE,
    created_by_user_id uuid REFERENCES users (id) ON DELETE SET NULL,
    topic TEXT NOT NULL,
    keywords TEXT[] NOT NULL,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS channel_feeds (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organisation_id uuid NOT NULL REFERENCES organisations (id) ON DELETE CASCADE,
    created_by_user_id uuid REFERENCES users (id) ON DELETE SET NULL,
    channel TEXT NOT NULL,
    platform TEXT NOT NULL,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_channel_feeds_unique
ON channel_feeds (organisation_id, channel, platform)
WHERE is_archived = FALSE;

CREATE UNIQUE INDEX idx_keyword_feeds_unique
ON keyword_feeds (organisation_id, topic)
WHERE is_archived = FALSE;

COMMIT;
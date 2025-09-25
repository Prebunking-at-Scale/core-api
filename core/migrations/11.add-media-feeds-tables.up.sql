BEGIN;

CREATE TABLE IF NOT EXISTS media_feeds (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organisation_id uuid NOT NULL REFERENCES organisations (id) ON DELETE CASCADE,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create keyword_feeds table inheriting from media_feeds
CREATE TABLE IF NOT EXISTS keyword_feeds (
    topic TEXT NOT NULL,
    keywords TEXT[] NOT NULL
) INHERITS (media_feeds);

-- Create channel_feeds table inheriting from media_feeds
CREATE TABLE IF NOT EXISTS channel_feeds (
    channel TEXT NOT NULL,
    platform TEXT NOT NULL
) INHERITS (media_feeds);

CREATE UNIQUE INDEX idx_channel_feeds_unique
ON channel_feeds (organisation_id, channel, platform)
WHERE is_archived = FALSE;

CREATE UNIQUE INDEX idx_keyword_feeds_unique
ON keyword_feeds (organisation_id, topic)
WHERE is_archived = FALSE;

COMMIT;
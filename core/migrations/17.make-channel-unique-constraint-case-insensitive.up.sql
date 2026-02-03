BEGIN;

-- Drop the existing case-sensitive unique index
DROP INDEX IF EXISTS idx_channel_feeds_unique;

-- Create a new case-insensitive unique index using LOWER()
CREATE UNIQUE INDEX idx_channel_feeds_unique
ON channel_feeds (organisation_id, LOWER(channel), platform)
WHERE is_archived = FALSE;

COMMIT;

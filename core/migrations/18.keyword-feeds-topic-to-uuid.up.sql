BEGIN;

-- Convert old topic name values to their corresponding UUIDs
UPDATE keyword_feeds
SET topic = CASE topic
    WHEN 'public health' THEN 'bb52f622-b9ee-4d5b-9b70-5fd05046528b'
    WHEN 'eu'            THEN 'ff1bedb9-43e4-49e6-9c3f-27babfb7bfa1'
    WHEN 'climate'       THEN 'db3d996b-e691-4ce5-8c46-e35a82a9b28c'
    WHEN 'migration'     THEN '3cd4a9cd-5906-4b0b-9167-57ff22c2345a'
    WHEN 'conflict'      THEN '0d7aaf8d-5b7e-4c0c-b03a-28457e27ac7d'
END
WHERE topic IN ('public health', 'eu', 'climate', 'migration', 'conflict');

-- Drop the old unique index
DROP INDEX IF EXISTS idx_keyword_feeds_unique;

-- Rename column and change type to UUID
ALTER TABLE keyword_feeds RENAME COLUMN topic TO topic_id;
ALTER TABLE keyword_feeds ALTER COLUMN topic_id TYPE uuid USING topic_id::uuid;

-- Add foreign key constraint
ALTER TABLE keyword_feeds ADD CONSTRAINT fk_keyword_feeds_topic
    FOREIGN KEY (topic_id) REFERENCES topics(id);

-- Recreate unique index with new column name
CREATE UNIQUE INDEX idx_keyword_feeds_unique
ON keyword_feeds (organisation_id, topic_id)
WHERE is_archived = FALSE;

COMMIT;

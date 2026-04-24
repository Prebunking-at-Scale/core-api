BEGIN;

ALTER TABLE narratives ADD COLUMN narrative_context TEXT;

COMMIT;

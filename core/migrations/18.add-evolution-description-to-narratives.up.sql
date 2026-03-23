BEGIN;

ALTER TABLE narratives ADD COLUMN evolution_description TEXT;

COMMIT;

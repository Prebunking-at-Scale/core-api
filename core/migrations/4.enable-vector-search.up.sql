BEGIN;

    CREATE EXTENSION IF NOT EXISTS vector;

    ALTER TABLE videos ADD COLUMN embedding vector(384);

    ALTER TABLE transcript_sentences ADD COLUMN embedding vector(384);

    ALTER TABLE video_claims ADD COLUMN embedding vector(384);

COMMIT;
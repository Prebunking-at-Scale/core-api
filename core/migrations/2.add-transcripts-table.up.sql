BEGIN;

CREATE TABLE transcript_sentences (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    video_id uuid REFERENCES videos (id) ON DELETE CASCADE,
    source text NOT NULL,
    text text NOT NULL,
    start_time_s float NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMIT;
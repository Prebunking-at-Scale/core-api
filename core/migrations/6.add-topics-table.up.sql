BEGIN;

CREATE TABLE IF NOT EXISTS topics (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    topic text NOT NULL UNIQUE,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS narrative_topics (
    narrative_id uuid REFERENCES narratives(id) ON DELETE CASCADE,
    topic_id uuid REFERENCES topics(id) ON DELETE CASCADE,
    PRIMARY KEY (narrative_id, topic_id)
);

COMMIT;
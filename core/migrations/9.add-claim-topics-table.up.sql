BEGIN;

-- Create the many-to-many relationship table between claims and topics
CREATE TABLE IF NOT EXISTS claim_topics (
    claim_id UUID NOT NULL REFERENCES video_claims(id) ON DELETE CASCADE,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (claim_id, topic_id)
);

-- Create indexes for faster lookups
CREATE INDEX idx_claim_topics_claim_id ON claim_topics(claim_id);
CREATE INDEX idx_claim_topics_topic_id ON claim_topics(topic_id);

COMMIT;
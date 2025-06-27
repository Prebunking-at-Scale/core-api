BEGIN;

CREATE TABLE IF NOT EXISTS videos (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    platform TEXT NOT NULL,
    source_url TEXT NOT NULL,
    destination_path TEXT NOT NULL,
    uploaded_at TIMESTAMP,
    views BIGINT,
    likes BIGINT,
    comments BIGINT,
    channel TEXT,
    channel_followers BIGINT,
    scrape_topic TEXT,
    scrape_keyword TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMIT;
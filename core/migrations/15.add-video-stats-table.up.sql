BEGIN;

-- Create new video_stats table for historical tracking
CREATE TABLE IF NOT EXISTS video_stats (
    video_id uuid NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    views BIGINT,
    likes BIGINT,
    comments BIGINT,
    channel_followers BIGINT,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_video_stats_video_id ON video_stats(video_id);
CREATE INDEX idx_video_stats_recorded_at ON video_stats(recorded_at);
CREATE INDEX idx_video_stats_video_id_recorded_at on video_stats(video_id, recorded_at);

-- Migrate existing data from videos table to video_stats for historical record
INSERT INTO video_stats (video_id, views, likes, comments, channel_followers, recorded_at)
SELECT id, views, likes, comments, channel_followers, COALESCE(created_at, CURRENT_TIMESTAMP)
FROM videos;

-- Keep columns in videos table for fast query access (do not drop them)
-- The videos table columns store the latest values for efficient reads
-- The video_stats table stores historical values for tracking over time

COMMIT;

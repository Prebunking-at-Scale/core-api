BEGIN;

-- Create new video_stats table
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
CREATE INDEX idx_video_statS_video_id_recorded_at on video_stats(video_id, recorded_at);

-- Migrate existing data from videos table
INSERT INTO video_stats (video_id, views, likes, comments, channel_followers, recorded_at)
SELECT id, views, likes, comments, channel_followers, COALESCE(created_at, CURRENT_TIMESTAMP)
FROM videos;

-- Remove columns from videos table
ALTER TABLE videos
    DROP COLUMN views,
    DROP COLUMN likes,
    DROP COLUMN comments,
    DROP COLUMN channel_followers;

COMMIT;

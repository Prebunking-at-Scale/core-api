-- /api/videos, /api/claims and /api/narratives all filter on
-- videos.uploaded_at now, over 105k rows and often, so it wants an index.
-- No transaction wrapper, same as the other index migration (14).

CREATE INDEX IF NOT EXISTS videos_uploaded_at_idx
ON videos (uploaded_at);

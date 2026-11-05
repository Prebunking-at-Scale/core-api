-- /api/videos, /api/claims and /api/narratives all filter on
-- videos.uploaded_at now, often, and production has over a million videos
-- (September 2026), so it wants an index. No transaction wrapper, same as the
-- other index migration (14). A plain CREATE INDEX blocks writes to videos while
-- it builds: in production, build it first with
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS videos_uploaded_at_idx ON videos (uploaded_at);
-- and this migration finds it there.

CREATE INDEX IF NOT EXISTS videos_uploaded_at_idx
ON videos (uploaded_at);

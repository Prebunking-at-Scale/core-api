-- Indexes for the search (/api/search), one per filter or sort that needs it.
--
-- No transaction wrapper, like the other index migrations (14, 23). In production
-- these tables are large (4M claims, 1.1M videos in September 2026) and a plain
-- CREATE INDEX blocks writes while it builds, so build them beforehand with
-- scripts/search-indexes-concurrently.sql; this migration then finds them all and
-- does nothing. On new, local and test databases it builds them here.

-- Keywords: claim text, video titles, narrative titles (normalize_text: migration 24)
CREATE INDEX IF NOT EXISTS video_claims_claim_trgm_idx
ON video_claims USING gin (normalize_text(claim) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS videos_title_trgm_idx
ON videos USING gin (normalize_text(title) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS narratives_title_trgm_idx
ON narratives USING gin (normalize_text(title) gin_trgm_ops);

-- Platform and channel filters (exact, channel ignoring case)
CREATE INDEX IF NOT EXISTS videos_platform_channel_idx
ON videos (platform, lower(channel));

-- The channel list's search box
CREATE INDEX IF NOT EXISTS videos_channel_trgm_idx
ON videos USING gin (lower(channel) gin_trgm_ops);

-- Claims and videos are listed newest upload first, unknown dates last
CREATE INDEX IF NOT EXISTS videos_uploaded_at_desc_idx
ON videos (uploaded_at DESC NULLS LAST, id DESC);

-- Language filter, on the claim's language
CREATE INDEX IF NOT EXISTS video_claims_language_idx
ON video_claims ((metadata->>'language'));

-- Narratives: spread pattern filter, newest-first sort, and their own topics (the
-- primary key of narrative_topics starts with narrative_id)
CREATE INDEX IF NOT EXISTS narratives_spread_pattern_idx
ON narratives (spread_pattern);

CREATE INDEX IF NOT EXISTS narratives_created_at_idx
ON narratives (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS narrative_topics_topic_id_idx
ON narrative_topics (topic_id);

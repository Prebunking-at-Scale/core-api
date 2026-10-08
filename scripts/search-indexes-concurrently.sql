-- Builds the search indexes (migrations 23 and 25) in production without blocking
-- writes. Run it BEFORE deploying the core-api version that carries migrations 24
-- and 25:
--
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/search-indexes-concurrently.sql
--
-- then deploy. At startup, migrations 23 and 25 find every index already built and
-- do nothing, and migration 24 finds the extensions and the function in place.
--
-- CREATE INDEX CONCURRENTLY can't run inside a transaction: don't wrap this file in
-- BEGIN/COMMIT, and don't run it through core/migrate.py. The claim-text index can
-- take several minutes. If a statement fails it leaves an INVALID index: drop it with
-- DROP INDEX CONCURRENTLY <name>; and run the file again (everything is idempotent).

-- What the indexes need from migration 24 (same definitions)
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE OR REPLACE FUNCTION normalize_text(t text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT
    RETURN lower(public.unaccent('public.unaccent'::regdictionary, replace(t, '-', ' ')));

-- Migration 23
CREATE INDEX CONCURRENTLY IF NOT EXISTS videos_uploaded_at_idx
ON videos (uploaded_at);

-- Migration 25
CREATE INDEX CONCURRENTLY IF NOT EXISTS video_claims_claim_trgm_idx
ON video_claims USING gin (normalize_text(claim) gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS videos_title_trgm_idx
ON videos USING gin (normalize_text(title) gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS narratives_title_trgm_idx
ON narratives USING gin (normalize_text(title) gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS videos_platform_channel_idx
ON videos (platform, lower(channel));

CREATE INDEX CONCURRENTLY IF NOT EXISTS videos_channel_trgm_idx
ON videos USING gin (lower(channel) gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS videos_uploaded_at_desc_idx
ON videos (uploaded_at DESC NULLS LAST, id DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS video_claims_language_idx
ON video_claims ((metadata->>'language'));

CREATE INDEX CONCURRENTLY IF NOT EXISTS narratives_spread_pattern_idx
ON narratives (spread_pattern);

CREATE INDEX CONCURRENTLY IF NOT EXISTS narratives_created_at_idx
ON narratives (created_at DESC, id DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS narrative_topics_topic_id_idx
ON narrative_topics (topic_id);

-- Check: this should return no rows (an index left INVALID by a failed build)
SELECT indexrelid::regclass AS invalid_index
FROM pg_index
WHERE NOT indisvalid;

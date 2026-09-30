BEGIN;

-- Text matching for the search (/api/search): case, accents and hyphens ignored.
-- unaccent strips accents; pg_trgm lets an index answer "contains this text
-- anywhere", which a plain LIKE '%term%' can't use.
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- "Vacunación contra-la GRIPE" -> "vacunacion contra la gripe". Declared IMMUTABLE
-- so indexes can be built on it (unaccent alone isn't); the dictionary is named
-- with its schema so the result doesn't depend on the search_path. OR REPLACE:
-- scripts/search-indexes-concurrently.sql creates the same function in production
-- before this migration runs.
CREATE OR REPLACE FUNCTION normalize_text(t text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT
    RETURN lower(public.unaccent('public.unaccent'::regdictionary, replace(t, '-', ' ')));

-- Selections of filter values a person saves in the search, private to them.
-- The organisation's defaults aren't stored here: they are read from its feeds.
CREATE TABLE saved_selections (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organisation_id uuid NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
    user_id         uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    kind            text NOT NULL CHECK (kind IN ('keyword', 'entity_id', 'channel')),
    name            text NOT NULL CHECK (length(name) BETWEEN 1 AND 60),
    values          text[] NOT NULL CHECK (cardinality(values) > 0),
    created_at      timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- One name per person and kind, ignoring case; also serves listing them.
CREATE UNIQUE INDEX saved_selections_owner_name
    ON saved_selections (organisation_id, user_id, kind, lower(name));

COMMIT;

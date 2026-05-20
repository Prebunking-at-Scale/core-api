-- ============================================================
-- One-off cleanup: collapse historical concatenated
-- narrative_context values into the latest segment.
--
-- Background:
--   Until commit <fix-narrative-evolution-description>, the
--   backend appended every new narrative_context to the existing
--   one with a "\n\n--<iso-timestamp>--\n\n" separator. After the
--   fix, new updates replace instead. This script rewrites any
--   existing concatenated rows so they only contain the latest
--   segment, matching the new semantics.
--
-- Safety:
--   1. Creates a backup table OUTSIDE the transaction. The backup
--      survives a ROLLBACK and a second run (IF NOT EXISTS).
--   2. The UPDATE runs inside a transaction that you must commit
--      manually. Review the output before typing COMMIT.
--   3. Idempotent. Re-running after a COMMIT is a no-op because
--      the pattern no longer matches.
--
-- Usage:
--   psql -h <prod-host> -U <user> -d <db>
--   \i 2026-05-19-cleanup-narrative-context.sql
--   -- inspect the printed counts and sample
--   COMMIT;     -- to make changes permanent
--   ROLLBACK;   -- to abort (backup table still kept)
-- ============================================================

\set ON_ERROR_STOP on

-- Regex for the separator inserted by the old _merge_narrative_context.
-- isoformat() omits microseconds when they are zero, so .[0-9]+ is optional.
\set re '\n\n--[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?--\n\n'

-- 1) Backup. Lives outside the transaction so ROLLBACK does not drop it.
CREATE TABLE IF NOT EXISTS narratives_context_backup_20260519 AS
SELECT id, narrative_context, now() AS snapshotted_at
FROM narratives
WHERE narrative_context ~ :'re';

\echo
\echo '--- Backup table size (rows snapshotted) ---'
SELECT count(*) AS backed_up FROM narratives_context_backup_20260519;

-- 2) Open transaction for the destructive write.
BEGIN;

\echo
\echo '--- Rows about to be updated ---'
SELECT count(*) AS will_update
FROM narratives
WHERE narrative_context ~ :'re';

\echo
\echo '--- Sample (up to 5): before length, after length, # segments ---'
WITH sample AS (
    SELECT id,
           narrative_context AS before_value,
           regexp_split_to_array(narrative_context, :'re') AS arr
    FROM narratives
    WHERE narrative_context ~ :'re'
    LIMIT 5
)
SELECT id,
       length(before_value)                       AS len_before,
       length(arr[array_upper(arr, 1)])           AS len_after,
       array_length(arr, 1)                       AS segments
FROM sample;

-- 3) Apply: keep only the last segment.
UPDATE narratives
SET narrative_context = sub.arr[array_upper(sub.arr, 1)]
FROM (
    SELECT id, regexp_split_to_array(narrative_context, :'re') AS arr
    FROM narratives
    WHERE narrative_context ~ :'re'
) sub
WHERE narratives.id = sub.id;

\echo
\echo '--- Sanity: rows still matching the separator pattern (must be 0) ---'
SELECT count(*) AS remaining
FROM narratives
WHERE narrative_context ~ :'re';

\echo
\echo '------------------------------------------------------------'
\echo 'Review the numbers above.'
\echo '  COMMIT;    -> make changes permanent'
\echo '  ROLLBACK;  -> abort (backup table is kept)'
\echo '------------------------------------------------------------'

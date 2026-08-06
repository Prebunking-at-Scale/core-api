BEGIN;

-- The redesigned alert taxonomy (see docs/narrative-spread-pattern-redesign.md, D1).
--
-- The two axes give four corners of meaning:
--   viral         big AND still climbing   (composite >= 0.80 AND accel >= 0.80)
--   early_surge   small but climbing       (composite <= 0.40 AND accel >= 0.50)
--   consolidated  big and flat             (composite >= 0.50 AND accel <= 0.40)
--   trending      the broad middle         (composite >= 0.40 AND accel >= 0.40)
--   (no badge)    small and flat           -- alert_level IS NULL
--
-- `consolidated` is the genuinely new one: the old taxonomy had nowhere to put a large
-- narrative that had stopped growing, so `viral` absorbed it.
--
-- ADDITIVE ON PURPOSE. `alert` and `watch` are retired -- the classifier stops emitting
-- them the moment this ships -- but they stay in the type so that a consumer still
-- sending ?alert_level=alert keeps getting an empty result rather than a 400. Postgres
-- cannot drop a value from an enum; retiring them for real means recreating the type,
-- which is a follow-up migration to run once no consumer references them.
--
-- ALTER TYPE ... ADD VALUE is transaction-safe on PG 12+ so long as the new value is
-- not USED in the same transaction. Nothing below uses it.
ALTER TYPE narrative_alert_level ADD VALUE IF NOT EXISTS 'trending';
ALTER TYPE narrative_alert_level ADD VALUE IF NOT EXISTS 'consolidated';

COMMIT;

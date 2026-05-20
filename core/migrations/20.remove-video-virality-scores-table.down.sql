BEGIN;

-- Drop alert_level column from narratives table
ALTER TABLE narratives DROP COLUMN IF EXISTS alert_level;

-- Drop alert_level enum type
DROP TYPE IF EXISTS narrative_alert_level;

-- Drop narrative_analysis_indicators table and indexes
DROP INDEX IF EXISTS idx_narrative_analysis_indicators_narrative_id_indicator_type;
DROP INDEX IF EXISTS idx_narrative_analysis_indicators_calculated_at;
DROP INDEX IF EXISTS idx_narrative_analysis_indicators_indicator_type;
DROP INDEX IF EXISTS idx_narrative_analysis_indicators_narrative_id;
DROP TABLE IF EXISTS narrative_analysis_indicators;

-- Drop narrative_virality_scores table and indexes
DROP INDEX IF EXISTS idx_narrative_virality_scores_narrative_id_score_type;
DROP INDEX IF EXISTS idx_narrative_virality_scores_calculated_at;
DROP INDEX IF EXISTS idx_narrative_virality_scores_score_type;
DROP INDEX IF EXISTS idx_narrative_virality_scores_narrative_id;
DROP TABLE IF EXISTS narrative_virality_scores;

COMMIT;

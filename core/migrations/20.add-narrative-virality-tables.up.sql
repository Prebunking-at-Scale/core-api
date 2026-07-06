BEGIN;

-- Create alert_level enum type
CREATE TYPE narrative_alert_level AS ENUM ('none', 'viral', 'early_surge', 'alert', 'watch');

-- Add alert_level column to narratives table
ALTER TABLE narratives ADD COLUMN IF NOT EXISTS alert_level narrative_alert_level;

-- Create narrative_virality_scores table for tracking virality metrics
CREATE TABLE IF NOT EXISTS narrative_virality_scores (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    narrative_id uuid NOT NULL REFERENCES narratives(id) ON DELETE CASCADE,
    score_value FLOAT NOT NULL,
    score_type VARCHAR(50) NOT NULL CHECK (score_type IN ('engagement_score', 'reach_score', 'velocity_score')),
    metadata JSONB,
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX idx_narrative_virality_scores_narrative_id ON narrative_virality_scores(narrative_id);
CREATE INDEX idx_narrative_virality_scores_score_type ON narrative_virality_scores(score_type);
CREATE INDEX idx_narrative_virality_scores_calculated_at ON narrative_virality_scores(calculated_at);
CREATE INDEX idx_narrative_virality_scores_narrative_id_score_type ON narrative_virality_scores(narrative_id, score_type);

-- Create narrative_analysis_indicators table for tracking narrative analysis metrics
CREATE TABLE IF NOT EXISTS narrative_analysis_indicators (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    narrative_id uuid NOT NULL REFERENCES narratives(id) ON DELETE CASCADE,
    indicator_value FLOAT NOT NULL,
    indicator_type VARCHAR(50) NOT NULL CHECK (indicator_type IN ('composite_virality', 'acceleration_rate')),
    metadata JSONB,
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX idx_narrative_analysis_indicators_narrative_id ON narrative_analysis_indicators(narrative_id);
CREATE INDEX idx_narrative_analysis_indicators_indicator_type ON narrative_analysis_indicators(indicator_type);
CREATE INDEX idx_narrative_analysis_indicators_calculated_at ON narrative_analysis_indicators(calculated_at);
CREATE INDEX idx_narrative_analysis_indicators_narrative_id_indicator_type ON narrative_analysis_indicators(narrative_id, indicator_type);

COMMIT;

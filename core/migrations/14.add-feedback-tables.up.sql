BEGIN;

-- Table for narrative feedback
CREATE TABLE IF NOT EXISTS narrative_feedback (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    narrative_id uuid NOT NULL REFERENCES narratives(id) ON DELETE CASCADE,
    feedback_score numeric(3,2) NOT NULL CHECK (feedback_score >= 0.00 AND feedback_score <= 1.00),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, narrative_id)
);

-- Table for claim-narrative feedback
CREATE TABLE IF NOT EXISTS claim_narratives_feedback (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    claim_id uuid NOT NULL REFERENCES video_claims(id) ON DELETE CASCADE,
    narrative_id uuid NOT NULL REFERENCES narratives(id) ON DELETE CASCADE,
    feedback_score numeric(3,2) NOT NULL CHECK (feedback_score >= 0.00 AND feedback_score <= 1.00),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, claim_id, narrative_id),
    FOREIGN KEY (claim_id, narrative_id) REFERENCES claim_narratives(claim_id, narrative_id) ON DELETE CASCADE
);

-- Indexes for better query performance
CREATE INDEX idx_narrative_feedback_user_id ON narrative_feedback(user_id);
CREATE INDEX idx_narrative_feedback_narrative_id ON narrative_feedback(narrative_id);
CREATE INDEX idx_narrative_feedback_score ON narrative_feedback(feedback_score);

CREATE INDEX idx_claim_narratives_feedback_user_id ON claim_narratives_feedback(user_id);
CREATE INDEX idx_claim_narratives_feedback_claim_id ON claim_narratives_feedback(claim_id);
CREATE INDEX idx_claim_narratives_feedback_narrative_id ON claim_narratives_feedback(narrative_id);
CREATE INDEX idx_claim_narratives_feedback_score ON claim_narratives_feedback(feedback_score);
CREATE INDEX idx_claim_narratives_feedback_claim_narrative ON claim_narratives_feedback(claim_id, narrative_id);

COMMIT;
BEGIN;

CREATE TABLE IF NOT EXISTS claim_narratives (
    claim_id uuid REFERENCES video_claims(id) ON DELETE CASCADE,
    narrative_id uuid REFERENCES narratives(id) ON DELETE CASCADE,
    PRIMARY KEY (claim_id, narrative_id)
);

CREATE INDEX idx_claim_narratives_claim_id ON claim_narratives(claim_id);
CREATE INDEX idx_claim_narratives_narrative_id ON claim_narratives(narrative_id);

COMMIT;
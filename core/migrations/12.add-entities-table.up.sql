BEGIN;

-- Create entities table
CREATE TABLE IF NOT EXISTS entities (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    wikidata_id text NOT NULL UNIQUE,
    name text NOT NULL,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_entities_wikidata_id ON entities(wikidata_id);
CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name);

-- Create claim_entities junction table
CREATE TABLE IF NOT EXISTS claim_entities (
    claim_id uuid NOT NULL REFERENCES video_claims(id) ON DELETE CASCADE,
    entity_id uuid NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    PRIMARY KEY (claim_id, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_claim_entities_claim_id ON claim_entities(claim_id);
CREATE INDEX IF NOT EXISTS idx_claim_entities_entity_id ON claim_entities(entity_id);

-- Create narrative_entities junction table
CREATE TABLE IF NOT EXISTS narrative_entities (
    narrative_id uuid NOT NULL REFERENCES narratives(id) ON DELETE CASCADE,
    entity_id uuid NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    PRIMARY KEY (narrative_id, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_narrative_entities_narrative_id ON narrative_entities(narrative_id);
CREATE INDEX IF NOT EXISTS idx_narrative_entities_entity_id ON narrative_entities(entity_id);

COMMIT;
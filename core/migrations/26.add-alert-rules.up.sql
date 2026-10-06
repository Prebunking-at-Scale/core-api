BEGIN;

-- The new alerts (frontend docs/alerts-implementation.md). An alert belongs to the
-- person who created it and is e-mailed to them only. It has conditions, combined with
-- OR: a new narrative, a new claim, or a new claim in one narrative, each with the
-- search's filters. Today's tables (alerts, alerts_triggered, alert_executions) stay
-- until the new alerts have been checked in production.
-- Times are TIMESTAMP like the content tables they are compared with.

CREATE TYPE alert_condition_type AS ENUM ('new_narrative', 'new_claim', 'new_claim_in_narrative');

CREATE TABLE alert_rules (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organisation_id uuid NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name text NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 120),
    enabled boolean NOT NULL DEFAULT true,
    -- Order in the alerts panel, and in the e-mail
    position integer NOT NULL,
    -- The starting point: only what appears after it is reported (no backlog). Reset
    -- when the conditions change or the alert is enabled again.
    counting_since TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX alert_rules_owner_idx ON alert_rules (user_id, organisation_id, position);

CREATE TABLE alert_rule_conditions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id uuid NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    -- "Condition 1, 2…"
    position integer NOT NULL,
    type alert_condition_type NOT NULL,
    -- The followed narrative. No foreign key: a deleted narrative just never matches
    -- again; a merged one is followed in its target (see the merge endpoint).
    narrative_id uuid,
    filters jsonb NOT NULL DEFAULT '{}',
    CHECK ((type = 'new_claim_in_narrative') = (narrative_id IS NOT NULL))
);
CREATE INDEX alert_rule_conditions_alert_idx ON alert_rule_conditions (alert_id, position);
CREATE INDEX alert_rule_conditions_narrative_idx ON alert_rule_conditions (narrative_id)
    WHERE narrative_id IS NOT NULL;

-- What each alert has reported: every element once per alert.
CREATE TABLE alert_rule_reports (
    alert_id uuid NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    element_kind text NOT NULL CHECK (element_kind IN ('narrative', 'claim')),
    element_id uuid NOT NULL,
    -- The positions of the conditions it met, and where the e-mail listed it
    conditions integer[] NOT NULL,
    section text NOT NULL CHECK (section IN ('narratives', 'in_narrative', 'claims')),
    in_narrative_id uuid,
    reported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (alert_id, element_kind, element_id)
);
CREATE INDEX alert_rule_reports_reported_idx ON alert_rule_reports (alert_id, reported_at DESC);

-- When a claim joined a narrative, for "New claim in this narrative". Links made
-- before this migration keep NULL: they joined before any alert's starting point.
ALTER TABLE claim_narratives ADD COLUMN created_at TIMESTAMP;
ALTER TABLE claim_narratives ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP;
CREATE INDEX claim_narratives_narrative_created_idx ON claim_narratives (narrative_id, created_at);

COMMIT;

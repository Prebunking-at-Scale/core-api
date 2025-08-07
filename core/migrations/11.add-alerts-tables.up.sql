BEGIN;

CREATE TYPE alert_type AS ENUM (
    'narrative_views',
    'narrative_claims_count',
    'narrative_videos_count',
    'narrative_with_topic',
    'keyword'
);

CREATE TYPE alert_scope AS ENUM (
    'general',
    'specific'
);

CREATE TABLE IF NOT EXISTS alerts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    organisation_id uuid NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
    alert_type alert_type NOT NULL,
    scope alert_scope NOT NULL,
    narrative_id uuid REFERENCES narratives(id) ON DELETE CASCADE,
    threshold INTEGER,
    topic_id uuid REFERENCES topics(id) ON DELETE CASCADE,
    keyword TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_views_alert CHECK (
        alert_type != 'narrative_views' OR threshold IS NOT NULL
    ),
    CONSTRAINT valid_claims_count_alert CHECK (
        alert_type != 'narrative_claims_count' OR threshold IS NOT NULL
    ),
    CONSTRAINT valid_videos_count_alert CHECK (
        alert_type != 'narrative_videos_count' OR threshold IS NOT NULL
    ),
    CONSTRAINT valid_topic_alert CHECK (
        alert_type != 'narrative_with_topic' OR topic_id IS NOT NULL
    ),
    CONSTRAINT valid_keyword_alert CHECK (
        alert_type != 'keyword' OR keyword IS NOT NULL
    ),
    CONSTRAINT valid_specific_alert CHECK (
        scope != 'specific' OR narrative_id IS NOT NULL
    ),
    CONSTRAINT valid_general_alert CHECK (
        scope != 'general' OR narrative_id IS NULL
    )
);

CREATE TABLE IF NOT EXISTS alert_executions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    executed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    alerts_checked INTEGER NOT NULL DEFAULT 0,
    alerts_triggered INTEGER NOT NULL DEFAULT 0,
    emails_sent INTEGER NOT NULL DEFAULT 0,
    metadata JSONB DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS alerts_triggered (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id uuid NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    narrative_id uuid NOT NULL REFERENCES narratives(id) ON DELETE CASCADE,
    triggered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    trigger_value INTEGER,
    threshold_crossed INTEGER, -- The threshold that was crossed, in the future we may want to allow setting multiple thresholds
    notification_sent BOOLEAN NOT NULL DEFAULT FALSE,
    metadata JSONB DEFAULT '{}',
    
    UNIQUE(alert_id, narrative_id, threshold_crossed)
);

CREATE INDEX idx_alerts_user_org ON alerts(user_id, organisation_id);
CREATE INDEX idx_alerts_type_enabled ON alerts(alert_type, enabled);
CREATE INDEX idx_alerts_narrative ON alerts(narrative_id) WHERE narrative_id IS NOT NULL;
CREATE INDEX idx_alerts_topic ON alerts(topic_id) WHERE topic_id IS NOT NULL;
CREATE INDEX idx_alerts_keyword ON alerts(keyword) WHERE keyword IS NOT NULL;
CREATE INDEX idx_alerts_triggered_alert ON alerts_triggered(alert_id);
CREATE INDEX idx_alerts_triggered_narrative ON alerts_triggered(narrative_id);
CREATE INDEX idx_alerts_triggered_at ON alerts_triggered(triggered_at);

COMMIT;
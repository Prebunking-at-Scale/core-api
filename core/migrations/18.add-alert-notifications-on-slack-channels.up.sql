BEGIN;

-- Table for Slack OAuth temporary states (for CSRF protection during OAuth flow)
CREATE TABLE IF NOT EXISTS slack_oauth_states (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    state TEXT NOT NULL UNIQUE,
    organisation_id uuid NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL
);

-- Index for efficient state lookup and cleanup
CREATE INDEX idx_slack_oauth_states_state ON slack_oauth_states(state);
CREATE INDEX idx_slack_oauth_states_expires_at ON slack_oauth_states(expires_at);

-- Table for Slack workspace installations
CREATE TABLE IF NOT EXISTS slack_installations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organisation_id uuid NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
    
    -- Slack workspace identification
    team_id TEXT NOT NULL,
    team_name TEXT,
    enterprise_id TEXT,
    enterprise_name TEXT,
    enterprise_url TEXT,
    
    -- Slack app identification
    app_id TEXT,
    
    -- Bot credentials and info
    bot_token TEXT NOT NULL,
    bot_id TEXT,
    bot_user_id TEXT,
    bot_scopes TEXT,
    
    -- User who installed the app
    user_id TEXT,
    user_token TEXT,
    user_scopes TEXT,
    
    -- Incoming webhook configuration
    incoming_webhook_url TEXT,
    incoming_webhook_channel TEXT,
    incoming_webhook_channel_id TEXT,
    incoming_webhook_configuration_url TEXT,
    
    -- Installation metadata
    is_enterprise_install BOOLEAN DEFAULT FALSE,
    token_type TEXT,
    metadata JSONB DEFAULT '{}',
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure one installation per organisation per channel
    UNIQUE(organisation_id, incoming_webhook_channel_id)
);

-- Indexes for efficient lookups
CREATE INDEX idx_slack_installations_organisation_id ON slack_installations(organisation_id);
CREATE INDEX idx_slack_installations_enterprise_id ON slack_installations(enterprise_id) WHERE enterprise_id IS NOT NULL;
CREATE INDEX idx_slack_installations_incoming_webhook_channel_id ON slack_installations(incoming_webhook_channel_id) WHERE incoming_webhook_channel_id IS NOT NULL;

-- Add multi-channel notification tracking to alerts_triggered
ALTER TABLE alerts_triggered 
ADD COLUMN notification_status JSONB DEFAULT '{}'::jsonb;

-- Migrate existing notification_sent data to new format
UPDATE alerts_triggered 
SET notification_status = 
    CASE 
        WHEN notification_sent = TRUE THEN '{"email": "sent"}'::jsonb
        ELSE '{}'::jsonb
    END;

-- Create GIN index for efficient JSONB queries
CREATE INDEX idx_alerts_triggered_notification_status 
ON alerts_triggered USING gin(notification_status);

-- Note: notification_sent column kept for backward compatibility (deprecated)

-- Add channels column to alerts table for multi-channel notifications
ALTER TABLE alerts 
ADD COLUMN channels JSONB DEFAULT '[{"channel_type": "email"}]'::jsonb NOT NULL;

-- Create GIN index for efficient JSONB queries on channels
CREATE INDEX idx_alerts_channels 
ON alerts USING gin(channels);

-- Ensure all existing alerts have at least the email channel
-- This handles alerts that had no channels in metadata or had empty arrays
UPDATE alerts 
SET channels = '[{"channel_type": "email"}]'::jsonb
WHERE channels = '[]'::jsonb OR channels IS NULL;

-- Add constraint to ensure channels array is never empty
ALTER TABLE alerts
ADD CONSTRAINT alerts_channels_not_empty 
CHECK (jsonb_array_length(channels) > 0);


-- Rename the column (Rename emails_sent to notifications_sent in alert_executions table)
-- This migration makes the notification counting more generic to support multiple channels
ALTER TABLE alert_executions 
RENAME COLUMN emails_sent TO notifications_sent;

-- Update existing rows to include detailed breakdown in metadata
-- This preserves the email count in the new notifications_sent field
-- and adds it to metadata for backward compatibility
UPDATE alert_executions
SET metadata = jsonb_set(
    COALESCE(metadata, '{}'::jsonb),
    '{emails_sent}',
    to_jsonb(notifications_sent)
)
WHERE metadata IS NULL 
   OR NOT metadata ? 'emails_sent';

-- Add default for slack_messages_sent if not present
UPDATE alert_executions
SET metadata = jsonb_set(
    COALESCE(metadata, '{}'::jsonb),
    '{slack_messages_sent}',
    '0'::jsonb
)
WHERE metadata IS NULL 
   OR NOT metadata ? 'slack_messages_sent';

-- Add comment to the column
COMMENT ON COLUMN alert_executions.notifications_sent IS 'Total number of notifications sent across all channels (email + Slack + etc). Detailed breakdown available in metadata.';


COMMIT;

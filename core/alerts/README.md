# Alerts System

The alerts system monitors narratives and sends notifications via multiple channels (email, Slack) when specific thresholds or conditions are met.

## Architecture

- **Models** (`models.py`): Alert types, scopes, channel configs, and request/response models
- **Repository** (`repo.py`): Database access layer for alerts and triggered notifications
- **Service** (`service.py`): Business logic for processing alerts and sending notifications
- **Controller** (`controller.py`): HTTP endpoints for alert management

## Alert Types

- `NARRATIVE_VIEWS`: Alert when a narrative reaches a view count threshold
- `NARRATIVE_CLAIMS_COUNT`: Alert when a narrative reaches a claim count threshold
- `NARRATIVE_VIDEOS_COUNT`: Alert when a narrative reaches a video count threshold
- `NARRATIVE_WITH_TOPIC`: Alert when new narratives are tagged with a specific topic
- `KEYWORD`: Alert when narratives mention a specific keyword

## Alert Scopes

- `GENERAL`: Monitor all narratives in the organization
- `SPECIFIC`: Monitor a single narrative (requires `narrative_id`)

## Notification Channels

Alerts support multiple notification channels configured via the `channels` field:

### Email Channel
```json
{
  "channel_type": "email"
}
```
Sends notification to the alert owner's email address.

### Slack Channel
```json
{
  "channel_type": "slack",
  "slack_channel_id": "C12345678"
}
```
Sends notification to a specific Slack channel. Requires:
- Organization must have Slack installed (see [Slack Integration](../integrations/slack/README.md))
- `slack_channel_id` must match a channel from an existing Slack installation

**Note**: You can get the `slack_channel_id` from the `/integrations/slack/installations` endpoint.

### Multiple Channels
Alerts can be configured to send to multiple channels simultaneously:

```json
{
  "channels": [
    {"channel_type": "email"},
    {"channel_type": "slack", "slack_channel_id": "C12345678"}
  ]
}
```

## Database Schema

### `alerts` Table
- `id`: UUID primary key
- `user_id`: User who created the alert
- `organisation_id`: Organisation the alert belongs to
- `name`: Human-readable alert name
- `alert_type`: Type of alert (enum)
- `scope`: General or specific (enum)
- `narrative_id`: Required for specific scope
- `threshold`: Required for view/claim/video count alerts
- `topic_id`: Required for topic alerts
- `keyword`: Required for keyword alerts
- `enabled`: Boolean to enable/disable alerts
- `channels`: JSONB array of channel configurations (NOT NULL)
- `metadata`: JSONB for additional data
- `created_at`, `updated_at`: Timestamps

**Constraints**:
- `channels` must not be an empty array (`jsonb_array_length(channels) > 0`)
- Default value: `[{"channel_type": "email"}]`
- Various CHECK constraints for alert type validation

### `alerts_triggered` Table
Tracks when alerts are triggered and notification delivery status.

- `id`: UUID primary key
- `alert_id`: Foreign key to alerts
- `narrative_id`: Narrative that triggered the alert
- `triggered_at`: Timestamp
- `trigger_value`: Value that triggered the alert
- `threshold_crossed`: Threshold value
- `notification_sent`: Boolean (deprecated, use `notification_status`)
- `notification_status`: JSONB tracking per-channel delivery
  ```json
  {
    "email": "sent",
    "slack": "sent"
  }
  ```
- `metadata`: Additional data

### `alert_executions` Table
Tracks alert processing runs.

- `id`: UUID primary key
- `executed_at`: Timestamp
- `alerts_checked`: Count of alerts checked
- `alerts_triggered`: Count of alerts triggered
- `emails_sent`: Count of emails sent
- `metadata`: Execution stats

## API Usage

### Create an Alert

#### Email-only Alert (Default)
```http
POST /alerts
Authorization: Bearer <token>
Content-Type: application/json

{
  "name": "High View Count Alert",
  "alert_type": "narrative_views",
  "scope": "general",
  "threshold": 100000
}
```

#### Slack-only Alert
```http
POST /alerts
Authorization: Bearer <token>
Content-Type: application/json

{
  "name": "Climate Topic Alert",
  "alert_type": "narrative_with_topic",
  "scope": "general",
  "topic_id": "456e7890-e89b-12d3-a456-426614174000",
  "channels": [
    {"channel_type": "slack", "slack_channel_id": "C87654321"}
  ]
}
```

#### Multi-channel Alert
```http
POST /alerts
Authorization: Bearer <token>
Content-Type: application/json

{
  "name": "Critical Claims Alert",
  "alert_type": "narrative_claims_count",
  "scope": "specific",
  "narrative_id": "123e4567-e89b-12d3-a456-426614174000",
  "threshold": 50,
  "channels": [
    {"channel_type": "email"},
    {"channel_type": "slack", "slack_channel_id": "C12345678"}
  ]
}
```

### Get User Alerts
```http
GET /alerts?enabled_only=true&limit=50&offset=0
Authorization: Bearer <token>
```

Returns paginated list of alerts with their channel configurations.

### Get Alert by ID
```http
GET /alerts/{alert_id}
Authorization: Bearer <token>
```

### Update Alert
```http
PATCH /alerts/{alert_id}
Authorization: Bearer <token>
Content-Type: application/json

{
  "name": "Updated Alert Name",
  "enabled": false,
  "channels": [{"channel_type": "email"}]
}
```

### Delete Alert
```http
DELETE /alerts/{alert_id}
Authorization: Bearer <token>
```

## Alert Processing

Alerts are processed periodically by the background service:

1. **Check Alerts**: Query enabled alerts and check if their conditions are met
2. **Trigger Alerts**: Record triggered alerts in `alerts_triggered` table
3. **Send Notifications**: 
   - Group alerts by (user_id, organisation_id)
   - Separate by channel type (email, Slack)
   - Send notifications independently per channel
   - Track delivery status in `notification_status` JSONB field
4. **Record Execution**: Log statistics in `alert_executions`

### Notification Delivery

- **Email**: Sent to alert owner's email address via configured email service
- **Slack**: Sent to specified channel using organization's Slack bot token
- **Failure Handling**: Each channel delivery is independent - Slack failure won't block email delivery

## Setup for Slack Notifications

To use Slack notifications, organizations must:

1. Install the Slack app via `/integrations/slack/install-url`
2. Complete OAuth flow (redirects to `/integrations/slack/oauth/callback`)
3. Get the `slack_channel_id` from `/integrations/slack/installations`
4. Use the channel ID when creating alerts

See [Slack Integration README](../integrations/slack/README.md) for details.

## Examples

### Example Response from GET /alerts
```json
{
  "data": [
    {
      "id": "24ad2de3-db26-4bd3-aacb-261214fd02a3",
      "user_id": "f1234567-e89b-12d3-a456-426614174000",
      "organisation_id": "a1234567-e89b-12d3-a456-426614174000",
      "name": "Development Alert",
      "alert_type": "narrative_views",
      "scope": "general",
      "threshold": 10000,
      "enabled": true,
      "channels": [
        {"channel_type": "email"},
        {"channel_type": "slack", "slack_channel_id": "C0AMLPMBTGR"}
      ],
      "metadata": {},
      "created_at": "2026-03-25T10:00:00Z",
      "updated_at": "2026-03-25T10:00:00Z"
    }
  ],
  "total": 1,
  "page": 1,
  "size": 50
}
```

### Example Notification Status
After an alert is triggered and notifications are sent:

```json
{
  "id": "t1234567-e89b-12d3-a456-426614174000",
  "alert_id": "24ad2de3-db26-4bd3-aacb-261214fd02a3",
  "narrative_id": "n1234567-e89b-12d3-a456-426614174000",
  "triggered_at": "2026-03-25T12:30:00Z",
  "trigger_value": 15000,
  "threshold_crossed": 10000,
  "notification_status": {
    "email": "sent",
    "slack": "sent"
  },
  "metadata": {}
}
```

## Migration History

- **Migration 11**: Initial alerts tables (alerts, alerts_triggered, alert_executions)
- **Migration 18**: 
  - Added `notification_status` JSONB to `alerts_triggered`
  - Added `channels` JSONB to `alerts` with default `[{"channel_type": "email"}]`
  - Added constraint: `channels` must not be empty
  - Migrated existing channel configs from metadata to dedicated column

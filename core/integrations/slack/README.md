# Slack Integration

This module provides OAuth2 authentication and messaging capabilities for Slack workspaces, storing installation data in PostgreSQL instead of file-based storage.

## Architecture

The integration follows the repository pattern used throughout the project:

- **Models** (`models.py`): Pydantic models for `SlackInstallation` and `SlackOAuthState`
- **Repository** (`repo.py`): Database access layer for managing installations and OAuth states
- **Service** (`service.py`): Business logic for OAuth flow and messaging
- **Controller** (`controller.py`): HTTP endpoints for installation and OAuth callback

## Database Tables

### `slack_oauth_states`
Temporary storage for OAuth state tokens (CSRF protection). States expire after 5 minutes.

- `id`: UUID primary key
- `state`: Unique OAuth state token
- `organisation_id`: Organisation this installation is for
- `created_at`: Timestamp
- `expires_at`: Expiration timestamp

### `slack_installations`
Persistent storage for Slack workspace installations.

- `id`: UUID primary key
- `organisation_id`: Foreign key to organisations table
- `team_id`: Slack workspace ID
- `team_name`: Slack workspace name
- `bot_token`: Bot token for API calls (sensitive!)
- `incoming_webhook_url`: URL for posting messages
- `incoming_webhook_channel`: Default channel name
- `incoming_webhook_channel_id`: Default channel ID
- Additional fields for enterprise installations, user tokens, etc.
- `created_at`, `updated_at`: Timestamps

**Constraint**: One installation per (organisation_id, team_id) pair. Reinstalling updates the existing record.

## Usage

### Installing Slack for an Organisation

1. **Generate installation URL** (requires authentication):
   ```http
   GET /integrations/slack/install-url
   Authorization: Bearer <token>
   ```

   Returns:
   ```json
   {
     "install_url": "https://slack.com/oauth/v2/authorize?state=..."
   }
   ```

2. **User clicks the URL** and authorizes the app in their Slack workspace.

3. **Slack redirects** to `/integrations/slack/oauth/callback?code=...&state=...`

4. The callback endpoint:
   - Validates the state token
   - Exchanges the code for access tokens via Slack API
   - Saves the installation to the database
   - Returns success message

### Sending Messages

Use the service method:

```python
from core.integrations.slack.service import SlackService

await slack_service.send_message_to_slack(
    organisation_id=org.id,
    channel="#general",  # or channel ID like "C12345678"
    text="Hello from the API!"
)
```

This will:
1. Look up the installation for the organisation
2. Use the stored `bot_token` to authenticate with Slack
3. Post the message to the specified channel

### Getting Installation Info

**Get all installations for an organisation:**

```http
GET /api/integrations/slack/installations
Authorization: Bearer <token>
```

Returns a list of all Slack workspace installations for the authenticated user's organisation:

```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "organisation_id": "660e8400-e29b-41d4-a716-446655440000",
    "team_id": "T12345678",
    "team_name": "Marketing Workspace",
    "bot_id": "B12345",
    "incoming_webhook_channel": "#marketing",
    "incoming_webhook_channel_id": "C12345678",
    "created_at": "2026-03-18T23:14:16"
  },
  {
    "id": "770e8400-e29b-41d4-a716-446655440000",
    "organisation_id": "660e8400-e29b-41d4-a716-446655440000",
    "team_id": "T87654321",
    "team_name": "Engineering Workspace",
    "bot_id": "B87654",
    "incoming_webhook_channel": "#engineering",
    "incoming_webhook_channel_id": "C87654321",
    "created_at": "2026-03-18T22:30:00"
  }
]
```

**Empty list if no installations:**
```json
[]
```

**Programmatically:**

```python
# Get all installations for an organisation
installations = await slack_service.get_installations_by_organisation(org.id)

if installations:
    # Use the first (most recent) installation
    installation = installations[0]
    print(f"Primary workspace: {installation.team_name} - Channel: {installation.incoming_webhook_channel}")
    
    # Or iterate through all installations
    for installation in installations:
        print(f"Workspace: {installation.team_name}")
else:
    print("No Slack installations found for this organisation")
```

**Note:** An organisation can have multiple Slack installations, one for each Slack workspace they want to connect. Each installation is identified by the unique combination of `(organisation_id, team_id)`. The installations are returned ordered by creation date (most recent first).

## Security Considerations

- **Bot tokens** are stored in plain text. Consider encrypting sensitive fields in production.
- **OAuth states** expire after 5 minutes to prevent replay attacks.
- The `/install-url` endpoint requires authentication to prevent unauthorized installations.
- The `/oauth/callback` endpoint is public (called by Slack) but validates the state token.

## Testing

Run the test suite:

```bash
python -m pytest tests/integrations/slack/ -v
```

Tests cover:
- OAuth state issuance and consumption
- State expiration
- Installation save/update/delete
- Bot lookup by team_id or organisation_id
- Message sending (with mocked Slack API)
- Error cases (invalid state, missing installation, etc.)

## Migration from File-Based Storage

If you have existing installations in `./data/`:

1. Apply the database migration: `python -m core.migrate`
2. The new code will start using the database
3. Old file-based installations will be ignored
4. Users should re-install the app through the new OAuth flow

## Database Migration

The migration file `18.add-alert-notifications-on-slack-channels.up` creates both tables with proper indexes and foreign key constraints.

Apply it with:
```bash
python -m core.migrate
```

## Scopes

Current Slack app scopes:
- `incoming-webhook`: Post messages to channels

To add more functionality (e.g., read messages, manage channels), update the scopes in `service.py`:

```python
authorize_url_generator = AuthorizeUrlGenerator(
    client_id=SLACK_CLIENT_ID,
    scopes=["incoming-webhook", "chat:write"],
    redirect_uri=SLACK_REDIRECT_URI,
)
```

## Configuration

Required environment variables (typically in `.env`):

- `SLACK_CLIENT_ID`: Slack app client ID
- `SLACK_CLIENT_SECRET`: Slack app client secret
- `SLACK_REDIRECT_URI`: OAuth redirect URI (must match Slack app config)

Example:
```
SLACK_CLIENT_ID=123456789012.123456789012
SLACK_CLIENT_SECRET=abcdef0123456789abcdef0123456789
SLACK_REDIRECT_URI=https://api.example.com/integrations/slack/oauth/callback
```

## Future Enhancements

Potential improvements:

1. **Background job** to clean up expired OAuth states periodically
2. **Encryption** for sensitive fields (bot_token, user_token)
3. **Audit log** for all Slack operations
4. **Multiple installations** per organisation (different teams/channels)
5. **Slack Events API** for receiving messages/events
6. **Slash commands** and interactive components

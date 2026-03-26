from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class SlackOAuthState(BaseModel):
    """Temporary OAuth state for CSRF protection during Slack OAuth flow"""

    model_config = ConfigDict(str_strip_whitespace=True)

    id: UUID = Field(default_factory=uuid4)
    state: str
    organisation_id: UUID
    created_at: datetime | None = None
    expires_at: datetime


class SlackInstallation(BaseModel):
    """Slack workspace installation containing bot credentials and webhook configuration"""

    model_config = ConfigDict(str_strip_whitespace=True)

    id: UUID = Field(default_factory=uuid4)
    organisation_id: UUID

    # Slack workspace identification
    team_id: str
    team_name: str | None = None
    enterprise_id: str | None = None
    enterprise_name: str | None = None
    enterprise_url: str | None = None

    # Slack app identification
    app_id: str | None = None

    # Bot credentials and info
    bot_token: str
    bot_id: str | None = None
    bot_user_id: str | None = None
    bot_scopes: str | None = None

    # User who installed the app
    user_id: str | None = None
    user_token: str | None = None
    user_scopes: str | None = None

    # Incoming webhook configuration
    incoming_webhook_url: str | None = None
    incoming_webhook_channel: str | None = None
    incoming_webhook_channel_id: str | None = None
    incoming_webhook_configuration_url: str | None = None

    # Installation metadata
    is_enterprise_install: bool = False
    token_type: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    # Timestamps
    created_at: datetime | None = None
    updated_at: datetime | None = None


class SlackInstallationResponse(BaseModel):
    """Public-facing Slack installation info without sensitive credentials"""

    model_config = ConfigDict(str_strip_whitespace=True)

    id: UUID
    organisation_id: UUID

    # Slack workspace identification
    team_id: str
    team_name: str | None = None
    enterprise_id: str | None = None
    enterprise_name: str | None = None

    # Slack app identification
    app_id: str | None = None

    # Bot info (without token)
    bot_id: str | None = None
    bot_user_id: str | None = None
    bot_scopes: str | None = None

    # Incoming webhook configuration
    incoming_webhook_channel: str | None = None
    incoming_webhook_channel_id: str | None = None

    # Installation metadata
    is_enterprise_install: bool = False

    # Timestamps
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_installation(cls, installation: "SlackInstallation") -> "SlackInstallationResponse":
        """Convert a SlackInstallation to a public response, excluding sensitive fields"""
        return cls(
            id=installation.id,
            organisation_id=installation.organisation_id,
            team_id=installation.team_id,
            team_name=installation.team_name,
            enterprise_id=installation.enterprise_id,
            enterprise_name=installation.enterprise_name,
            app_id=installation.app_id,
            bot_id=installation.bot_id,
            bot_user_id=installation.bot_user_id,
            bot_scopes=installation.bot_scopes,
            incoming_webhook_channel=installation.incoming_webhook_channel,
            incoming_webhook_channel_id=installation.incoming_webhook_channel_id,
            is_enterprise_install=installation.is_enterprise_install,
            created_at=installation.created_at,
            updated_at=installation.updated_at,
        )

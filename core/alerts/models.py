from datetime import datetime
from enum import Enum
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, ConfigDict, model_validator


class AlertType(str, Enum):
    NARRATIVE_VIEWS = "narrative_views"
    NARRATIVE_CLAIMS_COUNT = "narrative_claims_count"
    NARRATIVE_VIDEOS_COUNT = "narrative_videos_count"
    NARRATIVE_WITH_TOPIC = "narrative_with_topic"
    KEYWORD = "keyword"


class AlertScope(str, Enum):
    GENERAL = "general"
    SPECIFIC = "specific"


class ChannelType(str, Enum):
    """Notification channel types for alerts"""
    EMAIL = "email"
    SLACK = "slack"


class ChannelConfig(BaseModel):
    """Configuration for a notification channel"""
    channel_type: ChannelType
    slack_channel_id: str | None = None
    
    @model_validator(mode='after')
    def validate_slack_config(self) -> 'ChannelConfig':
        if self.channel_type == ChannelType.SLACK and not self.slack_channel_id:
            raise ValueError("slack_channel_id is required for Slack channels")
        if self.channel_type == ChannelType.EMAIL and self.slack_channel_id:
            raise ValueError("slack_channel_id should not be provided for email channels")
        return self


class Alert(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    user_id: UUID
    organisation_id: UUID
    name: str
    alert_type: AlertType
    scope: AlertScope
    narrative_id: UUID | None = None
    threshold: int | None = None
    topic_id: UUID | None = None
    keyword: str | None = None
    enabled: bool = True
    channels: list[dict[str, Any]] = Field(
        default_factory=lambda: [{"channel_type": "email"}]
    )
    metadata: dict[str, Any] = {}
    created_at: datetime | None = None
    updated_at: datetime | None = None
    
    @property
    def channel_configs(self) -> list[dict[str, Any]]:
        """Get configured notification channels with their configs"""
        return self.channels
    
    @property
    def has_email_channel(self) -> bool:
        """Check if alert is configured for email notifications"""
        return any(c.get("channel_type") == "email" for c in self.channels)
    
    @property
    def has_slack_channel(self) -> bool:
        """Check if alert is configured for Slack notifications"""
        return any(c.get("channel_type") == "slack" for c in self.channels)
    
    @property
    def slack_channel_ids(self) -> list[str]:
        """Get all configured Slack channel IDs"""
        return [
            c["slack_channel_id"] 
            for c in self.channels 
            if c.get("channel_type") == "slack" and "slack_channel_id" in c
        ]


class AlertExecution(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    executed_at: datetime
    alerts_checked: int
    alerts_triggered: int
    notifications_sent: int
    metadata: dict[str, Any] = {}


class AlertTriggered(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    alert_id: UUID
    narrative_id: UUID
    triggered_at: datetime
    trigger_value: int | None = None
    threshold_crossed: int | None = None
    notification_sent: bool = False  # Deprecated: use notification_status instead
    notification_status: dict[str, str] = {}  # {"email": "sent", "slack": "failed"}
    metadata: dict[str, Any] = {}


class CreateAlertRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "High View Count Alert",
                    "alert_type": "narrative_views",
                    "scope": "general",
                    "threshold": 100000,
                    "channels": [{"channel_type": "email"}],
                    "metadata": {"description": "Alert when any narrative exceeds 100000 views"}
                },
                {
                    "name": "Claims Threshold Alert",
                    "alert_type": "narrative_claims_count",
                    "scope": "specific",
                    "narrative_id": "123e4567-e89b-12d3-a456-426614174000",
                    "threshold": 50,
                    "channels": [
                        {"channel_type": "email"},
                        {"channel_type": "slack", "slack_channel_id": "C12345678"}
                    ],
                    "metadata": {"description": "Alert when specific narrative has 50+ claims"}
                },
                {
                    "name": "Climate Topic Monitor",
                    "alert_type": "narrative_with_topic",
                    "scope": "general",
                    "topic_id": "456e7890-e89b-12d3-a456-426614174000",
                    "channels": [{"channel_type": "slack", "slack_channel_id": "C87654321"}],
                    "metadata": {"description": "Alert for new narratives with climate topic"}
                },
                {
                    "name": "Vaccine Keyword Tracker",
                    "alert_type": "keyword",
                    "scope": "general",
                    "keyword": "vaccine",
                    "channels": [{"channel_type": "email"}],
                    "metadata": {"description": "Alert when narratives mention 'vaccine'"}
                }
            ]
        }
    )
    
    name: str = Field(..., description="Name to identify the alert", min_length=1, max_length=255)
    alert_type: AlertType = Field(..., description="Type of alert to create")
    scope: AlertScope = Field(..., description="Scope of the alert (general for all narratives, specific for one)")
    narrative_id: UUID | None = Field(None, description="Required for specific scope alerts. Cannot be used with general scope")
    threshold: int | None = Field(None, description="Required for narrative_views, narrative_claims_count, narrative_videos_count alerts", ge=1)
    topic_id: UUID | None = Field(None, description="Required for narrative_with_topic alerts only")
    keyword: str | None = Field(None, description="Required for keyword alerts only", min_length=1, max_length=255)
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata for the alert")
    channels: list[ChannelConfig] = Field(
        default_factory=lambda: [ChannelConfig(channel_type=ChannelType.EMAIL)],
        description="Notification channels for this alert (email, Slack, or both)"
    )
    
    @model_validator(mode='after')
    def validate_alert_fields(self):
        # Validate scope and narrative_id combination
        if self.scope == AlertScope.GENERAL and self.narrative_id is not None:
            raise ValueError("General scope alerts cannot have a narrative_id")
        
        if self.scope == AlertScope.SPECIFIC and self.narrative_id is None:
            raise ValueError("Specific scope alerts require a narrative_id")
        
        # Validate alert type specific fields
        threshold_types = {
            AlertType.NARRATIVE_VIEWS,
            AlertType.NARRATIVE_CLAIMS_COUNT,
            AlertType.NARRATIVE_VIDEOS_COUNT
        }
        
        if self.alert_type in threshold_types:
            if self.threshold is None:
                raise ValueError(f"{self.alert_type.value} alerts require a threshold")
            if self.topic_id is not None:
                raise ValueError(f"{self.alert_type.value} alerts cannot have a topic_id")
            if self.keyword is not None:
                raise ValueError(f"{self.alert_type.value} alerts cannot have a keyword")
        
        elif self.alert_type == AlertType.NARRATIVE_WITH_TOPIC:
            if self.topic_id is None:
                raise ValueError("narrative_with_topic alerts require a topic_id")
            if self.threshold is not None:
                raise ValueError("narrative_with_topic alerts cannot have a threshold")
            if self.keyword is not None:
                raise ValueError("narrative_with_topic alerts cannot have a keyword")
            if self.scope != AlertScope.GENERAL:
                raise ValueError("narrative_with_topic alerts must have general scope")
        
        elif self.alert_type == AlertType.KEYWORD:
            if self.keyword is None:
                raise ValueError("keyword alerts require a keyword")
            if self.threshold is not None:
                raise ValueError("keyword alerts cannot have a threshold")
            if self.topic_id is not None:
                raise ValueError("keyword alerts cannot have a topic_id")
            if self.scope != AlertScope.GENERAL:
                raise ValueError("keyword alerts must have general scope")
        
        return self


class UpdateAlertRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "Updated Alert Name",
                    "enabled": False
                },
                {
                    "name": "High Priority Alert",
                    "threshold": 2000,
                    "enabled": True
                },
                {
                    "keyword": "climate change"
                }
            ]
        }
    )
    
    name: str | None = Field(None, description="New name for the alert", min_length=1, max_length=255)
    enabled: bool | None = Field(None, description="Enable or disable the alert")
    threshold: int | None = Field(None, description="New threshold value", ge=1)
    keyword: str | None = Field(None, description="New keyword to search for", min_length=1, max_length=255)
    channels: list[ChannelConfig] | None = Field(
        None,
        description="Updated notification channels for this alert (email, Slack, or both). If not provided, channels will not be updated."
    )
    metadata: dict[str, Any] = {}
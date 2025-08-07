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


class Alert(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    user_id: UUID
    organisation_id: UUID
    alert_type: AlertType
    scope: AlertScope
    narrative_id: UUID | None = None
    threshold: int | None = None
    topic_id: UUID | None = None
    keyword: str | None = None
    enabled: bool = True
    metadata: dict[str, Any] = {}
    created_at: datetime | None = None
    updated_at: datetime | None = None


class AlertExecution(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    executed_at: datetime
    alerts_checked: int
    alerts_triggered: int
    emails_sent: int
    metadata: dict[str, Any] = {}


class AlertTriggered(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    alert_id: UUID
    narrative_id: UUID
    triggered_at: datetime
    trigger_value: int | None = None
    threshold_crossed: int | None = None
    notification_sent: bool = False
    metadata: dict[str, Any] = {}


class CreateAlertRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "alert_type": "narrative_views",
                    "scope": "general",
                    "threshold": 1000,
                    "metadata": {"description": "Alert when any narrative exceeds 1000 views"}
                },
                {
                    "alert_type": "narrative_claims_count",
                    "scope": "specific",
                    "narrative_id": "123e4567-e89b-12d3-a456-426614174000",
                    "threshold": 50,
                    "metadata": {"description": "Alert when specific narrative has 50+ claims"}
                },
                {
                    "alert_type": "narrative_with_topic",
                    "scope": "general",
                    "topic_id": "456e7890-e89b-12d3-a456-426614174000",
                    "metadata": {"description": "Alert for new narratives with climate topic"}
                },
                {
                    "alert_type": "keyword",
                    "scope": "general",
                    "keyword": "vaccine",
                    "metadata": {"description": "Alert when narratives mention 'vaccine'"}
                }
            ]
        }
    )
    
    alert_type: AlertType = Field(..., description="Type of alert to create")
    scope: AlertScope = Field(..., description="Scope of the alert (general for all narratives, specific for one)")
    narrative_id: UUID | None = Field(None, description="Required for specific scope alerts. Cannot be used with general scope")
    threshold: int | None = Field(None, description="Required for narrative_views, narrative_claims_count, narrative_videos_count alerts", ge=1)
    topic_id: UUID | None = Field(None, description="Required for narrative_with_topic alerts only")
    keyword: str | None = Field(None, description="Required for keyword alerts only", min_length=1, max_length=255)
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata for the alert")
    
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
                    "enabled": False
                },
                {
                    "threshold": 2000,
                    "enabled": True
                },
                {
                    "keyword": "climate change"
                }
            ]
        }
    )
    
    enabled: bool | None = Field(None, description="Enable or disable the alert")
    threshold: int | None = Field(None, description="New threshold value", ge=1)
    keyword: str | None = Field(None, description="New keyword to search for", min_length=1, max_length=255)
    metadata: dict[str, Any] = {}
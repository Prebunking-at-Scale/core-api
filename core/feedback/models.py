from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class FeedbackInput(BaseModel):
    """Base input model for feedback submission"""
    feedback_score: float = Field(ge=0.0, le=1.0, description="Feedback score between 0 and 1")
    feedback_text: str | None = Field(default=None, description="Optional text feedback")


class NarrativeFeedbackInput(FeedbackInput):
    """Input model for submitting narrative feedback"""
    pass


class ClaimNarrativeFeedbackInput(FeedbackInput):
    """Input model for submitting claim-narrative feedback"""
    pass


class FeedbackResponse(BaseModel):
    """Response model for feedback queries"""
    feedback_id: UUID
    user_id: UUID
    target_id: UUID
    target_type: Literal["narrative", "claim", "entity"]
    feedback_score: float
    created_at: str
    updated_at: str


class BulkFeedbackResponse(BaseModel):
    """Response model for bulk feedback operations"""
    success: bool
    message: str
    updated_count: int = 0

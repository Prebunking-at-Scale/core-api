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

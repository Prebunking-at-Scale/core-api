from uuid import UUID

from litestar import Controller, get, post
from litestar.di import Provide
from litestar.exceptions import NotFoundException

from core.auth.models import User
from core.feedback.models import (
    ClaimNarrativeFeedbackInput,
    NarrativeFeedbackInput,
)
from core.feedback.service import FeedbackService
from core.models import ClaimNarrativeFeedback, NarrativeFeedback
from core.response import JSON
from core.uow import ConnectionFactory


async def feedback_service(
    connection_factory: ConnectionFactory,
) -> FeedbackService:
    return FeedbackService(connection_factory=connection_factory)


class NarrativeFeedbackController(Controller):
    path = "/feedback/narratives/{narrative_id:uuid}"
    tags = ["feedback", "narratives"]

    dependencies = {
        "feedback_service": Provide(feedback_service),
    }

    @post(
        path="/",
        summary="Submit feedback for a narrative",
        description="Submit or update user feedback for a specific narrative. Score must be between 0.0 and 1.0.",
    )
    async def submit_narrative_feedback(
        self,
        feedback_service: FeedbackService,
        user: User,
        narrative_id: UUID,
        data: NarrativeFeedbackInput,
    ) -> JSON[NarrativeFeedback]:
        feedback = await feedback_service.submit_narrative_feedback(
            user_id=user.id,
            narrative_id=narrative_id,
            feedback_score=data.feedback_score,
        )
        return JSON(feedback)

    @get(
        path="/",
        summary="Get user's feedback for a narrative",
        description="Retrieve the current user's feedback for the specified narrative.",
    )
    async def get_narrative_feedback(
        self,
        feedback_service: FeedbackService,
        user: User,
        narrative_id: UUID,
    ) -> JSON[NarrativeFeedback]:
        feedback = await feedback_service.get_narrative_feedback(
            user_id=user.id, narrative_id=narrative_id
        )
        if feedback is None:
            raise NotFoundException(f"No feedback found for narrative {narrative_id}")
        return JSON(feedback)


class ClaimNarrativeFeedbackController(Controller):
    path = "/feedback/claims/{claim_id:uuid}/narratives/{narrative_id:uuid}"
    tags = ["feedback", "claims", "narratives"]

    dependencies = {
        "feedback_service": Provide(feedback_service),
    }

    @post(
        path="/",
        summary="Submit feedback for a claim-narrative relationship",
        description="Submit or update user feedback for a specific claim within a specific narrative context. Score must be between 0.0 and 1.0.",
    )
    async def submit_claim_narrative_feedback(
        self,
        feedback_service: FeedbackService,
        user: User,
        claim_id: UUID,
        narrative_id: UUID,
        data: ClaimNarrativeFeedbackInput,
    ) -> JSON[ClaimNarrativeFeedback]:
        feedback = await feedback_service.submit_claim_narrative_feedback(
            user_id=user.id,
            claim_id=claim_id,
            narrative_id=narrative_id,
            feedback_score=data.feedback_score,
        )
        return JSON(feedback)

    @get(
        path="/",
        summary="Get user's feedback for a claim-narrative relationship",
        description="Retrieve the current user's feedback for the specified claim-narrative relationship.",
    )
    async def get_claim_narrative_feedback(
        self,
        feedback_service: FeedbackService,
        user: User,
        claim_id: UUID,
        narrative_id: UUID,
    ) -> JSON[ClaimNarrativeFeedback]:
        feedback = await feedback_service.get_claim_narrative_feedback(
            user_id=user.id, claim_id=claim_id, narrative_id=narrative_id
        )
        if feedback is None:
            raise NotFoundException(
                f"No feedback found for claim {claim_id} and narrative {narrative_id}"
            )
        return JSON(feedback)


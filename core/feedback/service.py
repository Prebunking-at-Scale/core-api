import logging
from typing import AsyncContextManager
from uuid import UUID

import httpx
from litestar.exceptions import NotFoundException

from core.config import NARRATIVES_API_KEY, NARRATIVES_BASE_URL
from core.feedback.repo import FeedbackRepository
from core.models import ClaimNarrativeFeedback, NarrativeFeedback
from core.uow import ConnectionFactory, uow

logger = logging.getLogger(__name__)


class FeedbackService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[FeedbackRepository]:
        return uow(FeedbackRepository, self._connection_factory)

    # Narrative feedback methods
    async def submit_narrative_feedback(
        self, user_id: UUID, narrative_id: UUID, feedback_score: float, feedback_text: str | None = None
    ) -> NarrativeFeedback:
        """Submit feedback for a narrative"""
        async with self.repo() as repo:
            # Check if narrative exists
            if not await repo.narrative_exists(narrative_id):
                raise NotFoundException(f"Narrative with id {narrative_id} not found")

            # Send feedback to external narratives API FIRST
            await self.send_feedback_score_to_external_narratives_api(
                narrative_id=narrative_id,
                feedback_score=feedback_score,
                comment=feedback_text,
            )
            logger.info(f"Successfully sent narrative feedback to external API: narrative_id={narrative_id}, score={feedback_score}")

            # Only save to database if external API call succeeded
            feedback = await repo.submit_narrative_feedback(user_id, narrative_id, feedback_score, feedback_text)
            logger.info(f"Successfully saved narrative feedback to database: user_id={user_id}, narrative_id={narrative_id}, score={feedback_score}")

            return feedback

    async def get_narrative_feedback(
        self, user_id: UUID, narrative_id: UUID
    ) -> NarrativeFeedback | None:
        """Get user's feedback for a narrative"""
        async with self.repo() as repo:
            return await repo.get_narrative_feedback(user_id, narrative_id)

    # Claim-narrative feedback methods
    async def submit_claim_narrative_feedback(
        self, user_id: UUID, claim_id: UUID, narrative_id: UUID, feedback_score: float, feedback_text: str | None = None
    ) -> ClaimNarrativeFeedback:
        """Submit feedback for a claim-narrative relationship"""
        async with self.repo() as repo:
            # Check if claim-narrative relationship exists
            if not await repo.claim_narrative_relationship_exists(claim_id, narrative_id):
                raise NotFoundException(f"Claim-narrative relationship with claim_id {claim_id} and narrative_id {narrative_id} not found")

            # Send feedback to external narratives API FIRST
            await self.send_feedback_score_to_external_narratives_api(
                narrative_id=narrative_id,
                feedback_score=feedback_score,
                content_id=claim_id,  # Use claim_id as content_id
                comment=feedback_text,
            )
            logger.info(f"Successfully sent claim-narrative feedback to external API: claim_id={claim_id}, narrative_id={narrative_id}, score={feedback_score}")

            # Only save to database if external API call succeeded
            feedback = await repo.submit_claim_narrative_feedback(user_id, claim_id, narrative_id, feedback_score, feedback_text)
            logger.info(f"Successfully saved claim-narrative feedback to database: user_id={user_id}, claim_id={claim_id}, narrative_id={narrative_id}, score={feedback_score}")

            return feedback

    async def get_claim_narrative_feedback(
        self, user_id: UUID, claim_id: UUID, narrative_id: UUID
    ) -> ClaimNarrativeFeedback | None:
        """Get user's feedback for a claim-narrative relationship"""
        async with self.repo() as repo:
            return await repo.get_claim_narrative_feedback(user_id, claim_id, narrative_id)

    async def send_feedback_score_to_external_narratives_api(self, narrative_id: UUID, feedback_score: float, content_id: UUID | None = None, comment: str | None = None) -> None:
        """Send feedback score to external analytics service"""
        payload = {
            "narrative_id": str(narrative_id),
            "feedback_score": feedback_score,
            "comment": comment,
        }
        
        if content_id:
            payload["content_id"] = str(content_id)
        
        headers: dict[str, str] = {}
        if NARRATIVES_API_KEY:
            headers["X-API-TOKEN"] = NARRATIVES_API_KEY
        
        logger.debug(f"Sending feedback to external API: {NARRATIVES_BASE_URL}/feedback, payload={payload}")
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{NARRATIVES_BASE_URL}/feedback",
                json=payload,
                headers=headers,
                timeout=10.0  # Add timeout to prevent hanging
            )
            
            # Log the response for debugging
            if response.status_code >= 400:
                logger.error(f"External API error: status={response.status_code}, response={response.text}")
                response.raise_for_status()
            else:
                logger.debug(f"External API success: status={response.status_code}")

from uuid import UUID

import psycopg
from psycopg.rows import DictRow

from core.models import ClaimNarrativeFeedback, NarrativeFeedback


class FeedbackRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    # Narrative feedback methods
    async def submit_narrative_feedback(
        self, user_id: UUID, narrative_id: UUID, feedback_score: float, feedback_text: str | None = None
    ) -> NarrativeFeedback:
        """Submit or update feedback for a narrative"""
        await self._session.execute(
            """
            INSERT INTO narrative_feedback (user_id, narrative_id, feedback_score, feedback_text, created_at, updated_at)
            VALUES (%(user_id)s, %(narrative_id)s, %(feedback_score)s, %(feedback_text)s, now(), now())
            ON CONFLICT (user_id, narrative_id)
            DO UPDATE SET
                feedback_score = EXCLUDED.feedback_score,
                feedback_text = EXCLUDED.feedback_text,
                updated_at = now()
            RETURNING *
            """,
            {
                "user_id": user_id,
                "narrative_id": narrative_id,
                "feedback_score": feedback_score,
                "feedback_text": feedback_text,
            },
        )
        row = await self._session.fetchone()
        if row is None:
            raise ValueError("Failed to insert narrative feedback")
        return NarrativeFeedback(**row)

    async def get_narrative_feedback(
        self, user_id: UUID, narrative_id: UUID
    ) -> NarrativeFeedback | None:
        """Get specific feedback from a user for a narrative"""
        await self._session.execute(
            """
            SELECT * FROM narrative_feedback 
            WHERE user_id = %(user_id)s AND narrative_id = %(narrative_id)s
            """,
            {"user_id": user_id, "narrative_id": narrative_id},
        )
        row = await self._session.fetchone()
        return NarrativeFeedback(**row) if row else None

    # Claim-narrative feedback methods
    async def submit_claim_narrative_feedback(
        self, user_id: UUID, claim_id: UUID, narrative_id: UUID, feedback_score: float, feedback_text: str | None = None
    ) -> ClaimNarrativeFeedback:
        """Submit or update feedback for a claim-narrative relationship"""
        await self._session.execute(
            """
            INSERT INTO claim_narratives_feedback (user_id, claim_id, narrative_id, feedback_score, feedback_text, created_at, updated_at)
            VALUES (%(user_id)s, %(claim_id)s, %(narrative_id)s, %(feedback_score)s, %(feedback_text)s, now(), now())
            ON CONFLICT (user_id, claim_id, narrative_id)
            DO UPDATE SET
                feedback_score = EXCLUDED.feedback_score,
                feedback_text = EXCLUDED.feedback_text,
                updated_at = now()
            RETURNING *
            """,
            {
                "user_id": user_id,
                "claim_id": claim_id,
                "narrative_id": narrative_id,
                "feedback_score": feedback_score,
                "feedback_text": feedback_text,
            },
        )
        row = await self._session.fetchone()
        if row is None:
            raise ValueError("Failed to insert claim-narrative feedback")
        return ClaimNarrativeFeedback(**row)

    async def get_claim_narrative_feedback(
        self, user_id: UUID, claim_id: UUID, narrative_id: UUID
    ) -> ClaimNarrativeFeedback | None:
        """Get specific feedback from a user for a claim-narrative relationship"""
        await self._session.execute(
            """
            SELECT * FROM claim_narratives_feedback 
            WHERE user_id = %(user_id)s AND claim_id = %(claim_id)s AND narrative_id = %(narrative_id)s
            """,
            {"user_id": user_id, "claim_id": claim_id, "narrative_id": narrative_id},
        )
        row = await self._session.fetchone()
        return ClaimNarrativeFeedback(**row) if row else None

    # Utility methods
    async def narrative_exists(self, narrative_id: UUID) -> bool:
        """Check if narrative exists"""
        await self._session.execute(
            "SELECT 1 FROM narratives WHERE id = %(narrative_id)s",
            {"narrative_id": narrative_id},
        )
        return await self._session.fetchone() is not None

    async def claim_narrative_relationship_exists(self, claim_id: UUID, narrative_id: UUID) -> bool:
        """Check if claim-narrative relationship exists"""
        await self._session.execute(
            """
            SELECT 1 FROM claim_narratives 
            WHERE claim_id = %(claim_id)s AND narrative_id = %(narrative_id)s
            """,
            {"claim_id": claim_id, "narrative_id": narrative_id},
        )
        return await self._session.fetchone() is not None

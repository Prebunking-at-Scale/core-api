"""Client for the external prebunking-narratives API."""

import logging
from uuid import UUID

import httpx

from core.config import NARRATIVES_API_KEY, NARRATIVES_BASE_URL

logger = logging.getLogger(__name__)

TIMEOUT = 60.0


class NarrativesApiClient:
    """Thin wrapper around the external narratives API.

    Methods return the httpx.Response so callers can decide how to handle
    errors (raise, log-and-ignore, etc.).
    """

    @staticmethod
    def _headers() -> dict[str, str]:
        headers: dict[str, str] = {}
        if NARRATIVES_API_KEY:
            headers["X-API-TOKEN"] = NARRATIVES_API_KEY
        return headers

    @staticmethod
    def is_configured() -> bool:
        return bool(NARRATIVES_BASE_URL)

    async def delete_narrative(self, external_narrative_id: str) -> httpx.Response:
        url = f"{NARRATIVES_BASE_URL}/narrative/{external_narrative_id}"
        async with httpx.AsyncClient() as client:
            return await client.delete(url, headers=self._headers(), timeout=TIMEOUT)

    async def update_narrative_title(
        self, external_narrative_id: str, title: str
    ) -> httpx.Response:
        return await self.update_narrative(external_narrative_id, title=title)

    async def update_narrative(
        self,
        external_narrative_id: str,
        title: str | None = None,
        narrative_context: str | None = None,
    ) -> httpx.Response:
        url = f"{NARRATIVES_BASE_URL}/narrative/{external_narrative_id}"
        payload: dict[str, str] = {}
        if title is not None:
            payload["title"] = title
        if narrative_context is not None:
            payload["narrative_context"] = narrative_context
        async with httpx.AsyncClient() as client:
            return await client.patch(
                url, json=payload, headers=self._headers(), timeout=TIMEOUT
            )

    async def add_contents(
        self, claims: list[dict[str, str | float]]
    ) -> httpx.Response:
        url = f"{NARRATIVES_BASE_URL}/add-contents"
        async with httpx.AsyncClient() as client:
            return await client.post(
                url,
                json={"claims": claims},
                headers=self._headers(),
                timeout=TIMEOUT,
            )

    async def initialize_dashboard(
        self, payload: dict
    ) -> httpx.Response:
        url = f"{NARRATIVES_BASE_URL}/initialize-dashboard"
        async with httpx.AsyncClient() as client:
            return await client.post(
                url,
                json=payload,
                headers=self._headers(),
                timeout=TIMEOUT,
            )

    async def send_feedback(
        self,
        narrative_id: UUID,
        feedback_score: float,
        content_id: UUID | None = None,
    ) -> httpx.Response:
        url = f"{NARRATIVES_BASE_URL}/feedback"
        payload: dict[str, str | float] = {
            "narrative_id": str(narrative_id),
            "feedback_score": feedback_score,
        }
        if content_id:
            payload["content_id"] = str(content_id)

        async with httpx.AsyncClient() as client:
            return await client.post(
                url, json=payload, headers=self._headers(), timeout=TIMEOUT
            )
    
    async def delete_claim_on_narrative(
        self,
        narrative_id: UUID,
        claim_id: UUID,
    ) -> httpx.Response:
        """
        Delete a claim from a narrative on the narratives service. 
        This is used when a claim is unlinked from a narrative in our system, so we need to tell the narratives service to remove it from their system as well to keep things in sync.
        """
        url = f"{NARRATIVES_BASE_URL}/narrative/{narrative_id}/claim/{claim_id}"
        async with httpx.AsyncClient() as client:
            return await client.delete(
                url, headers=self._headers(), timeout=TIMEOUT
            )

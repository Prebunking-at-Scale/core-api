"""Client for the external prebunking-narratives API."""

import logging
from uuid import UUID

import httpx

from core.config import NARRATIVES_API_KEY, NARRATIVES_BASE_URL

logger = logging.getLogger(__name__)

TIMEOUT = 10.0


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
        url = f"{NARRATIVES_BASE_URL}/narrative/{external_narrative_id}"
        async with httpx.AsyncClient() as client:
            return await client.patch(
                url, json={"title": title}, headers=self._headers(), timeout=TIMEOUT
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

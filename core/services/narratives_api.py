from typing import Dict, Any, Optional
import httpx
from core.config import NARRATIVES_API_KEY, NARRATIVES_BASE_URL


class NarrativesAPIClient:
    """Client for interacting with the external Narratives API."""

    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None):
        """
        Initialize the Narratives API client.

        Args:
            base_url: The base URL of the Narratives API. Defaults to environment variable.
            api_key: The API key for authentication. Defaults to environment variable.
        """
        self.base_url: str = base_url or NARRATIVES_BASE_URL or ""
        self.api_key: str = api_key or NARRATIVES_API_KEY or ""

        if not self.base_url:
            raise ValueError("NARRATIVES_BASE_URL environment variable must be set")

        if not self.api_key:
            raise ValueError("NARRATIVES_API_KEY environment variable must be set")

        # Ensure base_url doesn't end with a slash
        self.base_url = self.base_url.rstrip("/")

    async def delete_narrative(self, narrative_id: int) -> Dict[str, Any]:
        """
        Delete a narrative from the external system using its backend ID.

        Args:
            narrative_id: The backend ID of the narrative to delete.

        Returns:
            A dictionary containing the response from the API.

        Raises:
            httpx.HTTPStatusError: If the request fails with a non-2xx status code.
            httpx.RequestError: If there's a network error.
        """
        headers: Dict[str, str] = {
            "X-API-TOKEN": self.api_key
        }

        url = f"{self.base_url}/narrative/{narrative_id}"

        async with httpx.AsyncClient() as client:
            response = await client.delete(url, headers=headers)

            # Raise an exception for non-2xx status codes
            response.raise_for_status()

            return response.json()

    async def health_check(self) -> bool:
        """
        Check if the external API is reachable.

        Returns:
            True if the API is reachable, False otherwise.
        """
        try:
            async with httpx.AsyncClient() as client:
                # Attempt a HEAD request to the base URL
                headers: Dict[str, str] = {"X-API-Key": self.api_key}
                response = await client.head(
                    self.base_url,
                    headers=headers,
                    timeout=5.0
                )
                return response.status_code < 500
        except (httpx.RequestError, httpx.TimeoutException):
            return False


class NarrativesAPIError(Exception):
    """Base exception for Narratives API errors."""
    pass


class NarrativeNotFoundError(NarrativesAPIError):
    """Raised when a narrative is not found in the external system."""
    pass


class AuthenticationError(NarrativesAPIError):
    """Raised when authentication fails with the external API."""
    pass


async def delete_narrative_from_external_api(narrative_id: int) -> Dict[str, Any]:
    """
    Convenience function to delete a narrative from the external API.

    Args:
        narrative_id: The backend ID of the narrative to delete.

    Returns:
        A dictionary containing the response from the API.

    Raises:
        NarrativeNotFoundError: If the narrative is not found.
        AuthenticationError: If the API key is invalid.
        NarrativesAPIError: For other API errors.
    """
    client = NarrativesAPIClient()

    try:
        result = await client.delete_narrative(narrative_id)
        return result
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise NarrativeNotFoundError(f"Narrative with ID {narrative_id} not found") from e
        elif e.response.status_code == 401:
            raise AuthenticationError("Invalid API key") from e
        else:
            raise NarrativesAPIError(f"API request failed: {e.response.text}") from e
    except httpx.RequestError as e:
        raise NarrativesAPIError(f"Network error: {str(e)}") from e
from typing import Any
from uuid import UUID

from litestar import Controller, Request, delete, get
from litestar.di import Provide
from litestar.params import Parameter
from litestar.status_codes import HTTP_204_NO_CONTENT

from core.auth.models import AuthToken, Identity
from core.errors import OrganisationIDRequiredError
from core.integrations.slack.models import SlackInstallationResponse
from core.integrations.slack.service import SlackService
from core.response import JSON
from core.uow import ConnectionFactory


async def slack_service(
    connection_factory: ConnectionFactory,
) -> SlackService:
    return SlackService(connection_factory=connection_factory)


class SlackController(Controller):
    path = "/slack"
    tags = ["integrations", "slack"]

    dependencies = {
        "slack_service": Provide(slack_service),
    }

    @get(
        path="/install-url",
        summary="Get Slack installation link",
    )
    async def get_install_url(
        self,
        request: Request[Identity, AuthToken, Any],
        slack_service: SlackService,
    ) -> JSON[dict[str, str]]:
        """
        Generate a Slack installation URL for the authenticated user's organisation.
        """
        if not request.user.organisation:
            raise OrganisationIDRequiredError(
                "User must be associated with an organisation"
            )

        url = await slack_service.generate_slack_auth_url(
            organisation_id=request.user.organisation.id
        )
        return JSON({"install_url": url})

    @get(
        path="/oauth/callback",
        summary="Handle Slack OAuth callback",
        exclude_from_auth=True,
    )
    async def oauth_callback(
        self,
        slack_service: SlackService,
        code: str,
        oauth_state: str = Parameter(query="state"),
    ) -> str:
        """
        Handle Slack OAuth callback. This endpoint is called by Slack after the user
        authorizes the app. The organisation_id is recovered from the state parameter.
        """
        try:
            await slack_service.process_oauth_callback(code=code, state=oauth_state)
            return "Installation successful! You can close this window."
        except ValueError as e:
            return f"Installation failed: {str(e)}"
        except Exception as e:
            return f"An unexpected error occurred. Please, contact support."

    @get(
        path="/installations",
        summary="Get all Slack installations for the user's organisation",
    )
    async def get_slack_installations(
        self,
        request: Request[Identity, AuthToken, Any],
        slack_service: SlackService,
    ) -> JSON[list[dict[str, Any]]]:
        """
        Get all Slack installation details for the authenticated user's organisation.
        An organisation can have multiple installations (one per Slack workspace).
        Returns installation info without sensitive credentials (bot_token excluded).
        """
        if not request.user.organisation:
            raise OrganisationIDRequiredError(
                "User must be associated with an organisation"
            )

        installations = await slack_service.get_installations_by_organisation(
            request.user.organisation.id
        )

        # Convert all installations to response models, excluding sensitive fields
        response = [
            SlackInstallationResponse.from_installation(installation).model_dump()
            for installation in installations
        ]
        return JSON(response)

    @delete(
        path="/installations/{installation_id:uuid}",
        summary="Delete a Slack installation",
        status_code=HTTP_204_NO_CONTENT,
    )
    async def delete_slack_installation(
        self,
        request: Request[Identity, AuthToken, Any],
        slack_service: SlackService,
        installation_id: UUID,
    ) -> None:
        """
        Delete a Slack installation for the authenticated user's organisation.
        This will:
        1. Revoke the bot token via Slack API (deactivates bot, removes channel memberships)
        2. Delete the installation record from the database

        The bot will lose access to all channels and the incoming webhook will stop working.
        """
        if not request.user.organisation:
            raise OrganisationIDRequiredError(
                "User must be associated with an organisation"
            )

        await slack_service.delete_installation(
            organisation_id=request.user.organisation.id,
            installation_id=installation_id,
        )

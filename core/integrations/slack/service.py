from uuid import UUID

from slack_sdk.oauth import AuthorizeUrlGenerator
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.web import WebClient

from core.config import SLACK_CLIENT_ID, SLACK_CLIENT_SECRET, SLACK_REDIRECT_URI
from core.integrations.slack.models import SlackInstallation
from core.integrations.slack.repo import SlackRepository
from core.uow import ConnectionFactory, uow


# Build https://slack.com/oauth/v2/authorize with sufficient query parameters
authorize_url_generator = AuthorizeUrlGenerator(
    client_id=SLACK_CLIENT_ID,
    scopes=["incoming-webhook", "chat:write"],
    redirect_uri=SLACK_REDIRECT_URI,
)


class SlackService:
    def __init__(self, connection_factory: ConnectionFactory):
        self.connection_factory = connection_factory

    async def generate_slack_auth_url(self, organisation_id: UUID) -> str:
        """
        Generate Slack installation URL with a temporary OAuth state.

        Args:
            organisation_id: The organisation UUID to associate with this installation

        Returns:
            The authorization URL for the user to visit
        """
        # Generate a random value and store it in the database
        async with uow(SlackRepository, self.connection_factory) as repo:
            state = await repo.issue_state(
                organisation_id=organisation_id, expiration_seconds=300
            )

        # https://slack.com/oauth/v2/authorize?state=(generated value)&client_id={client_id}&scope=incoming-webhook%2Cchat:write
        url = authorize_url_generator.generate(state)
        return url

    async def process_oauth_callback(self, code: str, state: str) -> SlackInstallation:
        """
        Process Slack OAuth callback and save the installation.

        Args:
            code: OAuth authorization code from Slack
            state: OAuth state parameter for CSRF protection

        Returns:
            The saved SlackInstallation

        Raises:
            ValueError: If the state is invalid or expired
        """
        # Validate and consume the state, getting the organisation_id back
        async with uow(SlackRepository, self.connection_factory) as repo:
            organisation_id = await repo.consume_state(state)

        if organisation_id is None:
            raise ValueError("Invalid or expired state parameter")

        client = WebClient()  # no prepared token needed for this
        # Complete the installation by calling oauth.v2.access API method
        oauth_response = client.oauth_v2_access(
            client_id=SLACK_CLIENT_ID,
            client_secret=SLACK_CLIENT_SECRET,
            redirect_uri=SLACK_REDIRECT_URI,
            code=code,
        )

        installed_enterprise = oauth_response.get("enterprise") or {}
        is_enterprise_install = oauth_response.get("is_enterprise_install")
        installed_team = oauth_response.get("team") or {}
        installer = oauth_response.get("authed_user") or {}
        incoming_webhook = oauth_response.get("incoming_webhook") or {}
        bot_token = oauth_response.get("access_token")

        # NOTE: oauth.v2.access doesn't include bot_id in response
        bot_id = None
        enterprise_url = None
        if bot_token is not None:
            auth_test = client.auth_test(token=bot_token)
            bot_id = auth_test["bot_id"]
            if is_enterprise_install is True:
                enterprise_url = auth_test.get("url")

        # Create our SlackInstallation model
        installation = SlackInstallation(
            organisation_id=organisation_id,
            app_id=oauth_response.get("app_id"),
            enterprise_id=installed_enterprise.get("id"),
            enterprise_name=installed_enterprise.get("name"),
            enterprise_url=enterprise_url,
            team_id=installed_team.get("id"),
            team_name=installed_team.get("name"),
            bot_token=bot_token,
            bot_id=bot_id,
            bot_user_id=oauth_response.get("bot_user_id"),
            bot_scopes=oauth_response.get("scope"),  # comma-separated string
            user_id=installer.get("id"),
            user_token=installer.get("access_token"),
            user_scopes=installer.get("scope"),  # comma-separated string
            incoming_webhook_url=incoming_webhook.get("url"),
            incoming_webhook_channel=incoming_webhook.get("channel"),
            incoming_webhook_channel_id=incoming_webhook.get("channel_id"),
            incoming_webhook_configuration_url=incoming_webhook.get(
                "configuration_url"
            ),
            is_enterprise_install=is_enterprise_install or False,
            token_type=oauth_response.get("token_type"),
        )

        # Store the installation in the database
        async with uow(SlackRepository, self.connection_factory) as repo:
            saved_installation = await repo.save_installation(installation)

        return saved_installation

    async def send_message_to_slack(
        self, organisation_id: UUID, channel: str, text: str
    ) -> None:
        """
        Send a message to a Slack channel using the stored bot token.

        Args:
            organisation_id: The organisation UUID whose Slack integration to use
            channel: The Slack channel name or ID to post to
            text: The message text to send

        Raises:
            ValueError: If no installation is found for the organisation
        """
        # Find the installations for this organisation
        async with uow(SlackRepository, self.connection_factory) as repo:
            installations = await repo.find_installations_by_organisation(
                organisation_id
            )

        if not installations:
            raise ValueError(
                f"No Slack installation found for organisation {organisation_id}"
            )

        # Use the first installation (most recent)
        installation = installations[0]

        # Send the message using the bot token
        client = AsyncWebClient(token=installation.bot_token)
        response = await client.chat_postMessage(channel=channel, text=text)
        
        if not response["ok"]:
            raise Exception(f"Slack API error: {response.get('error', 'Unknown error')}")

    async def get_installations_by_organisation(
        self, organisation_id: UUID
    ) -> list[SlackInstallation]:
        """
        Get all Slack installations for an organisation.
        An organisation can have multiple installations (one per Slack workspace).

        Args:
            organisation_id: The organisation UUID

        Returns:
            List of SlackInstallations (empty list if none found)
        """
        async with uow(SlackRepository, self.connection_factory) as repo:
            return await repo.find_installations_by_organisation(organisation_id)

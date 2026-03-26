from typing import Any
from uuid import UUID

from slack_sdk.errors import SlackApiError
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
    scopes=["incoming-webhook", "chat:write", "channels:join", "groups:write"],
    user_scopes=["channels:write", "groups:write"],  # User scopes needed for inviting bot
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
        Also ensures the bot has access to the incoming webhook channel.

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
        user_token = installer.get("access_token")

        # NOTE: oauth.v2.access doesn't include bot_id in response
        bot_id = None
        enterprise_url = None
        if bot_token is not None:
            auth_test = client.auth_test(token=bot_token)
            bot_id = auth_test["bot_id"]
            if is_enterprise_install is True:
                enterprise_url = auth_test.get("url")

        # Ensure bot has access to the incoming webhook channel
        incoming_webhook_channel_id = incoming_webhook.get("channel_id")
        bot_user_id = oauth_response.get("bot_user_id")
        
        if bot_token and incoming_webhook_channel_id and bot_user_id:
            await self._ensure_bot_in_channel(
                bot_token=bot_token,
                user_token=user_token,
                channel_id=incoming_webhook_channel_id,
                bot_user_id=bot_user_id,
            )

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
            bot_user_id=bot_user_id,
            bot_scopes=oauth_response.get("scope"),  # comma-separated string
            user_id=installer.get("id"),
            user_token=user_token,
            user_scopes=installer.get("scope"),  # comma-separated string
            incoming_webhook_url=incoming_webhook.get("url"),
            incoming_webhook_channel=incoming_webhook.get("channel"),
            incoming_webhook_channel_id=incoming_webhook_channel_id,
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

    async def _ensure_bot_in_channel(
        self,
        bot_token: str,
        user_token: str | None,
        channel_id: str,
        bot_user_id: str,
    ) -> None:
        """
        Ensure the bot has access to a Slack channel during installation.
        Tries to join (public channels) or be invited (private channels).

        Args:
            bot_token: The bot token for API calls
            user_token: The user token for inviting the bot (optional)
            channel_id: The Slack channel ID
            bot_user_id: The bot's user ID for invitation

        Raises:
            Exception: If bot cannot access the channel
        """
        bot_client = AsyncWebClient(token=bot_token)

        # Check if bot is already a member
        try:
            info_response = await bot_client.conversations_info(channel=channel_id)
            if info_response["ok"] and info_response["channel"].get("is_member"):
                return  # Already a member, nothing to do
        except Exception:
            pass  # Continue to join/invite

        # Strategy 1: Try bot self-join (works for public channels)
        try:
            join_response = await bot_client.conversations_join(channel=channel_id)
            if join_response["ok"]:
                return  # Successfully joined
        except SlackApiError as join_error:
            # Strategy 2: If self-join failed (likely private channel), try to invite bot
            if join_error.response.get("error") == "channel_not_found":
                if not user_token:
                    # Can't invite to private channel without user token, but that's okay
                    # User can manually invite later if needed
                    return

                try:
                    # Use user token to invite the bot to the private channel
                    user_client = AsyncWebClient(token=user_token)
                    await user_client.conversations_invite(
                        channel=channel_id,
                        users=bot_user_id
                    )
                    return  # Successfully invited
                except Exception:
                    # If invitation fails, it's okay - user can invite manually later
                    pass

    async def send_message_to_slack(
        self, organisation_id: UUID, channel: str, text: str
    ) -> None:
        """
        Send a message to a Slack channel using the stored bot token.
        Looks up the installation directly by channel ID for efficient workspace resolution.

        Args:
            organisation_id: The organisation UUID (for validation)
            channel: The Slack channel ID to post to
            text: The message text to send

        Raises:
            ValueError: If no installation is found for the channel or organisation mismatch
            Exception: If message sending fails
        """
        # Find the installation for this specific channel
        async with uow(SlackRepository, self.connection_factory) as repo:
            installation = await repo.find_installation_by_channel_id(channel)

        if not installation:
            raise ValueError(
                f"No Slack installation found for channel {channel}. "
                f"Make sure the Slack integration has been installed for this channel's workspace."
            )

        # Verify the installation belongs to the correct organisation
        if installation.organisation_id != organisation_id:
            raise ValueError(
                f"Channel {channel} belongs to a different organisation"
            )

        # Send the message using the installation's bot token
        bot_client = AsyncWebClient(token=installation.bot_token)

        try:
            response = await bot_client.chat_postMessage(channel=channel, text=text)

            if not response["ok"]:
                error = response.get('error', 'Unknown error')
                raise Exception(f"Slack API error: {error}")
                
        except Exception as e:
            raise Exception(
                f"Failed to send Slack message to channel '{channel}': {str(e)}. "
                f"Make sure the bot has been invited to this channel."
            )

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

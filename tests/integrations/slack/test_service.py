from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from pytest import raises

from core.auth.models import Organisation
from core.integrations.slack.models import SlackInstallation, SlackInstallationResponse
from core.integrations.slack.repo import SlackRepository
from core.integrations.slack.service import SlackService
from core.uow import uow
from tests.integrations.slack.conftest import SlackInstallationFactory


async def test_issue_state(conn_factory, slack_organisation: Organisation) -> None:
    """Test that we can issue an OAuth state"""
    async with uow(SlackRepository, conn_factory) as repo:
        state = await repo.issue_state(
            slack_organisation.id, expiration_seconds=300
        )
    assert state
    assert len(state) > 20  # Should be a long random string


async def test_consume_valid_state(conn_factory, slack_organisation: Organisation) -> None:
    """Test that we can consume a valid state and get the organisation_id back"""
    async with uow(SlackRepository, conn_factory) as repo:
        state = await repo.issue_state(
            slack_organisation.id, expiration_seconds=300
        )

    async with uow(SlackRepository, conn_factory) as repo:
        organisation_id = await repo.consume_state(state)
    assert organisation_id == slack_organisation.id

    # State should be consumed (deleted) after use
    async with uow(SlackRepository, conn_factory) as repo:
        organisation_id_again = await repo.consume_state(state)
    assert organisation_id_again is None


async def test_consume_expired_state(
    conn_factory, slack_organisation: Organisation
) -> None:
    """Test that expired states cannot be consumed"""
    # Create a state that's already expired
    async with uow(SlackRepository, conn_factory) as repo:
        state = await repo.issue_state(
            slack_organisation.id, expiration_seconds=-10
        )  # Negative = already expired

    async with uow(SlackRepository, conn_factory) as repo:
        organisation_id = await repo.consume_state(state)
    assert organisation_id is None


async def test_consume_invalid_state(conn_factory) -> None:
    """Test that invalid states cannot be consumed"""
    async with uow(SlackRepository, conn_factory) as repo:
        organisation_id = await repo.consume_state("invalid-state-12345")
    assert organisation_id is None


async def test_save_installation(
    conn_factory, slack_organisation: Organisation
) -> None:
    """Test saving a new installation"""
    installation = SlackInstallationFactory.build(
        organisation_id=slack_organisation.id,
        team_id="T12345678",
        team_name="Test Workspace",
        bot_token="xoxb-test-token",
    )

    async with uow(SlackRepository, conn_factory) as repo:
        saved = await repo.save_installation(installation)

    assert saved.id is not None
    assert saved.organisation_id == slack_organisation.id
    assert saved.team_id == "T12345678"
    assert saved.bot_token == "xoxb-test-token"
    assert saved.created_at is not None


async def test_update_existing_installation(
    conn_factory, slack_installation: SlackInstallation
) -> None:
    """Test updating an existing installation (same org + channel)"""
    original_id = slack_installation.id

    # Create an "updated" installation with same org and channel_id
    # This simulates reinstalling the app on the same channel
    updated = SlackInstallationFactory.build(
        organisation_id=slack_installation.organisation_id,
        incoming_webhook_channel_id=slack_installation.incoming_webhook_channel_id,
        team_id=slack_installation.team_id,  # Could be same or different workspace
        team_name="Updated Workspace Name",
        bot_token="xoxb-new-token",
    )

    async with uow(SlackRepository, conn_factory) as repo:
        saved = await repo.save_installation(updated)

    # Should have same ID (updated, not inserted)
    assert saved.id == original_id
    assert saved.team_name == "Updated Workspace Name"
    assert saved.bot_token == "xoxb-new-token"


async def test_multiple_installations_per_organisation(
    conn_factory, slack_organisation: Organisation
) -> None:
    """Test that one organisation can have multiple installations (one per channel)"""
    # Create first installation for channel 1
    installation1 = SlackInstallationFactory.build(
        organisation_id=slack_organisation.id,
        team_id="T11111111",
        bot_token="xoxb-token-1",
        incoming_webhook_channel_id="C11111111",
    )

    # Create second installation for channel 2 (same or different workspace)
    installation2 = SlackInstallationFactory.build(
        organisation_id=slack_organisation.id,
        team_id="T22222222",
        bot_token="xoxb-token-2",
        incoming_webhook_channel_id="C22222222",
    )

    async with uow(SlackRepository, conn_factory) as repo:
        saved1 = await repo.save_installation(installation1)
        saved2 = await repo.save_installation(installation2)

    # Both should be saved with different IDs
    assert saved1.id != saved2.id
    assert saved1.incoming_webhook_channel_id == "C11111111"
    assert saved2.incoming_webhook_channel_id == "C22222222"


async def test_find_bot_by_team_id(
    conn_factory, slack_installation: SlackInstallation
) -> None:
    """Test finding an installation by team_id"""
    async with uow(SlackRepository, conn_factory) as repo:
        found = await repo.find_bot(team_id=slack_installation.team_id)

    assert found is not None
    assert found.id == slack_installation.id
    assert found.team_id == slack_installation.team_id


async def test_find_bot_not_found(conn_factory) -> None:
    """Test that finding a nonexistent installation returns None"""
    async with uow(SlackRepository, conn_factory) as repo:
        found = await repo.find_bot(team_id="T99999999")
    assert found is None


async def test_find_installation_by_organisation(
    conn_factory, slack_installation: SlackInstallation
) -> None:
    """Test finding installations by organisation_id"""
    async with uow(SlackRepository, conn_factory) as repo:
        found = await repo.find_installations_by_organisation(
            slack_installation.organisation_id
        )

    assert found is not None
    assert len(found) == 1
    assert found[0].id == slack_installation.id
    assert found[0].organisation_id == slack_installation.organisation_id


async def test_find_installation_by_organisation_not_found(conn_factory) -> None:
    """Test that finding by nonexistent organisation returns empty list"""
    async with uow(SlackRepository, conn_factory) as repo:
        found = await repo.find_installations_by_organisation(uuid4())
    assert found == []


async def test_delete_installation(
    conn_factory, slack_installation: SlackInstallation
) -> None:
    """Test deleting an installation"""
    async with uow(SlackRepository, conn_factory) as repo:
        await repo.delete_installation(
            slack_installation.organisation_id, slack_installation.team_id
        )

    # Should not be found after deletion
    async with uow(SlackRepository, conn_factory) as repo:
        found = await repo.find_installations_by_organisation(
            slack_installation.organisation_id
        )
    assert found == []


async def test_generate_slack_auth_url(
    slack_service: SlackService, slack_organisation: Organisation
) -> None:
    """Test generating a Slack OAuth URL"""
    url = await slack_service.generate_slack_auth_url(slack_organisation.id)

    assert url
    assert "slack.com/oauth/v2/authorize" in url
    assert "state=" in url
    assert "client_id=" in url
    assert "scope=" in url


async def test_process_oauth_callback(
    slack_service: SlackService, slack_organisation: Organisation
) -> None:
    """Test processing an OAuth callback with mocked Slack API"""
    # Generate a valid state
    url = await slack_service.generate_slack_auth_url(slack_organisation.id)
    # Extract state from URL
    state = url.split("state=")[1].split("&")[0]

    # Mock the Slack WebClient responses
    mock_oauth_response = {
        "ok": True,
        "access_token": "xoxb-mock-token",
        "app_id": "A12345",
        "team": {"id": "T12345", "name": "Test Team"},
        "enterprise": {},
        "authed_user": {"id": "U12345", "access_token": "xoxp-user-token"},
        "incoming_webhook": {
            "url": "https://hooks.slack.com/services/TEST",
            "channel": "#general",
            "channel_id": "C12345",
            "configuration_url": "https://test.slack.com/config",
        },
        "scope": "incoming-webhook",
        "token_type": "bot",
        "bot_user_id": "U12345BOT",
    }

    mock_auth_test_response = {
        "ok": True,
        "bot_id": "B12345",
    }

    with patch("core.integrations.slack.service.WebClient") as MockWebClient, \
         patch("core.integrations.slack.service.AsyncWebClient") as MockAsyncWebClient:
        mock_client = MagicMock()
        mock_client.oauth_v2_access.return_value = mock_oauth_response
        mock_client.auth_test.return_value = mock_auth_test_response
        MockWebClient.return_value = mock_client

        # Mock async client for _ensure_bot_in_channel
        mock_async_client = MagicMock()
        mock_async_client.conversations_join = AsyncMock(return_value={"ok": True})
        MockAsyncWebClient.return_value = mock_async_client

        installation = await slack_service.process_oauth_callback(
            code="mock-code", state=state
        )

        assert installation is not None
        assert installation.organisation_id == slack_organisation.id
        assert installation.bot_token == "xoxb-mock-token"
        assert installation.team_id == "T12345"
        assert installation.team_name == "Test Team"
        assert installation.bot_id == "B12345"


async def test_process_oauth_callback_invalid_state(
    slack_service: SlackService,
) -> None:
    """Test that invalid state raises an error"""
    with raises(ValueError, match="Invalid or expired state"):
        await slack_service.process_oauth_callback(
            code="mock-code", state="invalid-state"
        )


async def test_send_message_to_slack(
    slack_service: SlackService, slack_installation: SlackInstallation
) -> None:
    """Test sending a message to Slack"""
    with patch("core.integrations.slack.service.AsyncWebClient") as MockAsyncWebClient:
        mock_client = MagicMock()
        mock_response = {"ok": True}
        mock_client.chat_postMessage = AsyncMock(return_value=mock_response)
        MockAsyncWebClient.return_value = mock_client

        await slack_service.send_message_to_slack(
            organisation_id=slack_installation.organisation_id,
            channel=slack_installation.incoming_webhook_channel_id,
            text="Test message",
        )

        # Verify AsyncWebClient was called with correct token
        MockAsyncWebClient.assert_called_once_with(token=slack_installation.bot_token)

        # Verify chat_postMessage was called
        mock_client.chat_postMessage.assert_called_once_with(
            channel=slack_installation.incoming_webhook_channel_id, text="Test message"
        )


async def test_send_message_no_installation(
    slack_service: SlackService, slack_organisation: Organisation
) -> None:
    """Test that sending a message without an installation raises an error"""
    with raises(ValueError, match="No Slack installation found"):
        await slack_service.send_message_to_slack(
            organisation_id=slack_organisation.id,
            channel="#general",
            text="Test message",
        )


async def test_get_installations_by_organisation_single(
    slack_service: SlackService, slack_installation: SlackInstallation
) -> None:
    """Test getting all installations when there's only one"""
    installations = await slack_service.get_installations_by_organisation(
        slack_installation.organisation_id
    )

    assert len(installations) == 1
    assert installations[0].id == slack_installation.id


async def test_get_installations_by_organisation_multiple(
    conn_factory, slack_service: SlackService, slack_organisation: Organisation
) -> None:
    """Test getting multiple installations for the same organisation (different channels)"""
    # Create multiple installations for the same organisation with different channels
    from core.integrations.slack.repo import SlackRepository
    from core.uow import uow

    installation1 = SlackInstallationFactory.build(
        organisation_id=slack_organisation.id,
        team_id="T11111111",
        team_name="Workspace 1",
        bot_token="xoxb-token-1",
        incoming_webhook_channel_id="C11111111",
    )

    installation2 = SlackInstallationFactory.build(
        organisation_id=slack_organisation.id,
        team_id="T22222222",
        team_name="Workspace 2",
        bot_token="xoxb-token-2",
        incoming_webhook_channel_id="C22222222",
    )

    # Save both installations
    async with uow(SlackRepository, conn_factory) as repo:
        await repo.save_installation(installation1)
        await repo.save_installation(installation2)

    # Get all installations
    installations = await slack_service.get_installations_by_organisation(
        slack_organisation.id
    )

    assert len(installations) == 2
    channel_ids = {inst.incoming_webhook_channel_id for inst in installations}
    assert channel_ids == {"C11111111", "C22222222"}


async def test_get_installations_by_organisation_empty(
    slack_service: SlackService,
) -> None:
    """Test getting installations when none exist"""
    installations = await slack_service.get_installations_by_organisation(uuid4())
    assert installations == []


async def test_slack_installation_response_excludes_sensitive_fields(
    slack_installation: SlackInstallation,
) -> None:
    """Test that SlackInstallationResponse excludes sensitive credentials"""
    # Create a response from the installation
    response = SlackInstallationResponse.from_installation(slack_installation)

    # Verify public fields are present
    assert response.id == slack_installation.id
    assert response.organisation_id == slack_installation.organisation_id
    assert response.team_id == slack_installation.team_id
    assert response.team_name == slack_installation.team_name
    assert response.bot_id == slack_installation.bot_id
    assert response.incoming_webhook_channel == slack_installation.incoming_webhook_channel

    # Verify response model doesn't have sensitive fields
    response_dict = response.model_dump()
    assert "bot_token" not in response_dict
    assert "user_token" not in response_dict
    assert "incoming_webhook_url" not in response_dict
    assert "incoming_webhook_configuration_url" not in response_dict

    # Verify the installation still has the sensitive fields (unchanged)
    assert slack_installation.bot_token == "xoxb-test-token"
    assert slack_installation.incoming_webhook_url is not None

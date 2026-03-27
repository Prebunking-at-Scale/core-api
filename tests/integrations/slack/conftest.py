from uuid import uuid4

from polyfactory.factories.pydantic_factory import ModelFactory
from polyfactory.field_meta import FieldMeta
from pytest import fixture

from core.auth.models import Organisation
from core.auth.service import AuthService
from core.integrations.slack.models import SlackInstallation
from core.integrations.slack.repo import SlackRepository
from core.integrations.slack.service import SlackService
from core.uow import uow
from tests.auth.conftest import create_organisation


@fixture
def tables_to_truncate() -> list[str]:
    return [
        "slack_installations",
        "slack_oauth_states",
        "organisations",
    ]


class SlackInstallationFactory(ModelFactory[SlackInstallation]):
    __check_model__ = True

    @classmethod
    def should_set_none_value(cls, field_meta: FieldMeta) -> bool:
        # Set optional fields to None by default for cleaner test data
        optional_fields = {
            "team_name",
            "enterprise_id",
            "enterprise_name",
            "enterprise_url",
            "app_id",
            "bot_id",
            "bot_user_id",
            "bot_scopes",
            "user_id",
            "user_token",
            "user_scopes",
            "incoming_webhook_url",
            "incoming_webhook_channel",
            "incoming_webhook_channel_id",
            "incoming_webhook_configuration_url",
            "token_type",
            "created_at",
            "updated_at",
        }
        if field_meta.name in optional_fields:
            return True

        return super().should_set_none_value(field_meta)


@fixture
async def slack_service(conn_factory) -> SlackService:
    return SlackService(conn_factory)


@fixture
async def slack_organisation(conn_factory) -> Organisation:
    auth_service = AuthService(conn_factory)
    return await create_organisation(auth_service)


@fixture
async def slack_installation(
    conn_factory, slack_organisation: Organisation
) -> SlackInstallation:
    """Create a test Slack installation"""
    installation = SlackInstallationFactory.build(
        organisation_id=slack_organisation.id,
        team_id=f"T{uuid4().hex[:10].upper()}",
        team_name="Test Workspace",
        bot_token="xoxb-test-token",
        incoming_webhook_url="https://hooks.slack.com/services/TEST/WEBHOOK/URL",
        incoming_webhook_channel="#general",
        incoming_webhook_channel_id=f"C{uuid4().hex[:10].upper()}",
    )

    # Save using the repository through UoW pattern
    async with uow(SlackRepository, conn_factory) as repo:
        return await repo.save_installation(installation)

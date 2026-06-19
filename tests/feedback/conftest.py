from pytest import fixture

from core.auth.models import Organisation
from core.auth.service import AuthService
from tests.auth.conftest import OrganisationFactory


@fixture
def tables_to_truncate() -> list[str]:
    # Note: users/organisations are intentionally NOT truncated — the api@pas
    # seed user (migration 10) backs X-API-TOKEN auth used by api_key_client.
    # Truncating narrative_feedback + narratives is enough to isolate these tests.
    return [
        "narrative_feedback",
        "narratives",
        "entities",
        "narrative_topics",
        "narrative_entities",
        "claim_narratives",
    ]


@fixture
async def auth_service(conn_factory) -> AuthService:
    return AuthService(conn_factory)


@fixture
async def organisation(auth_service: AuthService) -> Organisation:
    return await auth_service.create_organisation(
        OrganisationFactory.build(deactivated=None)
    )

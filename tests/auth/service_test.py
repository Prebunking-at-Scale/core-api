import uuid

from pytest import raises

from core import auth
from core.auth.models import Organisation, User
from core.auth.service import AuthService
from core.errors import NotAuthorizedError, NotFoundError
from tests.auth.conftest import OrganisationFactory


async def test_create_organisation(auth_service: AuthService) -> None:
    organisation = OrganisationFactory.build()
    created = await auth_service.create_organisation(organisation)
    assert created == organisation


async def test_get_organisation(
    auth_service: AuthService, organisation: Organisation
) -> None:
    fetched = await auth_service.get_organisation(organisation.id)
    assert fetched == organisation


async def test_get_missing_organisation(auth_service: AuthService) -> None:
    with raises(NotFoundError):
        await auth_service.get_organisation(uuid.uuid4())


async def test_invite_user(
    auth_service: AuthService, organisation: Organisation
) -> None:
    await auth_service.invite_token(
        organisation_id=organisation.id,
        email="james.mcminn@fullfact.org",
        as_admin=True,
        auto_accept=False,
    )


async def test_password_reset(auth_service: AuthService, user: User) -> None:
    token = await auth_service.password_reset_token(user.email)
    assert token is not None


async def test_deactivate_organisation(
    auth_service: AuthService, organisation: Organisation
) -> None:
    fetched_org = await auth_service.get_organisation(organisation.id)
    assert fetched_org.deactivated is None

    await auth_service.deactivate_organisation(organisation.id)
    fetched_org = await auth_service.get_organisation(organisation.id)
    assert fetched_org.deactivated is not None


async def test_deactivate_organisation_no_membership(
    auth_service: AuthService,
    organisation: Organisation,
    user: User,
) -> None:
    fetched_org = await auth_service.get_organisation(organisation.id)
    assert fetched_org.deactivated is None

    async with auth_service.repo() as repo:
        org, role = await repo.organisation_and_role(user.id, organisation.id)
        assert org == organisation

    await auth_service.deactivate_organisation(organisation.id)
    fetched_org = await auth_service.get_organisation(organisation.id)
    assert fetched_org.deactivated is not None

    with raises(NotAuthorizedError):
        async with auth_service.repo() as repo:
            await repo.organisation_and_role(user.id, organisation.id)

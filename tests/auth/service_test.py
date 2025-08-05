import uuid

from pytest import raises

from core.auth.models import Organisation, User
from core.auth.service import AuthService
from core.errors import NotFoundError
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
    new_password = "new-password-test"
    token = await auth_service.password_reset_token(user.email)
    # await auth_service.update_password()

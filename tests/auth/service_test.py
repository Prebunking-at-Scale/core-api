import uuid

from pytest import raises

from core.auth.models import AuthToken, Organisation, User
from core.auth.service import AuthService
from core.errors import (
    ConflictError,
    InvalidInviteError,
    NotAuthorizedError,
    NotFoundError,
)
from tests.auth.conftest import OrganisationFactory, create_organisation


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


async def test_add_organisation(auth_service: AuthService) -> None:
    org_to_create = Organisation(
        id=uuid.uuid4(),
        display_name="Test",
        country_codes=["GBR"],
        language="en",
        short_name="test",
    )
    returned_org = await auth_service.create_organisation(org_to_create)
    assert org_to_create == returned_org

    fetched_org = await auth_service.get_organisation(org_to_create.id)
    assert org_to_create == fetched_org


async def test_update_organisation(
    auth_service: AuthService, organisation: Organisation
) -> None:
    organisation.display_name += " test"
    organisation.language = "fr"
    organisation.country_codes += ["GBR"]
    updated = await auth_service.update_organisation(organisation.id, organisation)
    assert updated == organisation

    fetched = await auth_service.get_organisation(organisation.id)
    assert fetched == organisation


async def test_invite_user(
    auth_service: AuthService, organisation: Organisation
) -> None:
    token = await auth_service.invite_token(
        organisation_id=organisation.id,
        email="james.mcminn@fullfact.org",
        as_admin=True,
        auto_accept=False,
    )
    assert token

    options = await auth_service.accept_invite(token)
    assert options.first_time_setup
    assert len(options.organisations) == 1
    assert organisation.id in options.organisations

    login_token = options.organisations[organisation.id].token
    auth_token = AuthToken.decode(
        login_token,
        secret=auth_service.jwt_auth.token_secret,
        algorithm=auth_service.jwt_auth.algorithm,
    )
    assert auth_token.organisation_id == str(organisation.id)


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
        org, admin = await repo.organisation_and_role(user.id, organisation.id)
        assert org == organisation
        assert not admin

    await auth_service.deactivate_organisation(organisation.id)
    fetched_org = await auth_service.get_organisation(organisation.id)
    assert fetched_org.deactivated is not None

    with raises(NotAuthorizedError):
        async with auth_service.repo() as repo:
            await repo.organisation_and_role(user.id, organisation.id)


async def test_password_reset_token(auth_service: AuthService, user: User) -> None:
    token = await auth_service.password_reset_token(user.email)
    assert token

    auth_token = AuthToken.decode(
        token,
        secret=auth_service.jwt_auth.token_secret,
        algorithm=auth_service.jwt_auth.algorithm,
    )
    assert auth_token.is_password_reset


async def test_login_correct_details(auth_service: AuthService, user: User) -> None:
    password = "password123"
    await auth_service.update_password(user, password)

    login = await auth_service.login(user.email, password)
    assert len(login.organisations) == 1


async def test_login_incorrect_details(auth_service: AuthService, user: User) -> None:
    password = "password123"
    await auth_service.update_password(user, password)

    with raises(NotAuthorizedError):
        await auth_service.login(user.email, "wrong password")


async def test_login_no_organisations(
    auth_service: AuthService, user: User, organisation: Organisation
) -> None:
    password = "password123"
    await auth_service.update_password(user, password)
    await auth_service.deactivate_organisation(organisation.id)

    with raises(NotAuthorizedError):
        await auth_service.login(user.email, password)


async def test_login_multiple_organisations(
    auth_service: AuthService, user: User, organisation: Organisation
) -> None:
    password = "password123"
    await auth_service.update_password(user, password)
    login = await auth_service.login(user.email, password)
    assert len(login.organisations) == 1

    for _ in range(5):
        additional_org = await create_organisation(auth_service)
        await auth_service.invite_token(additional_org.id, user.email, False, True)

    login = await auth_service.login(user.email, password)
    assert len(login.organisations) == 6


async def test_remove_membership(
    auth_service: AuthService, user: User, organisation: Organisation
) -> None:
    password = "password123"
    await auth_service.update_password(user, password)

    login = await auth_service.login(user.email, password)
    assert len(login.organisations) == 1

    await auth_service.remove_user(user.id, organisation.id)
    with raises(NotAuthorizedError):
        await auth_service.login(user.email, password)


async def test_cant_accept_invite_after_removal(
    auth_service: AuthService, organisation: Organisation
) -> None:
    email = "james.mcminn@fullfact.org"

    token = await auth_service.invite_token(
        organisation_id=organisation.id,
        email=email,
        as_admin=True,
        auto_accept=False,
    )
    assert token

    user = await auth_service.get_user_by_email(email)
    assert user

    await auth_service.remove_user(user.id, organisation.id)
    with raises(InvalidInviteError):
        await auth_service.accept_invite(token)


async def test_can_re_invite_after_removal(
    auth_service: AuthService,
    user: User,
    organisation: Organisation,
) -> None:
    await auth_service.remove_user(user.id, organisation.id)

    token = await auth_service.invite_token(
        organisation_id=organisation.id,
        email=user.email,
        as_admin=True,
        auto_accept=False,
    )
    assert token
    await auth_service.accept_invite(token)

    password = "password123"
    await auth_service.update_password(user, password)

    login = await auth_service.login(user.email, password)
    assert len(login.organisations) == 1


async def test_resend_invite_token(
    auth_service: AuthService, organisation: Organisation
) -> None:
    email = "test@example.com"

    # First invite
    token1 = await auth_service.invite_token(
        organisation_id=organisation.id,
        email=email,
        as_admin=False,
        auto_accept=False,
    )
    assert token1

    # Resend invite
    token2 = await auth_service.resend_invite_token(
        organisation_id=organisation.id,
        email=email,
    )
    assert token2

    options = await auth_service.accept_invite(token2)
    assert options.user.email == email
    assert len(options.organisations) == 1


async def test_resend_invite_already_accepted_fails(
    auth_service: AuthService, organisation: Organisation
) -> None:
    email = "test@example.com"

    # Auto-accept invite
    await auth_service.invite_token(
        organisation_id=organisation.id,
        email=email,
        as_admin=False,
        auto_accept=True,
    )

    with raises(ConflictError, match="user has already accepted the invite"):
        await auth_service.resend_invite_token(
            organisation_id=organisation.id,
            email=email,
        )

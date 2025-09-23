from collections.abc import AsyncIterator

from litestar import Litestar
from litestar.testing import AsyncTestClient
from pytest import fixture
from testing.postgresql import Postgresql

import core.app as app
from core.auth.models import (
    AdminStatus,
    Organisation,
    OrganisationInvite,
    SuperAdminStatus,
    User,
)
from core.auth.service import AuthService
from tests.auth.conftest import OrganisationFactory, UserFactory, create_user


def get_access_token(login_options) -> str:
    """Helper to extract access token from login options"""
    return list(login_options.organisations.values())[0].token


async def create_user_with_password(
    auth_service: AuthService,
    organisation: Organisation,
    as_admin: bool = False,
    email: str | None = None,
    password: str = "password123456",
    is_super_admin: bool = False,
    **kwargs,
) -> tuple[User, str]:
    if email is None:
        user_factory = UserFactory.build(is_super_admin=is_super_admin, **kwargs)
        email = user_factory.email

    invite_token = await auth_service.invite_token(
        organisation_id=organisation.id,
        email=email,
        as_admin=as_admin,
        auto_accept=False,
    )
    assert invite_token is not None

    login_options = await auth_service.accept_invite(invite_token)
    user = login_options.user

    if is_super_admin:
        await auth_service.set_super_admin(user.id, True)
        user = await auth_service.get_user_by_email(user.email)
        assert user is not None

    await auth_service.update_password(user, password, None)
    return user, password


async def test_login_success(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, password = await create_user_with_password(
        auth_service, organisation, email="test@example.com"
    )

    response = await auth_client.post(
        "/api/auth/login",
        json={"email": user.email, "password": password},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert "organisations" in data
    assert data["user"]["email"] == user.email


async def test_login_invalid_credentials(
    auth_client: AsyncTestClient[Litestar],
) -> None:
    response = await auth_client.post(
        "/api/auth/login",
        json={"email": "nonexistent@example.com", "password": "wrongpassword123"},
    )
    assert response.status_code == 401


async def test_get_identity(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, password = await create_user_with_password(auth_service, organisation)
    login_options = await auth_service.login(user.email, password)

    response = await auth_client.get(
        "/api/auth/identity",
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["user"]["email"] == user.email
    assert data["organisation"]["id"] == str(organisation.id)


async def test_password_reset_request(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, _ = await create_user_with_password(auth_service, organisation)

    response = await auth_client.post(
        "/api/auth/request-password-reset",
        params={"email": user.email, "locale": "en"},
    )

    assert response.status_code == 200


async def test_password_reset_request_nonexistent_user(
    auth_client: AsyncTestClient[Litestar],
) -> None:
    response = await auth_client.post(
        "/api/auth/request-password-reset",
        params={"email": "nonexistent@example.com"},
    )

    assert response.status_code == 200


async def test_password_update(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, password = await create_user_with_password(auth_service, organisation)
    login_options = await auth_service.login(user.email, password)

    response = await auth_client.patch(
        "/api/auth/user/password",
        json={"new_password": "newpassword123456"},
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 200


async def test_get_user(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, password = await create_user_with_password(auth_service, organisation)
    login_options = await auth_service.login(user.email, password)

    response = await auth_client.get(
        "/api/auth/user",
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["email"] == user.email


async def test_update_user(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, password = await create_user_with_password(auth_service, organisation)
    login_options = await auth_service.login(user.email, password)

    update_data = {"display_name": "Updated Name"}

    response = await auth_client.patch(
        "/api/auth/user",
        json=update_data,
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["display_name"] == "Updated Name"


async def test_create_organisation_as_super_admin(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    super_admin, password = await create_user_with_password(
        auth_service, organisation, is_super_admin=True
    )
    login_options = await auth_service.login(super_admin.email, password)

    new_org = OrganisationFactory.build()

    response = await auth_client.post(
        "/api/auth/organisation",
        json=new_org.model_dump(mode="json"),
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 201
    data = response.json()["data"]
    assert data["display_name"] == new_org.display_name


async def test_create_organisation_as_org_admin_fails(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    org_admin, password = await create_user_with_password(
        auth_service, organisation, as_admin=True
    )
    login_options = await auth_service.login(org_admin.email, password)

    new_org = OrganisationFactory.build()

    response = await auth_client.post(
        "/api/auth/organisation",
        json=new_org.model_dump(mode="json"),
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 401


async def test_create_organisation_as_regular_user_fails(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, password = await create_user_with_password(auth_service, organisation)
    login_options = await auth_service.login(user.email, password)

    new_org = OrganisationFactory.build()

    response = await auth_client.post(
        "/api/auth/organisation",
        json=new_org.model_dump(mode="json"),
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 401


async def test_update_organisation_as_admin(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    admin, password = await create_user_with_password(
        auth_service, organisation, as_admin=True
    )
    login_options = await auth_service.login(admin.email, password)

    update_data = {"display_name": "Updated Organisation Name"}

    response = await auth_client.patch(
        "/api/auth/organisation",
        json=update_data,
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["display_name"] == "Updated Organisation Name"


async def test_update_organisation_as_regular_user_fails(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, password = await create_user_with_password(auth_service, organisation)
    login_options = await auth_service.login(user.email, password)

    update_data = {"display_name": "Updated Organisation Name"}

    response = await auth_client.patch(
        "/api/auth/organisation",
        json=update_data,
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 401


async def test_get_organisation(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, password = await create_user_with_password(auth_service, organisation)
    login_options = await auth_service.login(user.email, password)

    response = await auth_client.get(
        "/api/auth/organisation",
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["id"] == str(organisation.id)


async def test_remove_user_as_admin(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    admin, password = await create_user_with_password(
        auth_service, organisation, as_admin=True
    )
    user_to_remove = await create_user(auth_service, organisation, False)
    login_options = await auth_service.login(admin.email, password)

    response = await auth_client.delete(
        f"/api/auth/organisation/users/{user_to_remove.id}",
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 204


async def test_remove_user_as_regular_user_fails(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, password = await create_user_with_password(auth_service, organisation)
    user_to_remove = await create_user(auth_service, organisation, False)
    login_options = await auth_service.login(user.email, password)

    response = await auth_client.delete(
        f"/api/auth/organisation/users/{user_to_remove.id}",
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 401


async def test_set_admin_status_as_admin(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    admin, password = await create_user_with_password(
        auth_service, organisation, as_admin=True
    )
    user = await create_user(auth_service, organisation, False)
    login_options = await auth_service.login(admin.email, password)

    admin_status = AdminStatus(is_admin=True)

    response = await auth_client.patch(
        f"/api/auth/organisation/users/{user.id}/admin",
        json=admin_status.model_dump(mode="json"),
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 200


async def test_set_admin_status_as_regular_user_fails(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, password = await create_user_with_password(auth_service, organisation)
    target_user = await create_user(auth_service, organisation, False)
    login_options = await auth_service.login(user.email, password)

    admin_status = AdminStatus(is_admin=True)

    response = await auth_client.patch(
        f"/api/auth/organisation/users/{target_user.id}/admin",
        json=admin_status.model_dump(mode="json"),
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 401


async def test_set_super_admin_status_as_super_admin(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    super_admin, password = await create_user_with_password(
        auth_service, organisation, is_super_admin=True
    )
    target_user = await create_user(auth_service, organisation, False)
    login_options = await auth_service.login(super_admin.email, password)

    # First set the user as super admin using the service method directly
    await auth_service.set_super_admin(super_admin.id, True)

    super_admin_status = SuperAdminStatus(is_super_admin=True)

    response = await auth_client.patch(
        f"/api/auth/users/{target_user.id}/super-admin",
        json=super_admin_status.model_dump(mode="json"),
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 200


async def test_set_super_admin_status_as_regular_user_fails(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, password = await create_user_with_password(auth_service, organisation)
    target_user = await create_user(auth_service, organisation, False)
    login_options = await auth_service.login(user.email, password)

    super_admin_status = SuperAdminStatus(is_super_admin=True)

    response = await auth_client.patch(
        f"/api/auth/users/{target_user.id}/super-admin",
        json=super_admin_status.model_dump(mode="json"),
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 401


async def test_invite_user_as_admin(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    admin, password = await create_user_with_password(
        auth_service, organisation, as_admin=True
    )
    login_options = await auth_service.login(admin.email, password)

    invite_data = OrganisationInvite(user_email="newuser@example.com", as_admin=False)

    response = await auth_client.post(
        "/api/auth/organisation/invite",
        json=invite_data.model_dump(mode="json"),
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )

    assert response.status_code == 201


async def test_invite_user_as_regular_user_fails(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, password = await create_user_with_password(auth_service, organisation)
    login_options = await auth_service.login(user.email, password)

    invite_data = OrganisationInvite(user_email="newuser@example.com", as_admin=False)

    response = await auth_client.post(
        "/api/auth/organisation/invite",
        json=invite_data.model_dump(mode="json"),
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 401


async def test_join_organisation_with_valid_token(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    _ = await create_user_with_password(auth_service, organisation, as_admin=True)
    invite_token = await auth_service.invite_token(
        organisation_id=organisation.id,
        email="newuser@example.com",
        as_admin=False,
        auto_accept=False,
    )
    assert invite_token is not None

    response = await auth_client.get(
        "/api/auth/organisation/invite/accept",
        params={"invite_token": invite_token},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert "organisations" in data


async def test_join_organisation_with_invalid_token(
    auth_client: AsyncTestClient[Litestar],
) -> None:
    response = await auth_client.get(
        "/api/auth/organisation/invite/accept",
        params={"invite_token": "invalid_token"},
    )
    assert response.status_code == 500


async def test_organisation_users(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    user, password = await create_user_with_password(auth_service, organisation)
    admin = await create_user(auth_service, organisation, True)

    # Create an invited user who hasn't accepted yet
    invite_token = await auth_service.invite_token(
        organisation_id=organisation.id,
        email="invited@example.com",
        as_admin=False,
        auto_accept=False,
    )
    assert invite_token is not None

    login_options = await auth_service.login(user.email, password)

    response = await auth_client.get(
        "/api/auth/organisation/users",
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) == 3

    # Check that we have both accepted and invited users
    user_data_by_email = {user_data["email"]: user_data for user_data in data}

    # Accepted users should have accepted timestamp
    assert user.email in user_data_by_email
    assert user_data_by_email[user.email]["accepted"] is not None
    assert admin.email in user_data_by_email
    assert user_data_by_email[admin.email]["accepted"] is not None

    # Invited user should have invited timestamp but no accepted timestamp
    assert "invited@example.com" in user_data_by_email
    invited_user_data = user_data_by_email["invited@example.com"]
    assert invited_user_data["invited"] is not None
    assert invited_user_data["accepted"] is None


async def test_organisation_users_admin_status(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    # Create regular user
    regular_user, password = await create_user_with_password(auth_service, organisation)

    # Create admin user
    admin_user = await create_user(auth_service, organisation, True)

    # Create invited admin user who hasn't accepted yet
    invite_token = await auth_service.invite_token(
        organisation_id=organisation.id,
        email="invitedadmin@example.com",
        as_admin=True,
        auto_accept=False,
    )
    assert invite_token is not None

    login_options = await auth_service.login(regular_user.email, password)

    response = await auth_client.get(
        "/api/auth/organisation/users",
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) == 3

    user_data_by_email = {user_data["email"]: user_data for user_data in data}

    # Regular user should not be admin
    assert user_data_by_email[regular_user.email]["is_admin"] is False

    # Admin user should be admin
    assert user_data_by_email[admin_user.email]["is_admin"] is True

    # Invited admin user should be admin even though not accepted yet
    assert user_data_by_email["invitedadmin@example.com"]["is_admin"] is True


# Tests for super admin organisation override functionality


async def test_super_admin_organisation_override_success(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    """Test that super admin can switch organisation context using organisation_id parameter"""
    other_org = await auth_service.create_organisation(OrganisationFactory.build())

    super_admin, password = await create_user_with_password(
        auth_service, organisation, is_super_admin=True
    )
    login_options = await auth_service.login(super_admin.email, password)
    token = get_access_token(login_options)

    response = await auth_client.get(
        f"/api/auth/identity?organisation_id={other_org.id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["user"]["email"] == super_admin.email
    assert data["organisation"]["id"] == str(other_org.id)
    assert data["is_organisation_admin"] is True


async def test_regular_user_organisation_override_fails(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    """Test that regular users cannot use organisation override even with valid UUID"""
    other_org = await auth_service.create_organisation(OrganisationFactory.build())

    user, password = await create_user_with_password(auth_service, organisation)
    login_options = await auth_service.login(user.email, password)
    token = get_access_token(login_options)

    response = await auth_client.get(
        f"/api/auth/identity?organisation_id={other_org.id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 401


async def test_organisation_admin_override_fails(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    """Test that organisation admins cannot override to different organisations"""
    other_org = await auth_service.create_organisation(OrganisationFactory.build())

    org_admin, password = await create_user_with_password(
        auth_service, organisation, as_admin=True
    )
    login_options = await auth_service.login(org_admin.email, password)
    token = get_access_token(login_options)

    response = await auth_client.get(
        f"/api/auth/identity?organisation_id={other_org.id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 401


async def test_super_admin_override_with_deactivated_organisation_fails(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    """Test that super admin cannot override to deactivated organisation"""
    other_org = await auth_service.create_organisation(OrganisationFactory.build())
    await auth_service.deactivate_organisation(other_org.id)

    super_admin, password = await create_user_with_password(
        auth_service, organisation, is_super_admin=True
    )
    login_options = await auth_service.login(super_admin.email, password)
    token = get_access_token(login_options)

    response = await auth_client.get(
        f"/api/auth/identity?organisation_id={other_org.id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 401


async def test_super_admin_override_with_nonexistent_organisation_fails(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    """Test that super admin cannot override to non-existent organisation"""
    from uuid import uuid4

    super_admin, password = await create_user_with_password(
        auth_service, organisation, is_super_admin=True
    )
    login_options = await auth_service.login(super_admin.email, password)
    token = get_access_token(login_options)

    fake_org_id = uuid4()
    response = await auth_client.get(
        f"/api/auth/identity?organisation_id={fake_org_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 401


async def test_super_admin_override_preserves_functionality(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    """Test that super admin can perform admin actions in overridden organisation context"""
    other_org = await auth_service.create_organisation(OrganisationFactory.build())
    other_user = await create_user(auth_service, other_org, False)

    super_admin, password = await create_user_with_password(
        auth_service, organisation, is_super_admin=True
    )
    login_options = await auth_service.login(super_admin.email, password)
    token = get_access_token(login_options)

    update_data = {"display_name": "Updated by Super Admin"}
    response = await auth_client.patch(
        f"/api/auth/organisation?organisation_id={other_org.id}",
        json=update_data,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["display_name"] == "Updated by Super Admin"

    admin_status = AdminStatus(is_admin=True)
    response = await auth_client.patch(
        f"/api/auth/organisation/users/{other_user.id}/admin?organisation_id={other_org.id}",
        json=admin_status.model_dump(mode="json"),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200


async def test_list_organisations_as_super_admin(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    """Test that super admin can list all organisations"""
    # Create additional organisations to test listing
    org2 = await auth_service.create_organisation(OrganisationFactory.build())
    org3 = await auth_service.create_organisation(OrganisationFactory.build())

    super_admin, password = await create_user_with_password(
        auth_service, organisation, is_super_admin=True
    )
    login_options = await auth_service.login(super_admin.email, password)

    response = await auth_client.get(
        "/api/auth/organisations",
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) == 3

    # Check that all organisations are present
    org_ids = {org["id"] for org in data}
    assert str(organisation.id) in org_ids
    assert str(org2.id) in org_ids
    assert str(org3.id) in org_ids


async def test_list_organisations_as_org_admin_fails(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    """Test that organisation admin cannot list all organisations"""
    org_admin, password = await create_user_with_password(
        auth_service, organisation, as_admin=True
    )
    login_options = await auth_service.login(org_admin.email, password)

    response = await auth_client.get(
        "/api/auth/organisations",
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 401


async def test_list_organisations_as_regular_user_fails(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    """Test that regular user cannot list all organisations"""
    user, password = await create_user_with_password(auth_service, organisation)
    login_options = await auth_service.login(user.email, password)

    response = await auth_client.get(
        "/api/auth/organisations",
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 401


async def test_list_organisations_excludes_deactivated(
    auth_client: AsyncTestClient[Litestar],
    auth_service: AuthService,
    organisation: Organisation,
) -> None:
    """Test that deactivated organisations are not returned"""
    # Create additional organisation and deactivate it
    org2 = await auth_service.create_organisation(OrganisationFactory.build())
    await auth_service.deactivate_organisation(org2.id)

    super_admin, password = await create_user_with_password(
        auth_service, organisation, is_super_admin=True
    )
    login_options = await auth_service.login(super_admin.email, password)

    response = await auth_client.get(
        "/api/auth/organisations",
        headers={"Authorization": f"Bearer {get_access_token(login_options)}"},
    )
    assert response.status_code == 200
    data = response.json()["data"]

    # Should only contain the active organisation
    assert len(data) == 1
    assert data[0]["id"] == str(organisation.id)

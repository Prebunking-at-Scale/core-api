from typing import Annotated, Any
from uuid import UUID

from litestar import Controller, Request, Response, delete, get, patch, post
from litestar.background_tasks import BackgroundTask
from litestar.datastructures import State
from litestar.di import Provide
from litestar.dto import DTOData
from pydantic import Field

from core.auth.guards import organisation_admin, super_admin
from core.auth.models import (
    AdminStatus,
    AuthToken,
    Identity,
    Login,
    LoginOptions,
    Organisation,
    OrganisationCreateDTO,
    OrganisationInvite,
    OrganisationUpdateDTO,
    PasswordChange,
    SuperAdminStatus,
    User,
    UserUpdateDTO,
)
from core.auth.service import AuthService
from core.email import Emailer, messages
from core.errors import NotAuthorizedError
from core.response import JSON


async def send_invite_email(
    emailer: Emailer, to: str, organisation: Organisation, token: str
) -> None:
    subject, body = messages.invite_message(
        organisation.display_name, token, organisation.language
    )
    emailer.send(to, subject, body)


async def send_password_reset_email(
    emailer: Emailer, to: str, locale: str, token: str
) -> None:
    subject, body = messages.password_reset_message(token, locale)
    emailer.send(to, subject, body)


async def auth_service(state: State) -> AuthService:
    return AuthService(connection_factory=state.connection_factory)


class AuthController(Controller):
    path = "/auth"

    dependencies = {
        "auth_service": Provide(auth_service),
    }

    @post(
        path="/login",
        summary="Login to the system",
        exclude_from_auth=True,
        tags=["auth"],
        raises=[NotAuthorizedError],
        status_code=200,
    )
    async def login(self, auth_service: AuthService, data: Login) -> JSON[LoginOptions]:
        return JSON(
            await auth_service.login(data.email, data.password.get_secret_value())
        )

    @get(
        path="/identity",
        summary="Get user, organisation and role information for the current session",
        tags=["auth"],
    )
    async def identity(
        self,
        request: Request[Identity, AuthToken, Any],
    ) -> JSON[Identity]:
        return JSON(request.user)

    @post(
        path="/request-password-reset",
        summary="Request an email to reset the users password",
        exclude_from_auth=True,
        tags=["auth"],
        status_code=200,
    )
    async def password_reset(
        self,
        emailer: Emailer,
        auth_service: AuthService,
        email: str,
        locale: str = "en",
    ) -> Response[None]:
        token = await auth_service.password_reset_token(email)
        if not token:
            return Response(None)

        return Response(
            None,
            background=BackgroundTask(
                send_password_reset_email,
                emailer,
                email,
                locale,
                token,
            ),
        )

    @patch(
        path="/user/password",
        summary="Update the current users password",
        tags=["users"],
    )
    async def password_update(
        self,
        request: Request[Identity, AuthToken, Any],
        auth_service: AuthService,
        user: User,
        data: PasswordChange,
    ) -> None:
        last_update_before = None
        if request.auth.is_password_reset:
            last_update_before = request.auth.iat

        await auth_service.update_password(
            user, data.new_password.get_secret_value(), last_update_before
        )

    @get(
        path="/user",
        summary="Get details of the logged in user",
        tags=["users"],
    )
    async def get_user(self, user: User) -> JSON[User]:
        return JSON(user)

    @patch(
        path="/user",
        summary="Update details for the logged in user",
        dto=UserUpdateDTO,
        return_dto=None,
        tags=["users"],
    )
    async def update_user(
        self, auth_service: AuthService, user: User, data: DTOData[User]
    ) -> JSON[User]:
        return JSON(await auth_service.update_user(user, data))

    @post(
        path="/organisation",
        guards=[super_admin],
        summary="Create a new organisation",
        dto=OrganisationCreateDTO,
        return_dto=None,
        tags=["organisations"],
    )
    async def create_organisation(
        self, auth_service: AuthService, data: Organisation
    ) -> JSON[Organisation]:
        return JSON(await auth_service.create_organisation(data))

    @patch(
        path="/organisation",
        guards=[organisation_admin],
        summary="Update an organisation",
        dto=OrganisationUpdateDTO,
        return_dto=None,
        tags=["organisations"],
    )
    async def update_organisation(
        self,
        auth_service: AuthService,
        organisation: Organisation,
        data: DTOData[Organisation],
    ) -> JSON[Organisation]:
        return JSON(await auth_service.update_organisation(organisation.id, data))

    @get(
        path="/organisation",
        summary="Get details about the organisation of the currently logged in user",
        tags=["organisations"],
    )
    async def get_organisation(
        self, auth_service: AuthService, organisation: Organisation
    ) -> JSON[Organisation]:
        return JSON(await auth_service.get_organisation(organisation.id))

    @delete(
        path="/organisation/users/{user_id:uuid}",
        guards=[organisation_admin],
        summary="Remove a user from an organisation",
        tags=["organisations"],
    )
    async def remove_user(
        self,
        auth_service: AuthService,
        organisation: Organisation,
        user_id: UUID,
    ) -> None:
        await auth_service.remove_user(
            user_id=user_id,
            organisation_id=organisation.id,
        )

    @patch(
        path="/organisation/users/{user_id:uuid}/admin",
        guards=[organisation_admin],
        summary="Set the users admin status",
        tags=["organisations"],
    )
    async def set_admin_status(
        self,
        auth_service: AuthService,
        organisation: Organisation,
        user_id: UUID,
        data: AdminStatus,
    ) -> None:
        await auth_service.set_admin(
            organisation_id=organisation.id,
            user_id=user_id,
            is_admin=data.is_admin,
        )

    @patch(
        path="/users/{user_id:uuid}/super-admin",
        guards=[super_admin],
        summary="Set the users super admin status",
        tags=["users"],
    )
    async def set_super_admin_status(
        self,
        auth_service: AuthService,
        user_id: UUID,
        data: SuperAdminStatus,
    ) -> None:
        await auth_service.set_super_admin(
            user_id=user_id,
            is_super_admin=data.is_super_admin,
        )

    @post(
        path="/organisation/invite",
        guards=[organisation_admin],
        summary="Invite a someone to join an organisation",
        tags=["organisations"],
    )
    async def invite_user(
        self,
        emailer: Emailer,
        auth_service: AuthService,
        organisation: Organisation,
        data: OrganisationInvite,
    ) -> Response[None]:
        token = await auth_service.invite_token(
            organisation_id=organisation.id,
            email=data.user_email,
            as_admin=data.as_admin,
            auto_accept=False,
        )
        if not token:
            raise Exception("expected token but got None")

        return Response(
            None,
            background=BackgroundTask(
                send_invite_email,
                emailer,
                data.user_email,
                organisation,
                token,
            ),
        )

    @post(
        path="/organisation/invite/resend",
        guards=[organisation_admin],
        summary="Resend an invite to a user",
        tags=["organisations"],
    )
    async def resend_invite(
        self,
        emailer: Emailer,
        auth_service: AuthService,
        organisation: Organisation,
        data: OrganisationInvite,
    ) -> Response[None]:
        token = await auth_service.resend_invite_token(
            organisation_id=organisation.id,
            email=data.user_email,
        )
        if not token:
            raise Exception("expected token but got None")

        return Response(
            None,
            background=BackgroundTask(
                send_invite_email,
                emailer,
                data.user_email,
                organisation,
                token,
            ),
        )

    @get(
        path="/organisation/invite/accept",
        summary="Accept an invitation to join an organisation",
        exclude_from_auth=True,
        tags=["organisations"],
    )
    async def join_organisation(
        self,
        auth_service: AuthService,
        invite_token: Annotated[
            str, Field(description="A JWT containing invite claims")
        ],
    ) -> JSON[LoginOptions]:
        return JSON(await auth_service.accept_invite(invite_token))

    @get(
        path="/organisation/users",
        summary="List the users for an organisation",
        tags=["organisations"],
    )
    async def organisation_users(
        self,
        auth_service: AuthService,
        organisation: Organisation,
    ) -> JSON[list[User]]:
        return JSON(await auth_service.organisation_users(organisation.id))

from typing import Any
from uuid import UUID

from litestar import Controller, get, patch, post
from litestar.datastructures import State
from litestar.di import Provide
from litestar.dto import DTOData
from litestar.exceptions import NotFoundException

from core.auth.models import Organisation, OrganisationInvite, User
from core.auth.service import AuthService
from core.errors import ConflictError
from core.response import JSON


async def auth_service(state: State) -> AuthService:
    return state.connection_factory


class AuthController(Controller):
    path = "/auth"

    dependencies = {
        "auth_service": Provide(auth_service),
    }

    @post(
        path="/login",
        summary="Login to the system",
        tags=["auth"],
    )
    async def login(self) -> JSON[None]:
        return JSON(None)

    @post(
        path="/token/refresh",
        summary="Get a new token using a refresh token",
        tags=["auth"],
    )
    async def refresh(self) -> JSON[None]:
        return JSON(None)

    @post(
        path="/user/request-password-reset",
        summary="Request an email to reset the users password",
        tags=["users"],
    )
    async def password_reset(self) -> JSON[None]:
        return JSON(None)

    @post(
        path="/user/password",
        summary="Update the current users password",
        tags=["users"],
    )
    async def password_update(self) -> JSON[None]:
        return JSON(None)

    @get(
        path="/user",
        summary="Get details of the specified or currently logged in user",
        tags=["users"],
    )
    async def get_user(self) -> JSON[None]:
        return JSON(None)

    @patch(
        path="/user",
        summary="Update details for the specified or currently logged in user",
        tags=["users"],
    )
    async def update_user(self) -> JSON[None]:
        return JSON(None)

    @post(
        path="/organisation",
        summary="Create a new organisation",
        tags=["organisations"],
    )
    async def create_organisation(
        self, auth_service: AuthService, data: Organisation
    ) -> JSON[Organisation]:
        return JSON(await auth_service.create_organisation(data))

    @patch(
        path="/organisation",
        summary="Update an organisation",
        tags=["organisations"],
    )
    async def update_organisation(self) -> JSON[None]:
        return JSON(None)

    @get(
        path="/organisation",
        summary="Get details about the organisation of the currently logged in user",
        tags=["organisations"],
    )
    async def get_organisation(
        self, auth_service: AuthService, organisation_id: UUID
    ) -> JSON[Organisation]:
        return JSON(await auth_service.get_organisation(organisation_id))

    @post(
        path="/organisation/invite",
        summary="Invite a someone to join an organisation",
        tags=["organisations"],
    )
    async def invite_user(
        self, auth_service: AuthService, data: OrganisationInvite
    ) -> None:
        auth_service.invite_user()

    @get(
        path="/organisation/accept",
        summary="Accept an invitation to join an organisation",
        tags=["organisations"],
    )
    async def join_organisation(self) -> JSON[None]:
        return JSON(None)

    @get(
        path="/organisation/users",
        summary="List the users for an organisation",
        tags=["organisations"],
    )
    async def organisation_users(self) -> JSON[list[User]]:
        return JSON(None)

    @patch(
        path="/organisation/users/admin",
        summary="Set the users admin status",
        tags=["organisations"],
    )
    async def set_admin_status(self) -> User:
        return JSON(None)

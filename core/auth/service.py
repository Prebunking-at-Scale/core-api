import logging
from datetime import datetime
from typing import AsyncContextManager
from uuid import UUID

import bcrypt
import jwt
from litestar.connection import ASGIConnection
from litestar.dto import DTOData
from litestar.security.jwt import JWTAuth

from core.auth.models import (
    AuthToken,
    Identity,
    LoginOptions,
    Organisation,
    OrganisationToken,
    User,
)
from core.auth.repo import AuthRepository
from core.config import AUTH_TOKEN_TTL, INVITE_TTL, JWT_SECRET, PASSWORD_RESET_TTL
from core.errors import ConflictError, NotAuthorizedError
from core.uow import ConnectionFactory, uow

log = logging.getLogger(__name__)


class AuthService:
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        self.connection_factory = connection_factory
        self.jwt_auth = JWTAuth[Identity](
            retrieve_user_handler=self.retrieve_jwt_identity,
            auth_header="Authorization",
            exclude_opt_key="exclude_from_auth",
            token_secret=JWT_SECRET,
            algorithm="HS256",
            token_cls=AuthToken,
            exclude=["/schema"],
        )

    def repo(self) -> AsyncContextManager[AuthRepository]:
        assert self.connection_factory is not None
        return uow(AuthRepository, self.connection_factory)

    async def retrieve_jwt_identity(
        self, token: AuthToken, connection: ASGIConnection
    ) -> Identity:
        async with self.repo() as repo:
            if token.is_api_user:
                user = await repo.get_user_by_email("api@pas")
            else:
                user = await repo.get_user_by_id(id=UUID(hex=token.sub))
            if not user:
                raise NotAuthorizedError("invalid user token")

            organisation, is_admin = None, False
            if token.organisation_id:
                organisation_id = UUID(hex=token.organisation_id)
                organisation, is_admin = await repo.organisation_and_role(
                    user.id, organisation_id
                )

                if organisation.deactivated is not None and not user.is_super_admin:
                    raise NotAuthorizedError("organisation has been deactivated")

            return Identity(
                user=user,
                organisation=organisation,
                is_organisation_admin=is_admin,
            )

    def _password_hash(self, password: str) -> bytes:
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password=password.encode(), salt=salt)

    async def login(self, email: str, password: str) -> LoginOptions:
        async with self.repo() as repo:
            hashed = await repo.get_password_hash(email)
            user = await repo.get_user_by_email(email)

            if not user or not bcrypt.checkpw(password.encode(), hashed):
                raise NotAuthorizedError("user does not exist or password incorrect")

            organisations, admin_status = await repo.organisation_memberships(user.id)

        if not organisations:
            raise NotAuthorizedError("user does not belong to any organisations")

        options = LoginOptions(user=user, organisations={})
        for org, is_admin in zip(organisations, admin_status):
            options.organisations[org.id] = OrganisationToken(
                organisation=org,
                token=self.jwt_auth.create_token(
                    identifier=str(user.id),
                    token_expiration=AUTH_TOKEN_TTL,
                    organisation_id=str(org.id),
                ),
                is_organisation_admin=is_admin,
            )

        return options

    async def create_organisation(self, organisation: Organisation) -> Organisation:
        async with self.repo() as repo:
            return await repo.add_organisation(organisation)

    async def get_organisation(self, organisation_id: UUID) -> Organisation:
        async with self.repo() as repo:
            return await repo.get_organisation(organisation_id)

    async def update_organisation(
        self, organisation_id: UUID, data: DTOData[Organisation] | Organisation
    ) -> Organisation:
        async with self.repo() as repo:
            if not isinstance(data, Organisation):
                organisation = await repo.get_organisation(organisation_id)
                updated = data.update_instance(organisation)
            else:
                updated = data
            return await repo.update_organisation(updated)

    async def deactivate_organisation(self, organisation_id: UUID) -> None:
        async with self.repo() as repo:
            await repo.deactivate_organisation(organisation_id)

    async def organisation_users(self, organisation_id: UUID) -> list[User]:
        async with self.repo() as repo:
            return await repo.organisation_users(organisation_id)

    async def invited_users(self, organisation_id: UUID) -> list[User]:
        async with self.repo() as repo:
            return await repo.invited_users(organisation_id)

    async def invite_token(
        self,
        organisation_id: UUID,
        email: str,
        as_admin: bool,
        auto_accept: bool = False,
    ) -> str | None:
        async with self.repo() as repo:
            organisation = await repo.get_organisation(organisation_id)
            if organisation.deactivated is not None:
                raise ConflictError("cannot invite user to deactivated organisation")

            email = email.strip()
            user = await repo.get_user_by_email(email)
            if not user:
                display_name = email.split("@", maxsplit=1)[0]
                user = await repo.add_user(User(display_name=display_name, email=email))

            await repo.invite_user_to_organisation(
                user_id=user.id,
                organisation_id=organisation_id,
                auto_accept=auto_accept,
                admin=as_admin,
            )

            if auto_accept:
                return None

            return self.jwt_auth.create_token(
                identifier=str(user.id),
                token_expiration=INVITE_TTL,
                organisation_id=str(organisation_id),
            )

    async def accept_invite(self, token: str) -> LoginOptions:
        decoded = jwt.decode(
            token,
            algorithms=[self.jwt_auth.algorithm],
            key=self.jwt_auth.token_secret,
        )

        async with self.repo() as repo:
            user = await repo.get_user_by_id(decoded.get("sub"))
            if not user:
                raise NotAuthorizedError()

            organisation_id = decoded.get("organisation_id")
            await repo.accept_invite(user.id, organisation_id)
            organisation, is_admin = await repo.organisation_and_role(
                user.id, organisation_id
            )

        return LoginOptions(
            user=user,
            organisations={
                organisation_id: OrganisationToken(
                    organisation=organisation,
                    token=self.jwt_auth.create_token(
                        identifier=str(user.id),
                        token_expiration=AUTH_TOKEN_TTL,
                        organisation_id=organisation_id,
                    ),
                    is_organisation_admin=is_admin,
                ),
            },
            first_time_setup=not user.password_last_updated,
        )

    async def remove_user(self, user_id: UUID, organisation_id: UUID) -> None:
        async with self.repo() as repo:
            await repo.deactivate_user(user_id, organisation_id)

    async def get_user_by_email(self, email: str) -> User | None:
        async with self.repo() as repo:
            return await repo.get_user_by_email(email)

    async def update_user(self, user: User, update: DTOData[User]) -> User:
        async with self.repo() as repo:
            undated_user = update.update_instance(user)
            return await repo.update_user(undated_user)

    async def set_admin(
        self, user_id: UUID, organisation_id: UUID, is_admin: bool
    ) -> None:
        async with self.repo() as repo:
            await repo.set_admin(user_id, organisation_id, is_admin)

    async def password_reset_token(self, email: str) -> str | None:
        async with self.repo() as repo:
            user = await repo.get_user_by_email(email)
            if not user:
                log.warning(
                    f"attempt to reset password for {email} but user does not exist"
                )
                return None

            return self.jwt_auth.create_token(
                identifier=str(user.id),
                token_expiration=PASSWORD_RESET_TTL,
                is_password_reset=True,
            )

    async def update_password(
        self, user: User, new_password: str, last_update_before: datetime | None = None
    ) -> None:
        async with self.repo() as repo:
            hash = self._password_hash(new_password)
            await repo.update_password_hash(user.id, hash, last_update_before)

from datetime import timedelta
from typing import AsyncContextManager
from uuid import UUID

import jwt
from litestar.dto import DTOData

from core.auth.jwt import jwt_auth
from core.auth.models import Organisation, User
from core.auth.repo import AuthRepository
from core.errors import ConflictError, NotAuthorizedError
from core.uow import ConnectionFactory, uow

INVITE_TTL = timedelta(days=7)


class AuthService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[AuthRepository]:
        return uow(AuthRepository, self._connection_factory)

    async def create_organisation(self, organisation: Organisation) -> Organisation:
        async with self.repo() as repo:
            return await repo.add_organisation(organisation)

    async def get_organisation(self, id: UUID) -> Organisation:
        async with self.repo() as repo:
            return await repo.get_organisation(id)

    async def update_organisation(
        self, id: UUID, data: DTOData[Organisation]
    ) -> Organisation:
        async with self.repo() as repo:
            organisation = await repo.get_organisation(id)
            updated = data.update_instance(organisation)
            return await repo.update_organisation(updated)

    async def invite_user(
        self,
        organisation_id: UUID,
        email: str,
        as_admin: bool,
        auto_accept: bool = False,
    ) -> None:
        async with self.repo() as repo:
            email = email.strip()
            user = await repo.get_user_by_email(email)

            organisation = await repo.get_organisation(organisation_id)
            if organisation.deactivated is not None:
                raise ConflictError("cannot invite user to deactivated organisation")

            if not user:
                user = await repo.add_user(User(display_name=email, email=email))
            await repo.invite_user_to_org(
                user_id=user.id,
                organisation_id=organisation_id,
                auto_accept=auto_accept,
                admin=as_admin,
            )

            token = jwt_auth.create_token(
                identifier=str(user.id),
                token_expiration=INVITE_TTL,
                token_extras={
                    "organisation": organisation.model_dump(
                        mode="json", exclude_none=True
                    )
                },
            )
            print(jwt.decode(token, options={"verify_signature": False}))
            # TODO: Send email with JTW link

    async def accept_invite(self, token: str) -> Organisation:
        decoded = jwt.decode(
            token, algorithms=[jwt_auth.algorithm], key=jwt_auth.token_secret
        )
        print(decoded)

        async with self.repo() as repo:
            user = await repo.get_user_by_id(decoded.get("sub"))
            if not user:
                raise NotAuthorizedError()

            organisation_id = decoded.get("organisation", {}).get("id")
            organisation = await repo.get_organisation(organisation_id)
            await repo.accept_invite(user.id, organisation.id)

        return organisation

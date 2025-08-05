from datetime import datetime
from typing import Any

from polyfactory.factories.pydantic_factory import ModelFactory
from polyfactory.field_meta import FieldMeta
from pytest import fixture

from core.auth.models import Organisation, User
from core.auth.service import AuthService


@fixture
def tables_to_truncate() -> list[str]:
    return [
        "organisation_users",
        "users",
        "organisations",
    ]


class OrganisationFactory(ModelFactory[Organisation]):
    __check_model__ = True

    @classmethod
    def should_set_none_value(cls, field_meta: FieldMeta) -> bool:
        if field_meta.name == "deactivated":
            return True

        return super().should_set_none_value(field_meta)


class UserFactory(ModelFactory[User]):
    __check_model__ = True


@fixture
async def auth_service(conn_factory) -> AuthService:
    return AuthService(conn_factory)


async def create_organisation(
    auth_service: AuthService,
    deactivated: datetime | None = None,
    **kwargs: Any,
) -> Organisation:
    return await auth_service.create_organisation(
        OrganisationFactory.build(deactivated=None, **kwargs)
    )


@fixture
async def organisation(auth_service: AuthService) -> Organisation:
    return await create_organisation(auth_service)


async def create_user(
    auth_service: AuthService, organisation: Organisation, as_admin: bool, **kwargs: Any
) -> User:
    user = UserFactory.build(**kwargs)
    await auth_service.invite_token(
        organisation_id=organisation.id,
        email=user.email,
        as_admin=as_admin,
        auto_accept=True,
    )
    user = await auth_service.get_user_by_email(user.email)
    assert user is not None
    return user


@fixture
async def user(auth_service: AuthService, organisation: Organisation) -> User:
    return await create_user(auth_service, organisation, False, is_super_admin=False)


@fixture
async def admin(auth_service: AuthService, organisation: Organisation) -> User:
    return await create_user(auth_service, organisation, True, is_super_admin=False)


@fixture
async def super_admin(auth_service: AuthService, organisation: Organisation) -> User:
    return await create_user(auth_service, organisation, False, is_super_admin=True)

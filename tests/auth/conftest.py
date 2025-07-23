from datetime import datetime
from typing import Any

from polyfactory.factories.pydantic_factory import ModelFactory
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


class UserFactory(ModelFactory[Organisation]):
    __check_model__ = True


@fixture
async def auth_service(conn_factory) -> AuthService:
    return AuthService(conn_factory)


async def create_organisation(
    auth_service: AuthService,
    deactivated: datetime | None = None,
    **kwargs: dict[str, Any],
) -> Organisation:
    return await auth_service.create_organisation(
        OrganisationFactory.build(deactivated=deactivated, kwargs=kwargs)
    )


@fixture
async def organisation(auth_service: AuthService) -> Organisation:
    return await create_organisation(auth_service)


# async def create_user(auth_service: AuthService, **kwargs: dict[str, Any]) -> User:
#     return await auth_service.create_user(OrganisationFactory.build(kwargs=kwargs))


# @fixture
# async def user(auth_service: AuthService) -> User:
#     return await create_user(auth_service)

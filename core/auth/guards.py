from litestar.connection import ASGIConnection
from litestar.exceptions import NotAuthorizedException
from litestar.handlers import BaseRouteHandler


def super_admin(connection: ASGIConnection, _: BaseRouteHandler) -> None:
    if not connection.user.user.is_super_admin:
        raise NotAuthorizedException()


def organisation_admin(connection: ASGIConnection, _: BaseRouteHandler) -> None:
    if not connection.user.is_organisation_admin:
        raise NotAuthorizedException()

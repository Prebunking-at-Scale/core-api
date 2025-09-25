from litestar.connection import ASGIConnection
from litestar.exceptions import NotAuthorizedException
from litestar.handlers import BaseRouteHandler


def api_only(connection: ASGIConnection, _: BaseRouteHandler) -> None:
    if not connection.auth.is_api_user:
        raise NotAuthorizedException()


def super_admin(connection: ASGIConnection, _: BaseRouteHandler) -> None:
    if not connection.user.user.is_super_admin:
        raise NotAuthorizedException()


def organisation_admin(connection: ASGIConnection, _: BaseRouteHandler) -> None:
    if not connection.user.is_organisation_admin:
        raise NotAuthorizedException()

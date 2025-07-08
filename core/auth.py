import json
import os

from litestar.connection import ASGIConnection
from litestar.exceptions import NotAuthorizedException
from litestar.handlers import BaseRouteHandler

API_KEY_HEADER = "X-API-TOKEN"
VALID_API_KEYS = json.loads(os.environ.get("API_KEYS", "[]"))


async def base_guard(
    connection: ASGIConnection, route_handler: BaseRouteHandler
) -> None:
    if connection.scope.get("method") == "OPTIONS":
        return  # We don't want to perform auth on OPTIONS requests

    if API_KEY_HEADER in connection.headers:
        await api_key_guard(connection, route_handler)

    # todo: add user validation
    else:
        raise NotAuthorizedException(
            detail="no authorisation method supplied", status_code=403
        )


async def api_key_guard(
    connection: ASGIConnection, route_handler: BaseRouteHandler
) -> None:
    if connection.headers.get(API_KEY_HEADER, "") in VALID_API_KEYS:
        return
    raise NotAuthorizedException(detail="could not validate API key", status_code=403)

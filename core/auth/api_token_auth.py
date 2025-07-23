import json
import os

from litestar.config.app import AppConfig
from litestar.datastructures import MutableScopeHeaders, URL
from litestar.exceptions import NotAuthorizedException
from litestar.middleware import ASGIMiddleware
from litestar.security.jwt import JWTAuth
from litestar.types import ASGIApp, Receive, Scope, Send

API_KEY_HEADER = "X-API-TOKEN"
VALID_API_KEYS = json.loads(os.environ.get("API_KEYS", "[]"))

# See https://www.encode.io/articles/working-with-http-requests-in-asgi
# for details on working with asgi requests


class APITokenAuthMiddleware(ASGIMiddleware):
    exclude_opt_key = "no_api_key_auth"

    def __init__(self, jwt_auth: JWTAuth):
        self.jwt_auth = jwt_auth

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        # Make sure to be the first middleware otherwise the jwt check
        # will fire before we are able to inject the header
        app_config.middleware.insert(0, self)
        return app_config

    async def handle(
        self, scope: Scope, receive: Receive, send: Send, next_app: ASGIApp
    ) -> None:
        headers = MutableScopeHeaders(scope=scope)
        url = URL.from_scope(scope=scope)
        subject = url.query_params.get("organisation_id", "*")
        api_key = headers.get(API_KEY_HEADER)
        if api_key:
            if api_key in VALID_API_KEYS:
                token = self.jwt_auth.create_token(identifier="*")
                headers["Authorization"] = "Bearer " + token
            else:
                raise NotAuthorizedException(
                    detail="could not validate API key", status_code=403
                )

        await next_app(scope, receive, send)

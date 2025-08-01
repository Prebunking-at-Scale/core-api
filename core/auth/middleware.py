import json
import os
from datetime import timedelta

from litestar.config.app import AppConfig
from litestar.datastructures import URL, MutableScopeHeaders
from litestar.exceptions import NotAuthorizedException
from litestar.middleware import ASGIMiddleware
from litestar.security.jwt import JWTAuth
from litestar.types import ASGIApp, Receive, Scope, Send

API_KEY_HEADER = "X-API-TOKEN"
VALID_API_KEYS = json.loads(os.environ.get("API_KEYS", "[]"))


class APITokenAuthMiddleware(ASGIMiddleware):
    """APITokenMiddleware intercepts any API keys, validates them, and creates a JWT
    token for the given organisation which is injected into the request and used by
    any handers"""

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
        organisation_id = url.query_params.get("organisation_id", "")
        api_key = headers.get(API_KEY_HEADER)
        if api_key:
            if api_key not in VALID_API_KEYS:
                raise NotAuthorizedException(
                    detail="could not validate API key", status_code=403
                )

            token = self.jwt_auth.create_token(
                identifier="api-user",
                token_expiration=timedelta(seconds=30),
                is_api_user=True,
                organisation_id=organisation_id,
            )
            headers["Authorization"] = "Bearer " + token

        await next_app(scope, receive, send)

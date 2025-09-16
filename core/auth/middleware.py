from datetime import timedelta

import jwt
from litestar.config.app import AppConfig
from litestar.datastructures import URL, MutableScopeHeaders
from litestar.exceptions import NotAuthorizedException
from litestar.middleware import ASGIMiddleware
from litestar.security.jwt import JWTAuth
from litestar.types import ASGIApp, Receive, Scope, Send

from core import config

API_KEY_HEADER = "X-API-TOKEN"


class AuthenticationMiddleware(ASGIMiddleware):
    """Authentication middleware that handles two authentication flows:

    1. API Key Authentication: Validates API keys from X-API-TOKEN header and
       creates JWT tokens for the specified organisation
    2. Super Admin Override: Allows super admins to switch organisation context
       via organisation_id query parameter by creating new JWT tokens with override flags
    """

    exclude_opt_key = "no_api_key_auth"

    def __init__(self, jwt_auth: JWTAuth, temp_token_lifetime=30):
        self.jwt_auth = jwt_auth
        self.temp_token_lifetime = temp_token_lifetime

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

        # Handle API key authentication
        if api_key:
            if api_key not in config.VALID_API_KEYS:
                raise NotAuthorizedException(
                    detail="could not validate API key", status_code=403
                )

            token = self.jwt_auth.create_token(
                identifier="api-user",
                token_expiration=timedelta(seconds=self.temp_token_lifetime),
                is_api_user=True,
                organisation_id=organisation_id,
            )
            headers["Authorization"] = "Bearer " + token

        # Handle super admin organisation_id override
        elif organisation_id:
            auth_header = headers.get("Authorization")
            if auth_header and auth_header.startswith("Bearer "):
                try:
                    token_str = auth_header[7:]
                    decoded = jwt.decode(
                        token_str,
                        algorithms=[self.jwt_auth.algorithm],
                        key=self.jwt_auth.token_secret,
                    )

                    new_token = self.jwt_auth.create_token(
                        identifier=decoded["sub"],
                        token_expiration=timedelta(seconds=self.temp_token_lifetime),
                        is_super_admin_override=True,
                        organisation_id=organisation_id,
                    )
                    headers["Authorization"] = "Bearer " + new_token
                except (ValueError, KeyError, jwt.InvalidTokenError):
                    pass

        await next_app(scope, receive, send)

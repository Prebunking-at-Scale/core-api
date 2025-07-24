import os

from litestar.security.jwt import JWTAuth, Token

from core.auth.models import User

jwt_auth = JWTAuth[User](
    retrieve_user_handler=lambda x, y: User(
        display_name="James",
        email="james.mcminn@fullfact.org",
        roles=[],
        active=True,
        verified=True,
    ),
    # guards=
    token_secret=os.environ.get("JWT_SECRET", "abcd123"),
    algorithm="HS256",
    # we are specifying which endpoints should be excluded from authentication. In this case the login endpoint
    # and our openAPI docs.
    exclude=["/api/auth/login", "/schema"],
)

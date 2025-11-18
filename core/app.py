from typing import Any

from litestar import Litestar, Router, get
from litestar.datastructures import State
from litestar.di import Provide
from litestar.openapi import OpenAPIConfig
from litestar.openapi.spec import Components, SecurityScheme
from litestar.plugins.structlog import StructlogPlugin
from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from core import config, email
from core.alerts.controller import AlertController
from core.auth import dependencies, middleware
from core.auth.controller import AuthController
from core.auth.service import AuthService
from core.entities.controller import EntityController
from core.languages.controller import LanguageController
from core.media_feeds.controller import MediaFeedController
from core.migrate import migrate
from core.narratives.controller import NarrativeController
from core.topics.controller import TopicController
from core.videos.claims.controller import ClaimController, RootClaimController
from core.videos.controller import VideoController
from core.videos.transcripts.controller import TranscriptController

MIGRATION_TARGET_VERSION = 13


postgres_url = f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"

auth_service = AuthService()
auth_middleware = middleware.AuthenticationMiddleware(auth_service.jwt_auth)


def pool_factory(url: str) -> AsyncConnectionPool[AsyncConnection[DictRow]]:
    return AsyncConnectionPool(
        url,
        open=False,
        max_size=10,
        connection_class=AsyncConnection[DictRow],
        kwargs={"row_factory": dict_row},
    )


async def setup_db(app: Litestar) -> None:
    app.state.connection_pool = pool_factory(postgres_url)
    await app.state.connection_pool.open()
    app.state.connection_factory = app.state.connection_pool.connection
    connection_factory: Any = app.state.connection_factory
    auth_service.connection_factory = connection_factory


async def shutdown_db(app: Litestar) -> None:
    await app.state.connection_pool.close()


async def perform_migrations(app: Litestar) -> None:
    await migrate(app.state.connection_factory, MIGRATION_TARGET_VERSION)


@get(
    "/",
    include_in_schema=False,
    exclude_from_auth=True,
)
async def hello_world() -> str:
    return "Hello, world!"


@get(
    "/health",
    include_in_schema=False,
    exclude_from_auth=True,
)
async def health(state: State) -> str:
    async with state.connection_factory() as conn:
        cur = await conn.execute(
            "SELECT version FROM migrations ORDER BY id DESC LIMIT 1"
        )
        await cur.close()
        return "ok"


api_router = Router(
    path="/api",
    guards=[],
    security=[
        {"APIToken": []},
        {"BearerToken": []},
    ],
    route_handlers=[
        AuthController,
        AlertController,
        VideoController,
        TranscriptController,
        ClaimController,
        RootClaimController,
        NarrativeController,
        TopicController,
        MediaFeedController,
        EntityController,
        LanguageController,
    ],
)


app: Litestar = Litestar(
    debug=config.DEV_MODE,
    on_app_init=[
        auth_service.jwt_auth.on_app_init,
        # Order is important so that auth_middleware can override jwt_auth's settings
        auth_middleware.on_app_init,
    ],
    route_handlers=[
        hello_world,
        health,
        api_router,
    ],
    on_startup=[
        setup_db,
        perform_migrations,
    ],
    middleware=[],
    dependencies={
        **dependencies.auth,
        "connection_factory": Provide(
            lambda: app.state.connection_factory, sync_to_thread=False
        ),
        "emailer": Provide(email.get_emailer),
    },
    on_shutdown=[
        shutdown_db,
    ],
    plugins=[
        StructlogPlugin(),
    ],
    openapi_config=OpenAPIConfig(
        title="PAS Core API",
        version="0.0.1",
        security=[{"BearerToken": []}],
        components=Components(
            security_schemes={
                "APIToken": SecurityScheme(
                    name="X-API-TOKEN",
                    type="apiKey",
                    security_scheme_in="header",
                ),
                "BearerToken": SecurityScheme(
                    type="http",
                    scheme="bearer",
                    security_scheme_in="header",
                ),
            },
        ),
    ),
)

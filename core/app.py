import os

from dotenv import load_dotenv
from litestar import Litestar, Router, get
from litestar.datastructures import State
from litestar.di import Provide
from litestar.openapi import OpenAPIConfig
from litestar.openapi.spec import Components, SecurityScheme
from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from core.auth import base_guard
from core.migrate import migrate
from core.videos.controller import VideoController
from core.videos.transcripts.controller import TranscriptController

load_dotenv()

MIGRATION_TARGET_VERSION = 2
DB_HOST = os.environ.get("DATABASE_HOST")
DB_PORT = os.environ.get("DATABASE_PORT")
DB_USER = os.environ.get("DATABASE_USER")
DB_PASSWORD = os.environ.get("DATABASE_PASSWORD")
DB_NAME = os.environ.get("DATABASE_NAME")

postgres_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def pool_factory(url: str) -> AsyncConnectionPool[AsyncConnection[DictRow]]:
    # ugly, but useful for testing
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


async def shutdown_db(app: Litestar) -> None:
    await app.state.connection_pool.close()


async def perform_migrations(app: Litestar) -> None:
    await migrate(app.state.connection_factory, MIGRATION_TARGET_VERSION)


@get("/", include_in_schema=False)
async def hello_world() -> str:
    return "Hello, world!"


@get("/health", include_in_schema=False)
async def health(state: State) -> str:
    async with state.connection_factory() as conn:
        cur = await conn.execute(
            "SELECT version FROM migrations ORDER BY id DESC LIMIT 1"
        )
        await cur.close()
        return "ok"


api_router = Router(
    path="/api",
    route_handlers=[VideoController, TranscriptController],
    guards=[base_guard],
    security=[{"APIToken": []}],
)


app: Litestar = Litestar(
    debug=True,
    route_handlers=[
        hello_world,
        health,
        api_router,
    ],
    on_startup=[
        setup_db,
        perform_migrations,
    ],
    dependencies={
        "connection_factory": Provide(
            lambda: app.state.connection_factory, sync_to_thread=False
        ),
    },
    on_shutdown=[
        shutdown_db,
    ],
    plugins=[],
    middleware=[],
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
                )
            },
        ),
    ),
)

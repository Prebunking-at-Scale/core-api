import os

from litestar import Litestar, get
from litestar.di import Provide
from litestar.openapi import OpenAPIConfig
from litestar.openapi.spec import Components, SecurityScheme
from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from core.migrate import migrate
from core.videos.controller import VideoController


MIGRATION_TARGET_VERSION = 1
DB_HOST = os.environ.get("DATABASE_HOST", "localhost")
DB_PORT = os.environ.get("DATABASE_PORT", "5433")
DB_USER = os.environ.get("DATABASE_USER", "pas")
DB_PASSWORD = os.environ.get("DATABASE_PASSWORD", "s3cret")
DB_NAME = os.environ.get("DATABASE_NAME", "pas")

if DB_PASSWORD:
    DB_PASSWORD = ":" + DB_PASSWORD

dsn = f"postgresql://{DB_USER}{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

pool = AsyncConnectionPool(
    dsn,
    open=False,
    max_size=10,
    connection_class=AsyncConnection[DictRow],
    kwargs={"row_factory": dict_row},
)


async def setup_db(app: Litestar) -> None:
    await pool.open()
    app.state.connection_factory = pool.connection


async def shutdown_db(app: Litestar) -> None:
    await pool.close()


async def perform_migrations(app: Litestar) -> None:
    await migrate(app.state.connection_factory, MIGRATION_TARGET_VERSION)


@get("/")
async def hello_world() -> str:
    return "Hello, world!"


app = Litestar(
    path="/api/",
    debug=True,
    route_handlers=[
        hello_world,
        VideoController,
    ],
    on_startup=[
        setup_db,
        perform_migrations,
    ],
    dependencies={
        "connection_factory": Provide(lambda: pool.connection, sync_to_thread=False),
    },
    on_shutdown=[shutdown_db],
    plugins=[],
    middleware=[],
    openapi_config=OpenAPIConfig(
        title="PAS Core API",
        version="0.0.1",
        security=[{"BearerToken": []}],
        components=Components(
            security_schemes={
                "BearerToken": SecurityScheme(
                    type="http",
                    scheme="bearer",
                )
            },
        ),
    ),
)

from collections.abc import AsyncIterator
from typing import Any, Generator

from litestar import Litestar
from litestar.testing import AsyncTestClient
from pytest import fixture
from testing.postgresql import Postgresql

import core.app as app
from core import auth

TEST_API_KEY = "abc123"

app.app.debug = True

auth.VALID_API_KEYS = [TEST_API_KEY]


@fixture(scope="module")
def temp_db() -> Generator[Postgresql, Any, None]:
    db = Postgresql()
    yield db
    db.stop()


@fixture()
def tables_to_truncate() -> list[str]:
    """Truncate a set of tables after each test.
    To be overridden by each module when needed."""
    return []


@fixture(scope="function")
async def api_key_client(
    temp_db: Postgresql, tables_to_truncate: list[str]
) -> AsyncIterator[AsyncTestClient[Litestar]]:
    app.postgres_url = temp_db.url()
    async with AsyncTestClient(app=app.app) as client:
        client.headers.setdefault(auth.API_KEY_HEADER, TEST_API_KEY)
        yield client

        if tables_to_truncate:
            async with app.app.state.connection_factory() as conn:
                for table in tables_to_truncate:
                    await conn.execute(f"TRUNCATE TABLE {table} CASCADE")

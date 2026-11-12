import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

from psycopg.errors import InvalidTextRepresentation
from pytest import MonkeyPatch, fixture, raises
from testing.postgresql import Postgresql

import core.app as app
import core.migrate
from core.migrate import migrate

ConnFactory = Callable[[], Any]


@fixture
async def empty_db(temp_db: Postgresql) -> AsyncIterator[ConnFactory]:
    """The migrations table with no migrations yet, and a table for the test
    migrations to write to."""
    pool = app.pool_factory(temp_db.url())
    await pool.open()
    await migrate(pool.connection, 0)
    async with pool.connection() as conn:
        await conn.execute("CREATE TABLE migration_runs (n int)")
    yield pool.connection
    async with pool.connection() as conn:
        await conn.execute("DROP TABLE IF EXISTS migration_runs, migrations")
    await pool.close()


async def scalar(conn_factory: ConnFactory, query: str) -> Any:
    async with conn_factory() as conn:
        cur = await conn.execute(query)
        row = await cur.fetchone()
        return next(iter(row.values()))


def use_script(monkeypatch: MonkeyPatch, script: bytes) -> None:
    monkeypatch.setattr(
        core.migrate,
        "_get_migration_script",
        lambda version, direction: {"test": script},
    )


async def test_concurrent_runs_apply_a_migration_once(
    empty_db: ConnFactory, monkeypatch: MonkeyPatch
) -> None:
    # Each worker of each pod runs the migrations when it starts. Like the real
    # migrations, the script has its own BEGIN and COMMIT.
    use_script(
        monkeypatch,
        b"BEGIN; INSERT INTO migration_runs VALUES (1); SELECT pg_sleep(0.2); COMMIT;",
    )

    await asyncio.gather(*(migrate(empty_db, 1) for _ in range(5)))

    assert await scalar(empty_db, "SELECT count(*) FROM migration_runs") == 1
    assert await scalar(empty_db, "SELECT count(*) FROM migrations") == 1


async def test_a_failed_migration_releases_the_lock(
    empty_db: ConnFactory, monkeypatch: MonkeyPatch
) -> None:
    use_script(
        monkeypatch,
        b"BEGIN; INSERT INTO migration_runs VALUES ('not a number'); COMMIT;",
    )
    with raises(InvalidTextRepresentation):
        await migrate(empty_db, 1)

    use_script(monkeypatch, b"BEGIN; INSERT INTO migration_runs VALUES (1); COMMIT;")
    await asyncio.wait_for(migrate(empty_db, 1), timeout=10)

    assert await scalar(empty_db, "SELECT count(*) FROM migration_runs") == 1

import logging
from typing import Callable, Literal
from pathlib import Path
import glob

import psycopg
from psycopg.rows import DictRow

log = logging.getLogger(__name__)


async def migrate(
    connection_factory: Callable[[], psycopg.AsyncConnection[DictRow]],
    target_version: int,
) -> None:
    """
    Run database migrations to reach the target version.

    Migrations files should be placed in the `migrations` directory, following the naming
    format:
        `{version}.{description}.{direction}.sql`
    where `version` is an integer defining the order that migrations should be performed,
    `description` is a short description of the migration, and `direction` is either `up`
    or `down` indicating the migration direction. Examples:
     - `1.create_users_table.up.sql`
     -  1.drop_users_table.down.sql`
     - `2.add_email_to_users.up.sql`
     - `2.remove_email_from_users.down.sql`

     It is recommended that migrations are wrapped in a transaction (e.g `BEGIN;` and
     `COMMIT;` statements), so that if a migration fails, the database state is not left
     in an inconsistent state.
    """

    async with connection_factory() as conn:
        create_table = await conn.execute("""
            CREATE TABLE IF NOT EXISTS migrations (
                id serial PRIMARY KEY,
                version INTEGER NOT NULL,
                direction TEXT NOT NULL,
                performed timestamp NOT NULL DEFAULT NOW()
            )
        """)
        await create_table.close()

        await (
            await conn.execute("LOCK TABLE migrations IN ACCESS EXCLUSIVE MODE")
        ).close()

        initial_version = 0
        cur = await conn.execute(
            "SELECT version FROM migrations ORDER BY id DESC LIMIT 1"
        )
        if (row := await cur.fetchone()) is not None and "version" in row:
            initial_version = row["version"]
        await cur.close()

        log.info(f"Migration Current: {initial_version} Target: {target_version}")
        if initial_version < target_version:
            direction = "up"
            change = 1
            offset = 1
        elif initial_version > target_version:
            direction = "down"
            change = -1
            offset = 0
        else:
            log.info("No migration needed. Already at target version.")
            return

        for i in range(initial_version + offset, target_version + offset, change):
            script = _get_migration_script(i, direction)
            log.info(f"Running migration {i} {direction}...")
            await (await conn.execute(script)).close()
            current_version = i if direction == "up" else i - 1
            log.info(f"Current version: {current_version}")
            await (
                await conn.execute(
                    "INSERT INTO migrations (version, direction) VALUES (%s, %s);",
                    (current_version, direction),
                )
            ).close()


def _get_migration_script(version: int, direction: str) -> bytes:
    search_path = Path.joinpath(
        Path(__file__).parent.resolve(), "migrations", f"{version}.*{direction}.sql"
    ).as_posix()

    matches = glob.glob(search_path)
    if not matches:
        print(glob.glob("core/migrations/*"))
        raise FileNotFoundError(
            f"No migration script found for version {version} and direction {direction}."
        )

    if len(matches) > 1:
        raise FileNotFoundError(
            f"Multiple migration scripts found for version {version} and direction {direction}."
        )

    return Path(matches[0]).read_bytes()

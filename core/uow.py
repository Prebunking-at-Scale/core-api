from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable, TypeVar

import psycopg
from psycopg.rows import DictRow

ConnectionFactory = Callable[[], psycopg.AsyncConnection[DictRow]]
T = TypeVar("T")


@asynccontextmanager
async def uow(
    repo: Callable[[psycopg.AsyncCursor[DictRow]], T],
    conn_factory: ConnectionFactory,
) -> AsyncGenerator[T, None]:
    async with conn_factory() as conn:
        session = conn.cursor()
        try:
            yield repo(session)
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        finally:
            await session.close()

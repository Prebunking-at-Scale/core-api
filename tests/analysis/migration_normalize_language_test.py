import json
import os
from typing import Any, Generator

import pytest
from testing.postgresql import Postgresql

import core.app as app
from core.migrate import migrate


@pytest.fixture
def migration_db() -> Generator[Postgresql, Any, None]:
    os.environ.setdefault("LC_ALL", "C")
    db = Postgresql(
        initdb_params="--locale-provider=builtin --encoding=UTF8",
        postgres_args="-c client_encoding=UTF8",
    )
    yield db
    db.stop()


async def _insert_video(conn, language):
    metadata = {"language": language} if language is not None else {}
    cur = await conn.execute(
        """
        INSERT INTO videos (title, description, platform, source_url,
                            destination_path, metadata)
        VALUES ('t', 'd', 'p', 'u', 'dp', %s)
        RETURNING id
        """,
        (json.dumps(metadata),),
    )
    return (await cur.fetchone())["id"]


async def _insert_sentence(conn, video_id, language):
    cur = await conn.execute(
        """
        INSERT INTO transcript_sentences (video_id, source, text, start_time_s,
                                          metadata)
        VALUES (%s, 'audio', 'hello', 0.0, %s)
        RETURNING id
        """,
        (video_id, json.dumps({"language": language})),
    )
    return (await cur.fetchone())["id"]


async def _insert_claim(conn, video_id, language):
    cur = await conn.execute(
        """
        INSERT INTO video_claims (video_id, claim, start_time_s, metadata)
        VALUES (%s, 'a claim', 0.0, %s)
        RETURNING id
        """,
        (video_id, json.dumps({"language": language})),
    )
    return (await cur.fetchone())["id"]


async def _language_of(conn, table, row_id):
    cur = await conn.execute(
        f"SELECT metadata->>'language' AS language FROM {table} WHERE id = %s",
        (row_id,),
    )
    return (await cur.fetchone())["language"]


async def test_migration_normalizes_language_codes(migration_db):
    pool = app.pool_factory(migration_db.url())
    await pool.open()
    try:
        # Migrate to the version *before* the backfill, then seed dirty data.
        await migrate(pool.connection, 20)

        cases = {
            "eng": "en",  # ISO 639-2/T
            "ENG": "en",  # uppercased code
            "fre": "fr",  # ISO 639-2/B (bibliographic)
            "spa": "es",
            "EN": "en",  # uppercased two letter code
            "en": "en",  # already canonical
            "English": "en",  # full language name
            "Armenian": "hy",  # name resolving to a different code
            " Armenian ": "hy",  # name with surrounding whitespace
            "PUNJABI": "pa",  # alternate spelling (pycountry: "Panjabi")
            "arc": "arc",  # valid ISO 639-3 with no two letter code -> kept
            "xx": "xx",  # unknown -> kept, lowercased
        }

        async with pool.connection() as conn:
            video_ids = {stored: await _insert_video(conn, stored) for stored in cases}
            no_language_id = await _insert_video(conn, None)

            sample_video = video_ids["eng"]
            sentence_id = await _insert_sentence(conn, sample_video, "English")
            claim_id = await _insert_claim(conn, sample_video, "PUNJABI")

        await migrate(pool.connection, 21)

        async with pool.connection() as conn:
            for stored, expected in cases.items():
                assert await _language_of(conn, "videos", video_ids[stored]) == expected

            assert await _language_of(conn, "videos", no_language_id) is None
            assert await _language_of(conn, "transcript_sentences", sentence_id) == "en"
            assert await _language_of(conn, "video_claims", claim_id) == "pa"
    finally:
        await pool.close()

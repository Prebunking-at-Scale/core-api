import json
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from pydantic import JsonValue

from core.errors import ConflictError
from core.media_feeds.models import ChannelFeed, Cursor, KeywordFeed, MediaFeed


class MediaFeedRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def get_all_feeds(
        self, organisation_id: UUID | None = None
    ) -> list[MediaFeed]:
        await self._session.execute(
            """
            SELECT * FROM media_feeds
            WHERE
                (%(organisation_id)s::uuid IS NULL OR organisation_id = %(organisation_id)s::uuid)
                AND NOT is_archived
            ORDER BY organisation_id, created_at DESC
            """,
            {"organisation_id": str(organisation_id) if organisation_id else None},
        )
        return [MediaFeed(**row) for row in await self._session.fetchall()]

    async def get_channel_feeds(
        self, organisation_id: UUID | None = None
    ) -> list[ChannelFeed]:
        await self._session.execute(
            """
            SELECT * FROM channel_feeds
            WHERE
                (%(organisation_id)s::uuid IS NULL OR organisation_id = %(organisation_id)s::uuid)
                AND NOT is_archived
            ORDER BY organisation_id, created_at DESC
        """,
            {"organisation_id": str(organisation_id) if organisation_id else None},
        )
        return [ChannelFeed(**row) for row in await self._session.fetchall()]

    async def get_channel_feed_by_id(
        self, feed_id: UUID, organisation_id: UUID | None = None
    ) -> ChannelFeed | None:
        await self._session.execute(
            """
            SELECT * FROM channel_feeds
            WHERE id = %(feed_id)s
                AND NOT is_archived
                AND (%(organisation_id)s::uuid IS NULL OR organisation_id = %(organisation_id)s::uuid)
            """,
            {
                "feed_id": str(feed_id),
                "organisation_id": str(organisation_id) if organisation_id else None,
            },
        )
        row = await self._session.fetchone()
        if not row:
            return None
        return ChannelFeed(**row)

    async def get_keyword_feeds(
        self, organisation_id: UUID | None = None
    ) -> list[KeywordFeed]:
        await self._session.execute(
            """
            SELECT * FROM keyword_feeds
            WHERE
                (%(organisation_id)s::uuid IS NULL OR organisation_id = %(organisation_id)s::uuid)
                AND NOT is_archived
            ORDER BY organisation_id, created_at DESC
        """,
            {"organisation_id": str(organisation_id) if organisation_id else None},
        )
        return [KeywordFeed(**row) for row in await self._session.fetchall()]

    async def get_keyword_feed_by_id(
        self, feed_id: UUID, organisation_id: UUID | None = None
    ) -> KeywordFeed | None:
        await self._session.execute(
            """
            SELECT * FROM keyword_feeds
            WHERE id = %(feed_id)s
                AND NOT is_archived
                AND (%(organisation_id)s::uuid IS NULL OR organisation_id = %(organisation_id)s::uuid)
            """,
            {
                "feed_id": str(feed_id),
                "organisation_id": str(organisation_id) if organisation_id else None,
            },
        )
        row = await self._session.fetchone()
        if not row:
            return None
        return KeywordFeed(**row)

    async def create_channel_feed(
        self, organisation_id: UUID, channel: str, platform: str
    ) -> ChannelFeed:
        try:
            await self._session.execute(
                """
                INSERT INTO channel_feeds (organisation_id, channel, platform)
                VALUES (%(organisation_id)s, %(channel)s, %(platform)s)
                RETURNING *
                """,
                {
                    "organisation_id": str(organisation_id),
                    "channel": channel,
                    "platform": platform,
                },
            )
        except psycopg.errors.UniqueViolation:
            raise ConflictError("channel already exists")

        row = await self._session.fetchone()
        if not row:
            raise ValueError("failed to create topic")

        return ChannelFeed(**row)

    async def bulk_create_channel_feeds(
        self, organisation_id: UUID, channels: list[tuple[str, str]]
    ) -> list[ChannelFeed]:
        if not channels:
            return []

        created_feeds: list[ChannelFeed] = []
        for channel, platform in channels:
            try:
                await self._session.execute(
                    """
                    INSERT INTO channel_feeds (organisation_id, channel, platform)
                    VALUES (%(organisation_id)s, %(channel)s, %(platform)s)
                    ON CONFLICT (organisation_id, lower(channel), platform) WHERE is_archived = FALSE
                    DO NOTHING
                    RETURNING *
                    """,
                    {
                        "organisation_id": str(organisation_id),
                        "channel": channel,
                        "platform": platform,
                    },
                )
                row = await self._session.fetchone()
                if row:
                    created_feeds.append(ChannelFeed(**row))
            except psycopg.Error:
                continue

        return created_feeds

    async def create_keyword_feed(
        self, organisation_id: UUID, topic: str, keywords: list[str]
    ) -> KeywordFeed:
        try:
            await self._session.execute(
                """
                INSERT INTO keyword_feeds (organisation_id, topic, keywords)
                VALUES (%(organisation_id)s, %(topic)s, %(keywords)s)
                RETURNING *
                """,
                {
                    "organisation_id": str(organisation_id),
                    "topic": topic,
                    "keywords": keywords,
                },
            )
        except psycopg.errors.UniqueViolation:
            raise ConflictError("keyword already exists")

        row = await self._session.fetchone()
        if not row:
            raise ValueError("failed to create topic")

        return KeywordFeed(**row)

    async def update_channel_feed(
        self,
        feed_id: UUID,
        channel: str,
        platform: str,
        organisation_id: UUID | None = None,
    ) -> ChannelFeed:
        await self._session.execute(
            """
            UPDATE channel_feeds
            SET channel = %(channel)s, platform = %(platform)s, updated_at = NOW()
            WHERE id = %(feed_id)s
            AND NOT is_archived
            AND (%(organisation_id)s::uuid IS NULL OR organisation_id = %(organisation_id)s::uuid)
            RETURNING *
            """,
            {
                "feed_id": str(feed_id),
                "channel": channel,
                "platform": platform,
                "organisation_id": str(organisation_id) if organisation_id else None,
            },
        )

        row = await self._session.fetchone()
        if not row:
            raise ValueError("channel feed not found")

        return ChannelFeed(**row)

    async def update_keyword_feed(
        self,
        feed_id: UUID,
        topic: str,
        keywords: list[str],
        organisation_id: UUID | None = None,
    ) -> KeywordFeed:
        await self._session.execute(
            """
            UPDATE keyword_feeds
            SET topic = %(topic)s, keywords = %(keywords)s, updated_at = NOW()
            WHERE id = %(feed_id)s
            AND NOT is_archived
            AND (%(organisation_id)s::uuid IS NULL OR organisation_id = %(organisation_id)s::uuid)
            RETURNING *
            """,
            {
                "feed_id": str(feed_id),
                "topic": topic,
                "keywords": keywords,
                "organisation_id": str(organisation_id) if organisation_id else None,
            },
        )

        row = await self._session.fetchone()
        if not row:
            raise ValueError("keyword feed not found")

        return KeywordFeed(**row)

    async def archive_channel_feed(
        self, feed_id: UUID, organisation_id: UUID | None = None
    ) -> None:
        await self._session.execute(
            """
            UPDATE channel_feeds
            SET is_archived = true, updated_at = NOW()
            WHERE id = %(feed_id)s
            AND NOT is_archived
            AND (%(organisation_id)s::uuid IS NULL OR organisation_id = %(organisation_id)s::uuid)
            """,
            {
                "feed_id": str(feed_id),
                "organisation_id": str(organisation_id) if organisation_id else None,
            },
        )

        if self._session.rowcount == 0:
            raise ValueError("channel feed not found")

    async def archive_keyword_feed(
        self, feed_id: UUID, organisation_id: UUID | None = None
    ) -> None:
        await self._session.execute(
            """
            UPDATE keyword_feeds
            SET is_archived = true, updated_at = NOW()
            WHERE id = %(feed_id)s
            AND NOT is_archived
            AND (%(organisation_id)s::uuid IS NULL OR organisation_id = %(organisation_id)s::uuid)
            """,
            {
                "feed_id": str(feed_id),
                "organisation_id": str(organisation_id) if organisation_id else None,
            },
        )

        if self._session.rowcount == 0:
            raise ValueError("keyword feed not found")

    async def get_cursor(self, target: str, platform: str) -> Cursor | None:
        await self._session.execute(
            """
            SELECT * FROM media_feed_cursors
            WHERE target = %(target)s AND platform = %(platform)s
            """,
            {"target": target, "platform": platform},
        )
        row = await self._session.fetchone()
        if not row:
            return None
        return Cursor(**row)

    async def upsert_cursor(
        self, target: str, platform: str, cursor_data: JsonValue
    ) -> Cursor:
        await self._session.execute(
            """
            INSERT INTO media_feed_cursors (target, platform, cursor, updated_at)
            VALUES (%(target)s, %(platform)s, %(cursor)s, NOW())
            ON CONFLICT (target, platform)
            DO UPDATE SET
                cursor = EXCLUDED.cursor,
                updated_at = NOW()
            RETURNING *
            """,
            {
                "target": target,
                "platform": platform,
                "cursor": json.dumps(cursor_data),
            },
        )

        row = await self._session.fetchone()
        if not row:
            raise ValueError("failed to upsert cursor")

        return Cursor(**row)

    async def delete_cursor(self, target: str, platform: str) -> None:
        await self._session.execute(
            """
            DELETE FROM media_feed_cursors
            WHERE target = %(target)s AND platform = %(platform)s
            """,
            {"target": target, "platform": platform},
        )

        if self._session.rowcount == 0:
            raise ValueError("cursor not found")

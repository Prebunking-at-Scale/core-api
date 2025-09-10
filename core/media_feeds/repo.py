from uuid import UUID

import psycopg
from psycopg.rows import DictRow

from core.errors import ConflictError
from core.media_feeds.models import ChannelFeed, KeywordFeed


class MediaFeedRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def create_channel_feed(
        self, organisation_id: UUID, user_id: UUID, channel: str, platform: str
    ) -> ChannelFeed:
        try:
            await self._session.execute(
                """
                INSERT INTO channel_feeds (organisation_id, created_by_user_id, channel, platform)
                VALUES (%(organisation_id)s, %(created_by_user_id)s, %(channel)s, %(platform)s)
                RETURNING *
                """,
                {
                    "organisation_id": organisation_id,
                    "created_by_user_id": user_id,
                    "channel": channel,
                    "platform": platform,
                },
            )
        except psycopg.errors.UniqueViolation:
            raise ConflictError("keyword already exists")

        row = await self._session.fetchone()
        if not row:
            raise ValueError("failed to create topic")

        return ChannelFeed(**row)

    async def create_keyword_feed(
        self, organisation_id: UUID, user_id: UUID, topic: str, keywords: list[str]
    ) -> KeywordFeed:
        try:
            await self._session.execute(
                """
                INSERT INTO keyword_feeds (organisation_id, created_by_user_id, topic, keywords)
                VALUES (%(organisation_id)s, %(created_by_user_id)s, %(topic)s, %(keywords)s)
                RETURNING *
                """,
                {
                    "organisation_id": organisation_id,
                    "created_by_user_id": user_id,
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

    async def get_channel_feeds(
        self, organisation_id: UUID | None = None
    ) -> list[ChannelFeed]:
        await self._session.execute(
            """
            SELECT * FROM channel_feeds
            WHERE %(organisation_id) IS NULL OR organisation_id = %(organisation_id)
            ORDER BY organisation_id, created_at DESC
        """,
            {"organisation_id": organisation_id},
        )
        return [ChannelFeed(**row) for row in await self._session.fetchall()]

    async def get_keyword_feeds(
        self, organisation_id: UUID | None = None
    ) -> list[KeywordFeed]:
        await self._session.execute(
            """
            SELECT * FROM keyword_feeds
            WHERE %(organisation_id) IS NULL OR organisation_id = %(organisation_id)
            ORDER BY organisation_id, created_at DESC
        """,
            {"organisation_id": organisation_id},
        )
        return [KeywordFeed(**row) for row in await self._session.fetchall()]

    async def update_keyword_feed(
        self, feed_id: UUID, topic: str, keywords: list[str]
    ) -> KeywordFeed:
        await self._session.execute(
            """
            UPDATE keyword_feeds
            SET topic = %(topic)s, keywords = %(keywords)s, updated_at = NOW()
            WHERE id = %(feed_id)s
            RETURNING *
            """,
            {
                "feed_id": feed_id,
                "topic": topic,
                "keywords": keywords,
            },
        )

        row = await self._session.fetchone()
        if not row:
            raise ValueError("keyword feed not found")

        return KeywordFeed(**row)

    async def update_channel_feed(
        self, feed_id: UUID, channel: str, platform: str
    ) -> ChannelFeed:
        await self._session.execute(
            """
            UPDATE channel_feeds
            SET channel = %(channel)s, platform = %(platform)s, updated_at = NOW()
            WHERE id = %(feed_id)s
            RETURNING *
            """,
            {
                "feed_id": feed_id,
                "channel": channel,
                "platform": platform,
            },
        )

        row = await self._session.fetchone()
        if not row:
            raise ValueError("channel feed not found")

        return ChannelFeed(**row)

    async def archive_keyword_feed(self, feed_id: UUID) -> None:
        await self._session.execute(
            """
            UPDATE keyword_feeds
            SET is_archived = true, updated_at = NOW()
            WHERE id = %(feed_id)s
            """,
            {"feed_id": feed_id},
        )

        if self._session.rowcount == 0:
            raise ValueError("keyword feed not found")

    async def archive_channel_feed(self, feed_id: UUID) -> None:
        await self._session.execute(
            """
            UPDATE channel_feeds
            SET is_archived = true, updated_at = NOW()
            WHERE id = %(feed_id)s
            """,
            {"feed_id": feed_id},
        )

        if self._session.rowcount == 0:
            raise ValueError("channel feed not found")

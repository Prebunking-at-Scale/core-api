from typing import AsyncContextManager
from uuid import UUID

from litestar.dto import DTOData
from pydantic import JsonValue

from core.errors import NotFoundError
from core.media_feeds.models import (
    AllFeeds,
    ChannelFeed,
    Cursor,
    KeywordFeed,
)
from core.media_feeds.repo import MediaFeedRepository
from core.uow import ConnectionFactory, uow


class MediaFeedService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[MediaFeedRepository]:
        return uow(MediaFeedRepository, self._connection_factory)

    async def get_all_feeds(self, organisation_id: UUID | None = None) -> AllFeeds:
        async with self.repo() as repo:
            return AllFeeds(
                channel_feeds=await repo.get_channel_feeds(organisation_id),
                keyword_feeds=await repo.get_keyword_feeds(organisation_id),
            )

    async def get_channel_feeds(self, organisation_id: UUID) -> list[ChannelFeed]:
        async with self.repo() as repo:
            return await repo.get_channel_feeds(organisation_id)

    async def get_channel_feed_by_id(
        self, organisation_id: UUID, feed_id: UUID
    ) -> ChannelFeed | None:
        async with self.repo() as repo:
            return await repo.get_channel_feed_by_id(feed_id, organisation_id)

    async def get_keyword_feeds(self, organisation_id: UUID) -> list[KeywordFeed]:
        async with self.repo() as repo:
            return await repo.get_keyword_feeds(organisation_id)

    async def get_keyword_feed_by_id(
        self, organisation_id: UUID, feed_id: UUID
    ) -> KeywordFeed | None:
        async with self.repo() as repo:
            return await repo.get_keyword_feed_by_id(feed_id, organisation_id)

    async def create_channel_feed(
        self, organisation_id: UUID, channel: str, platform: str
    ) -> ChannelFeed:
        async with self.repo() as repo:
            return await repo.create_channel_feed(organisation_id, channel, platform)

    async def create_keyword_feed(
        self, organisation_id: UUID, topic: str, keywords: list[str]
    ) -> KeywordFeed:
        async with self.repo() as repo:
            return await repo.create_keyword_feed(organisation_id, topic, keywords)

    async def update_channel_feed(
        self, organisation_id: UUID, feed_id: UUID, data: DTOData[ChannelFeed]
    ) -> ChannelFeed:
        async with self.repo() as repo:
            feed = await repo.get_channel_feed_by_id(feed_id, organisation_id)
            if not feed:
                raise NotFoundError("channel feed not found")
            data.update_instance(feed)

            return await repo.update_channel_feed(
                feed_id, feed.channel, feed.platform, organisation_id
            )

    async def update_keyword_feed(
        self, organisation_id: UUID, feed_id: UUID, data: DTOData[KeywordFeed]
    ) -> KeywordFeed:
        async with self.repo() as repo:
            feed = await repo.get_keyword_feed_by_id(feed_id, organisation_id)
            if not feed:
                raise NotFoundError("keyword feed not found")
            data.update_instance(feed)

            return await repo.update_keyword_feed(
                feed_id, feed.topic, feed.keywords, organisation_id
            )

    async def archive_channel_feed(self, organisation_id: UUID, feed_id: UUID) -> None:
        async with self.repo() as repo:
            await repo.archive_channel_feed(feed_id, organisation_id)

    async def archive_keyword_feed(self, organisation_id: UUID, feed_id: UUID) -> None:
        async with self.repo() as repo:
            await repo.archive_keyword_feed(feed_id, organisation_id)

    async def get_cursor(self, target: str, platform: str) -> Cursor | None:
        async with self.repo() as repo:
            return await repo.get_cursor(target, platform)

    async def set_cursor(
        self, target: str, platform: str, cursor_data: JsonValue
    ) -> Cursor:
        async with self.repo() as repo:
            return await repo.upsert_cursor(target, platform, cursor_data)

    async def delete_cursor(self, target: str, platform: str) -> None:
        async with self.repo() as repo:
            await repo.delete_cursor(target, platform)

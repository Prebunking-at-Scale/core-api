from typing import Any, AsyncContextManager
from uuid import UUID

from litestar.dto import DTOData

from core.media_feeds.models import ChannelFeed, KeywordFeed
from core.media_feeds.repo import MediaFeedRepository
from core.uow import ConnectionFactory, uow


class MediaFeedService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[MediaFeedRepository]:
        return uow(MediaFeedRepository, self._connection_factory)

    async def create_channel_feed(
        self, organisation_id: UUID, user_id: UUID, channel: str, platform: str
    ) -> ChannelFeed:
        async with self.repo() as repo:
            return await repo.create_channel_feed(
                organisation_id, user_id, channel, platform
            )

    async def create_keyword_feed(
        self, organisation_id: UUID, user_id: UUID, topic: str, keywords: list[str]
    ) -> KeywordFeed:
        async with self.repo() as repo:
            return await repo.create_keyword_feed(
                organisation_id, user_id, topic, keywords
            )

    async def get_channel_feeds(
        self, organisation_id: UUID | None = None
    ) -> list[ChannelFeed]:
        async with self.repo() as repo:
            return await repo.get_channel_feeds(organisation_id)

    async def get_keyword_feeds(
        self, organisation_id: UUID | None = None
    ) -> list[KeywordFeed]:
        async with self.repo() as repo:
            return await repo.get_keyword_feeds(organisation_id)

    async def update_keyword_feed(
        self, feed_id: UUID, topic: str, keywords: list[str]
    ) -> KeywordFeed:
        async with self.repo() as repo:
            return await repo.update_keyword_feed(feed_id, topic, keywords)

    async def update_channel_feed(
        self, feed_id: UUID, channel: str, platform: str
    ) -> ChannelFeed:
        async with self.repo() as repo:
            return await repo.update_channel_feed(feed_id, channel, platform)

    async def archive_keyword_feed(self, feed_id: UUID) -> None:
        async with self.repo() as repo:
            await repo.archive_keyword_feed(feed_id)

    async def archive_channel_feed(self, feed_id: UUID) -> None:
        async with self.repo() as repo:
            await repo.archive_channel_feed(feed_id)

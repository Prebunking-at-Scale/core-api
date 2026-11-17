from typing import AsyncContextManager
from uuid import UUID

import structlog
from litestar.dto import DTOData
from litestar.exceptions import ValidationException
from pydantic import JsonValue

from core.errors import NotFoundError
from core.media_feeds.models import (
    MAX_CHANNEL_FEEDS_PER_ORGANISATION,
    MAX_KEYWORDS_PER_ORGANISATION,
    AllFeeds,
    ChannelFeed,
    Cursor,
    FeedLimits,
    KeywordFeed,
)
from core.media_feeds.repo import MediaFeedRepository
from core.uow import ConnectionFactory, uow

logger = structlog.get_logger(__name__)


async def check_channel_limit(repo: MediaFeedRepository, organisation_id: UUID) -> None:
    """Raise if the organisation is over its channel limit.

    Called after the write, inside the same transaction, so duplicates and
    archived feeds are already accounted for and the write rolls back on error.
    """
    count = await repo.count_channel_feeds(organisation_id)
    if count > MAX_CHANNEL_FEEDS_PER_ORGANISATION:
        logger.warning(
            "feed limit exceeded",
            limit_type="channels",
            organisation_id=str(organisation_id),
            count=count,
            limit=MAX_CHANNEL_FEEDS_PER_ORGANISATION,
        )
        raise ValidationException(
            f"This would bring your organisation to {count} channels, above the "
            f"limit of {MAX_CHANNEL_FEEDS_PER_ORGANISATION}. "
            "Archive some channels, or contact us to increase the limit."
        )


async def check_keyword_limit(repo: MediaFeedRepository, organisation_id: UUID) -> None:
    """Raise if the organisation is over its keyword limit. See check_channel_limit."""
    count = await repo.count_keywords(organisation_id)
    if count > MAX_KEYWORDS_PER_ORGANISATION:
        logger.warning(
            "feed limit exceeded",
            limit_type="keywords",
            organisation_id=str(organisation_id),
            count=count,
            limit=MAX_KEYWORDS_PER_ORGANISATION,
        )
        raise ValidationException(
            f"This would bring your organisation to {count} keywords, above the "
            f"limit of {MAX_KEYWORDS_PER_ORGANISATION}. "
            "Remove some keywords, or contact us to increase the limit."
        )


class MediaFeedsService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[MediaFeedRepository]:
        return uow(MediaFeedRepository, self._connection_factory)

    async def get_all_feeds(self, organisation_id: UUID | None = None) -> AllFeeds:
        async with self.repo() as repo:
            return AllFeeds(
                channel_feeds=await repo.get_channel_feeds(organisation_id),
                keyword_feeds=await repo.get_keyword_feeds(organisation_id),
                limits=FeedLimits(
                    max_channel_feeds=MAX_CHANNEL_FEEDS_PER_ORGANISATION,
                    max_keywords=MAX_KEYWORDS_PER_ORGANISATION,
                ),
            )

    async def get_channel_feeds(
        self, organisation_id: UUID | None = None
    ) -> list[ChannelFeed]:
        async with self.repo() as repo:
            return await repo.get_channel_feeds(organisation_id)

    async def get_channel_feed_by_id(
        self, organisation_id: UUID, feed_id: UUID
    ) -> ChannelFeed | None:
        async with self.repo() as repo:
            return await repo.get_channel_feed_by_id(feed_id, organisation_id)

    async def get_keyword_feeds(
        self, organisation_id: UUID | None = None
    ) -> list[KeywordFeed]:
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
            feed = await repo.create_channel_feed(organisation_id, channel, platform)
            await check_channel_limit(repo, organisation_id)
            return feed

    async def bulk_create_channel_feeds(
        self, organisation_id: UUID, channels: list[tuple[str, str]]
    ) -> list[ChannelFeed]:
        async with self.repo() as repo:
            feeds = await repo.bulk_create_channel_feeds(organisation_id, channels)
            await check_channel_limit(repo, organisation_id)
            return feeds

    async def create_keyword_feed(
        self, organisation_id: UUID, topic_id: UUID, keywords: list[str]
    ) -> KeywordFeed:
        async with self.repo() as repo:
            feed = await repo.create_keyword_feed(organisation_id, topic_id, keywords)
            await check_keyword_limit(repo, organisation_id)
            return feed

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

            updated = await repo.update_keyword_feed(
                feed_id, feed.topic_id, feed.keywords, organisation_id
            )
            await check_keyword_limit(repo, organisation_id)
            return updated

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

from uuid import UUID

from litestar import Controller, delete, get, patch, post
from litestar.di import Provide
from litestar.dto import DTOData
from litestar.exceptions import NotFoundException

from core.errors import ConflictError
from core.media_feeds.models import (
    ChannelFeed,
    ChannelFeedDTO,
    KeywordFeed,
    KeywordFeedDTO,
)
from core.media_feeds.service import MediaFeedService
from core.response import JSON
from core.uow import ConnectionFactory


async def media_feeds_service(
    connection_factory: ConnectionFactory,
) -> MediaFeedService:
    return MediaFeedService(connection_factory=connection_factory)


class MediaFeedController(Controller):
    path = "/media_feeds"
    tags = ["media_feeds"]

    dependencies = {
        "media_feeds_service": Provide(media_feeds_service),
    }

    @post(
        path="/channels",
        summary="Create a new channel feed",
        dto=ChannelFeedDTO,
        return_dto=None,
        raises=[ConflictError],
    )
    async def create_channel_feed(
        self,
        media_feeds_service: MediaFeedService,
        data: DTOData[ChannelFeed],
    ) -> JSON[ChannelFeed]:
        channel_data = data.create_instance()
        return JSON(
            await media_feeds_service.create_channel_feed(
                organisation_id=channel_data.organisation_id,
                user_id=channel_data.created_by_user_id,
                channel=channel_data.channel,
                platform=channel_data.platform,
            )
        )

    @post(
        path="/keywords",
        summary="Create a new keyword feed",
        dto=KeywordFeedDTO,
        return_dto=None,
        raises=[ConflictError],
    )
    async def create_keyword_feed(
        self,
        media_feeds_service: MediaFeedService,
        data: DTOData[KeywordFeed],
    ) -> JSON[KeywordFeed]:
        keyword_data = data.create_instance()
        return JSON(
            await media_feeds_service.create_keyword_feed(
                organisation_id=keyword_data.organisation_id,
                user_id=keyword_data.created_by_user_id,
                topic=keyword_data.topic,
                keywords=keyword_data.keywords,
            )
        )

    @get(
        path="/channels",
        summary="Get channel feeds",
    )
    async def get_channel_feeds(
        self,
        media_feeds_service: MediaFeedService,
        organisation_id: UUID | None = None,
    ) -> JSON[list[ChannelFeed]]:
        return JSON(await media_feeds_service.get_channel_feeds(organisation_id))

    @get(
        path="/keywords",
        summary="Get keyword feeds",
    )
    async def get_keyword_feeds(
        self,
        media_feeds_service: MediaFeedService,
        organisation_id: UUID | None = None,
    ) -> JSON[list[KeywordFeed]]:
        return JSON(await media_feeds_service.get_keyword_feeds(organisation_id))

    @patch(
        path="/keywords/{feed_id:uuid}",
        summary="Update a keyword feed",
        dto=KeywordFeedDTO,
        return_dto=None,
    )
    async def update_keyword_feed(
        self,
        media_feeds_service: MediaFeedService,
        feed_id: UUID,
        data: DTOData[KeywordFeed],
    ) -> JSON[KeywordFeed]:
        keyword_data = data.create_instance()
        try:
            return JSON(
                await media_feeds_service.update_keyword_feed(
                    feed_id=feed_id,
                    topic=keyword_data.topic,
                    keywords=keyword_data.keywords,
                )
            )
        except ValueError:
            raise NotFoundException()

    @patch(
        path="/channels/{feed_id:uuid}",
        summary="Update a channel feed",
        dto=ChannelFeedDTO,
        return_dto=None,
    )
    async def update_channel_feed(
        self,
        media_feeds_service: MediaFeedService,
        feed_id: UUID,
        data: DTOData[ChannelFeed],
    ) -> JSON[ChannelFeed]:
        channel_data = data.create_instance()
        try:
            return JSON(
                await media_feeds_service.update_channel_feed(
                    feed_id=feed_id,
                    channel=channel_data.channel,
                    platform=channel_data.platform,
                )
            )
        except ValueError:
            raise NotFoundException()

    @delete(
        path="/keywords/{feed_id:uuid}",
        summary="Archive a keyword feed",
    )
    async def archive_keyword_feed(
        self,
        media_feeds_service: MediaFeedService,
        feed_id: UUID,
    ) -> None:
        try:
            await media_feeds_service.archive_keyword_feed(feed_id)
        except ValueError:
            raise NotFoundException()

    @delete(
        path="/channels/{feed_id:uuid}",
        summary="Archive a channel feed",
    )
    async def archive_channel_feed(
        self,
        media_feeds_service: MediaFeedService,
        feed_id: UUID,
    ) -> None:
        try:
            await media_feeds_service.archive_channel_feed(feed_id)
        except ValueError:
            raise NotFoundException()

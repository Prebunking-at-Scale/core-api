from typing import Any
from uuid import UUID

from litestar import Controller, delete, get, patch, post
from litestar.di import Provide
from litestar.dto import DTOData
from litestar.exceptions import NotFoundException, ValidationException

from core.auth.guards import api_only, organisation_admin
from core.auth.models import Organisation
from core.errors import ConflictError
from core.media_feeds.models import (
    AllFeeds,
    ChannelFeed,
    ChannelFeedDTO,
    ChannelURLRequest,
    ChannelURLRequestDTO,
    Cursor,
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

    @get(
        path="/",
        summary="Get all media feeds for the current organisation",
    )
    async def get_organisation_feeds(
        self,
        media_feeds_service: MediaFeedService,
        organisation: Organisation,
    ) -> JSON[AllFeeds]:
        return JSON(await media_feeds_service.get_all_feeds(organisation.id))

    @get(
        path="/all",
        summary="Get all media feeds for all organisations",
        guards=[api_only],
    )
    async def get_all_feeds(
        self,
        media_feeds_service: MediaFeedService,
    ) -> JSON[AllFeeds]:
        return JSON(await media_feeds_service.get_all_feeds())

    @get(
        path="/channels",
        summary="Get channel feeds",
    )
    async def get_channel_feeds(
        self,
        media_feeds_service: MediaFeedService,
        organisation: Organisation,
    ) -> JSON[list[ChannelFeed]]:
        return JSON(await media_feeds_service.get_channel_feeds(organisation.id))

    @get(
        path="/channels/{feed_id:uuid}",
        summary="Get a specific channel feed by ID",
    )
    async def get_channel_feed_by_id(
        self,
        media_feeds_service: MediaFeedService,
        organisation: Organisation,
        feed_id: UUID,
    ) -> JSON[ChannelFeed]:
        feed = await media_feeds_service.get_channel_feed_by_id(
            organisation.id, feed_id
        )
        if not feed:
            raise NotFoundException()
        return JSON(feed)

    @get(
        path="/keywords",
        summary="Get keyword feeds",
    )
    async def get_keyword_feeds(
        self,
        media_feeds_service: MediaFeedService,
        organisation: Organisation,
    ) -> JSON[list[KeywordFeed]]:
        return JSON(await media_feeds_service.get_keyword_feeds(organisation.id))

    @get(
        path="/keywords/{feed_id:uuid}",
        summary="Get a specific keyword feed by ID",
    )
    async def get_keyword_feed_by_id(
        self,
        media_feeds_service: MediaFeedService,
        organisation: Organisation,
        feed_id: UUID,
    ) -> JSON[KeywordFeed]:
        feed = await media_feeds_service.get_keyword_feed_by_id(
            organisation.id, feed_id
        )
        if not feed:
            raise NotFoundException()
        return JSON(feed)

    @post(
        path="/channels",
        summary="Create a new channel feed",
        guards=[organisation_admin],
        dto=ChannelFeedDTO,
        return_dto=None,
        raises=[ConflictError],
    )
    async def create_channel_feed(
        self,
        media_feeds_service: MediaFeedService,
        organisation: Organisation,
        data: DTOData[ChannelFeed],
    ) -> JSON[ChannelFeed]:
        channel_data = data.create_instance(organisation_id=organisation.id)
        return JSON(
            await media_feeds_service.create_channel_feed(
                organisation_id=channel_data.organisation_id,
                channel=channel_data.channel,
                platform=channel_data.platform,
            )
        )

    @post(
        path="/channels/from-url",
        summary="Create a new channel feed from URL",
        guards=[organisation_admin],
        dto=ChannelURLRequestDTO,
        return_dto=None,
        raises=[ConflictError, ValidationException],
    )
    async def create_channel_feed_from_url(
        self,
        media_feeds_service: MediaFeedService,
        organisation: Organisation,
        data: DTOData[ChannelURLRequest],
    ) -> JSON[ChannelFeed]:
        url_data = data.create_instance()
        try:
            platform, channel = url_data.parse_channel_info()
        except ValueError as e:
            raise ValidationException(str(e))
        return JSON(
            await media_feeds_service.create_channel_feed(
                organisation_id=organisation.id,
                channel=channel,
                platform=platform,
            )
        )

    @post(
        path="/keywords",
        summary="Create a new keyword feed",
        guards=[organisation_admin],
        dto=KeywordFeedDTO,
        return_dto=None,
        raises=[ConflictError],
    )
    async def create_keyword_feed(
        self,
        media_feeds_service: MediaFeedService,
        organisation: Organisation,
        data: DTOData[KeywordFeed],
    ) -> JSON[KeywordFeed]:
        keyword_data = data.create_instance(organisation_id=organisation.id)
        return JSON(
            await media_feeds_service.create_keyword_feed(
                organisation_id=keyword_data.organisation_id,
                topic=keyword_data.topic,
                keywords=keyword_data.keywords,
            )
        )

    @patch(
        path="/channels/{feed_id:uuid}",
        summary="Update a channel feed",
        guards=[organisation_admin],
        dto=ChannelFeedDTO,
        return_dto=None,
    )
    async def update_channel_feed(
        self,
        media_feeds_service: MediaFeedService,
        organisation: Organisation,
        feed_id: UUID,
        data: DTOData[ChannelFeed],
    ) -> JSON[ChannelFeed]:
        try:
            return JSON(
                await media_feeds_service.update_channel_feed(
                    organisation_id=organisation.id,
                    feed_id=feed_id,
                    data=data,
                )
            )
        except ValueError:
            raise NotFoundException()

    @patch(
        path="/keywords/{feed_id:uuid}",
        summary="Update a keyword feed",
        guards=[organisation_admin],
        dto=KeywordFeedDTO,
        return_dto=None,
    )
    async def update_keyword_feed(
        self,
        media_feeds_service: MediaFeedService,
        organisation: Organisation,
        feed_id: UUID,
        data: DTOData[KeywordFeed],
    ) -> JSON[KeywordFeed]:
        try:
            return JSON(
                await media_feeds_service.update_keyword_feed(
                    organisation_id=organisation.id,
                    feed_id=feed_id,
                    data=data,
                )
            )
        except ValueError:
            raise NotFoundException()

    @delete(
        path="/channels/{feed_id:uuid}",
        summary="Archive a channel feed",
        guards=[organisation_admin],
    )
    async def archive_channel_feed(
        self,
        media_feeds_service: MediaFeedService,
        organisation: Organisation,
        feed_id: UUID,
    ) -> None:
        try:
            await media_feeds_service.archive_channel_feed(organisation.id, feed_id)
        except ValueError:
            raise NotFoundException()

    @delete(
        path="/keywords/{feed_id:uuid}",
        summary="Archive a keyword feed",
        guards=[organisation_admin],
    )
    async def archive_keyword_feed(
        self,
        media_feeds_service: MediaFeedService,
        organisation: Organisation,
        feed_id: UUID,
    ) -> None:
        try:
            await media_feeds_service.archive_keyword_feed(organisation.id, feed_id)
        except ValueError:
            raise NotFoundException()

    @get(
        path="/cursors/{target:str}/{platform:str}",
        summary="Get cursor for a target and platform",
        guards=[api_only],
    )
    async def get_cursor(
        self,
        media_feeds_service: MediaFeedService,
        target: str,
        platform: str,
    ) -> JSON[Cursor]:
        cursor = await media_feeds_service.get_cursor(target, platform)
        if not cursor:
            raise NotFoundException()
        return JSON(cursor)

    @post(
        path="/cursors/{target:str}/{platform:str}",
        summary="Set or update the cursor for a target and platform",
        guards=[api_only],
    )
    async def set_cursor(
        self,
        media_feeds_service: MediaFeedService,
        target: str,
        platform: str,
        data: Any,
    ) -> JSON[Cursor]:
        return JSON(
            await media_feeds_service.set_cursor(
                target=target,
                platform=platform,
                cursor_data=data,
            )
        )

    @delete(
        path="/cursors/{target:str}/{platform:str}",
        summary="Delete cursor for a target and platform",
        guards=[api_only],
    )
    async def delete_cursor(
        self,
        media_feeds_service: MediaFeedService,
        target: str,
        platform: str,
    ) -> None:
        try:
            await media_feeds_service.delete_cursor(target, platform)
        except ValueError:
            raise NotFoundException()

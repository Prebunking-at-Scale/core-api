import csv
import io
from typing import Any
from uuid import UUID

from litestar import Controller, delete, get, patch, post
from litestar.datastructures import UploadFile
from litestar.di import Provide
from litestar.dto import DTOData
from litestar.enums import RequestEncodingType
from litestar.exceptions import NotFoundException, ValidationException
from litestar.params import Body

from core.auth.guards import api_only, organisation_admin
from core.auth.models import Organisation
from core.errors import ConflictError
from core.media_feeds.models import (
    VALID_PLATFORMS,
    AllFeeds,
    BulkChannelUploadResult,
    ChannelFeed,
    ChannelFeedDTO,
    ChannelURLRequest,
    ChannelURLRequestDTO,
    Cursor,
    KeywordFeed,
    KeywordFeedDTO,
    SkippedChannel,
    parse_channel_from_url,
)
from core.media_feeds.service import MediaFeedsService
from core.response import JSON
from core.uow import ConnectionFactory

MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024
MAX_CHANNELS_PER_UPLOAD = 10000


async def media_feeds_service(
    connection_factory: ConnectionFactory,
) -> MediaFeedsService:
    return MediaFeedsService(connection_factory=connection_factory)


def parse_channels_from_csv(text: str) -> tuple[list[tuple[str, str]], list[str]]:
    channels: list[tuple[str, str]] = []
    errors: list[str] = []
    reader = csv.reader(io.StringIO(text))
    header: list[str] | None = None

    for row_num, row in enumerate(reader, start=1):
        if not row or all(not cell.strip() for cell in row):
            continue

        if row_num == 1:
            normalized_row = [col.strip().lower() for col in row]
            if (
                "channel" in normalized_row
                or "url" in normalized_row
                or "platform" in normalized_row
            ):
                header = normalized_row
                continue

        if header:
            row_dict = dict(zip(header, row))
            channel = row_dict.get("channel", "").strip()
            platform = row_dict.get("platform", "").strip().lower()
            url = row_dict.get("url", "").strip()

            if url and not channel:
                try:
                    platform, channel = parse_channel_from_url(url)
                except ValueError as e:
                    errors.append(f"Row {row_num}: {e}")
                    continue
            elif not channel:
                continue

            if not platform:
                errors.append(f"Row {row_num}: Missing platform")
                continue

            if platform not in VALID_PLATFORMS:
                errors.append(
                    f"Row {row_num}: Invalid platform '{platform}'. "
                    f"Valid platforms: {', '.join(VALID_PLATFORMS)}"
                )
                continue

            channels.append((channel, platform))
        else:
            # Assume we're getting a list of URLs
            value = row[0].strip() if row else ""
            if not value:
                continue
            try:
                platform, channel = parse_channel_from_url(value)
                channels.append((channel, platform))
            except ValueError as e:
                errors.append(f"Row {row_num}: {e}")

    return channels, errors


def parse_channels_from_text(text: str) -> tuple[list[tuple[str, str]], list[str]]:
    channels: list[tuple[str, str]] = []
    errors: list[str] = []
    for line_num, line in enumerate(text.splitlines(), start=1):
        value = line.strip()
        if not value:
            continue
        try:
            platform, channel = parse_channel_from_url(value)
            channels.append((channel, platform))
        except ValueError as e:
            errors.append(f"Line {line_num}: {e}")
    return channels, errors


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
        media_feeds_service: MediaFeedsService,
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
        media_feeds_service: MediaFeedsService,
    ) -> JSON[AllFeeds]:
        return JSON(await media_feeds_service.get_all_feeds())

    @get(
        path="/channels",
        summary="Get channel feeds",
    )
    async def get_channel_feeds(
        self,
        media_feeds_service: MediaFeedsService,
        optional_organisation: Organisation | None,
    ) -> JSON[list[ChannelFeed]]:
        return JSON(
            await media_feeds_service.get_channel_feeds(
                optional_organisation.id if optional_organisation else None
            )
        )

    @get(
        path="/channels/{feed_id:uuid}",
        summary="Get a specific channel feed by ID",
    )
    async def get_channel_feed_by_id(
        self,
        media_feeds_service: MediaFeedsService,
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
        media_feeds_service: MediaFeedsService,
        optional_organisation: Organisation | None,
    ) -> JSON[list[KeywordFeed]]:
        return JSON(
            await media_feeds_service.get_keyword_feeds(
                optional_organisation.id if optional_organisation else None
            )
        )

    @get(
        path="/keywords/{feed_id:uuid}",
        summary="Get a specific keyword feed by ID",
    )
    async def get_keyword_feed_by_id(
        self,
        media_feeds_service: MediaFeedsService,
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
        media_feeds_service: MediaFeedsService,
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
        media_feeds_service: MediaFeedsService,
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
        path="/channels/bulk-upload",
        summary="Bulk upload channels from CSV or TXT file",
        guards=[organisation_admin],
        raises=[ValidationException],
    )
    async def bulk_upload_channels(
        self,
        media_feeds_service: MediaFeedsService,
        organisation: Organisation,
        data: UploadFile = Body(media_type=RequestEncodingType.MULTI_PART),
    ) -> JSON[BulkChannelUploadResult]:
        content = await data.read()

        if len(content) > MAX_FILE_SIZE_BYTES:
            raise ValidationException(
                f"File size exceeds maximum allowed size of {MAX_FILE_SIZE_BYTES // (1024 * 1024)}MB"
            )

        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            raise ValidationException("File must be UTF-8 encoded")

        filename = data.filename or ""
        if filename.endswith(".csv"):
            channels_to_create, errors = parse_channels_from_csv(text)
        else:
            channels_to_create, errors = parse_channels_from_text(text)

        if not channels_to_create:
            raise ValidationException("No valid channels found in file")

        if len(channels_to_create) > MAX_CHANNELS_PER_UPLOAD:
            raise ValidationException(
                f"Too many channels ({len(channels_to_create)}). "
                f"Maximum allowed is {MAX_CHANNELS_PER_UPLOAD}"
            )

        created = await media_feeds_service.bulk_create_channel_feeds(
            organisation_id=organisation.id,
            channels=channels_to_create,
        )

        created_channels = {(feed.channel, feed.platform) for feed in created}
        skipped: list[SkippedChannel] = []
        for channel, platform in channels_to_create:
            if (channel, platform) not in created_channels:
                skipped.append(SkippedChannel(channel=channel, platform=platform))  # type: ignore[arg-type]

        return JSON(
            BulkChannelUploadResult(
                created=created,
                skipped=skipped,
                errors=errors,
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
        media_feeds_service: MediaFeedsService,
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
        media_feeds_service: MediaFeedsService,
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
        media_feeds_service: MediaFeedsService,
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
        media_feeds_service: MediaFeedsService,
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
        media_feeds_service: MediaFeedsService,
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
        media_feeds_service: MediaFeedsService,
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
        media_feeds_service: MediaFeedsService,
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
        media_feeds_service: MediaFeedsService,
        target: str,
        platform: str,
    ) -> None:
        try:
            await media_feeds_service.delete_cursor(target, platform)
        except ValueError:
            raise NotFoundException()

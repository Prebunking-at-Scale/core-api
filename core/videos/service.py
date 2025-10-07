from typing import Any, AsyncContextManager
from uuid import UUID

from litestar.dto import DTOData

from core.languages.models import LanguageWithVideoCount
from core.models import Narrative, Video
from core.uow import ConnectionFactory, uow
from core.videos.models import VideoFilters
from core.videos.repo import VideoRepository


class VideoService:
    def __init__(self, connection_factory: ConnectionFactory):
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[VideoRepository]:
        return uow(VideoRepository, self._connection_factory)

    async def get_video_by_id(self, video_id: UUID) -> Video | None:
        async with self.repo() as repo:
            return await repo.get_video_by_id(video_id)

    async def filter_videos(self, filters: VideoFilters) -> list[Video]:
        async with self.repo() as repo:
            return await repo.filter_videos(filters)

    async def add_video(self, video: Video) -> Video:
        async with self.repo() as repo:
            return await repo.add_video(video)

    async def patch_video(
        self, video_id: UUID, video_data: DTOData[Video] | Video
    ) -> Video:
        async with self.repo() as repo:
            video = await repo.get_video_by_id(video_id)
            if not video:
                raise ValueError(f"Video with ID {video_id} not found")
            if isinstance(video_data, DTOData):
                updated_video = video_data.update_instance(video)
            else:
                updated_video = video_data
            return await repo.update_video(updated_video)

    async def delete_video(self, video_id) -> None:
        async with self.repo() as repo:
            return await repo.delete_video(video_id)

    async def get_videos_paginated(
        self,
        limit: int = 100,
        offset: int = 0,
        platform: str | None = None,
        channel: str | None = None,
        text: str | None = None,
        video_language: str | None = None,
    ) -> tuple[list[Video], int]:
        async with self.repo() as repo:
            return await repo.get_videos_paginated(
                limit, offset, platform, channel, text, video_language
            )

    async def get_narratives_for_video(self, video_id: UUID) -> list[Narrative]:
        async with self.repo() as repo:
            return await repo.get_narratives_for_video(video_id)

    async def get_languages_associated_with_videos(self) -> list[LanguageWithVideoCount]:
        async with self.repo() as repo:
            return await repo.get_languages_associated_with_videos()

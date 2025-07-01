from uuid import UUID

from litestar import Controller, delete, get, patch, post
from litestar.di import Provide
from litestar.dto import DTOData

from core.response import JSON, CursorJSON
from core.uow import ConnectionFactory
from core.videos.models import VideoCreate, VideoFilters, VideoPatch, Video
from core.videos.service import VideoService


async def video_service(connection_factory: ConnectionFactory) -> VideoService:
    return VideoService(connection_factory=connection_factory)


class VideoController(Controller):
    path = "/videos"
    tags = ["videos"]

    dependencies = {
        "video_service": Provide(video_service),
    }

    @post(
        path="/filter",
        summary="Get all or a filtered subset of videos",
    )
    async def filter_videos(
        self, video_service: VideoService, data: VideoFilters
    ) -> CursorJSON[list[Video]]:
        videos = await video_service.filter_videos(data)
        cursor = videos[-1].id if videos else None
        return CursorJSON(data=videos, cursor=cursor)

    @post(
        path="/",
        summary="Add a new video",
        dto=VideoCreate,
        return_dto=None,
    )
    async def add_video(
        self, video_service: VideoService, data: DTOData[Video]
    ) -> JSON[Video]:
        return JSON(await video_service.add_video(data.create_instance()))

    @get(
        path="/{video_id:uuid}",
        summary="Get a video by ID",
    )
    async def get_video(
        self, video_service: VideoService, video_id: UUID
    ) -> JSON[Video | None]:
        return JSON(await video_service.get_video_by_id(video_id))

    @patch(
        path="/{video_id:uuid}",
        summary="Update a video by ID",
        dto=VideoPatch,
        return_dto=None,
    )
    async def patch_video(
        self, video_service: VideoService, video_id: UUID, data: DTOData[Video]
    ) -> JSON[Video]:
        return JSON(await video_service.patch_video(video_id, data))

    @delete(
        path="/{video_id:uuid}",
        summary="Delete a video by ID",
    )
    async def delete_video(self, video_service: VideoService, video_id: UUID) -> None:
        await video_service.delete_video(video_id)

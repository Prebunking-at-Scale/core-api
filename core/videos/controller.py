from uuid import UUID

from litestar import Controller, delete, get, patch, post
from litestar.di import Provide
from litestar.dto import DTOData

from core.response import JSON
from core.uow import ConnectionFactory
from core.videos.models import VideoCreate, VideoPatch, Video
from core.videos.service import VideoService


async def video_service(connection_factory: ConnectionFactory) -> VideoService:
    return VideoService(connection_factory=connection_factory)


class VideoController(Controller):
    path = "/videos"
    tags = ["videos"]

    dependencies = {
        "video_service": Provide(video_service),
    }

    @get(
        path="/{video_id:uuid}",
        summary="Get a video by ID",
    )
    async def get_video(
        self, video_service: VideoService, video_id: UUID
    ) -> JSON[Video | None]:
        return JSON(await video_service.get_video_by_id(video_id))

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

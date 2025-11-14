from typing import Any
from litestar import Controller, Response, get
from litestar.datastructures import State
from litestar.di import Provide
from core.languages.models import LanguageWithVideoCount
from core.response import JSON
from core.videos.service import VideoService

async def video_service(state: State) -> VideoService:
    return VideoService(state.connection_factory)

class LanguageController(Controller):
    path = "/languages"
    tags = ["languages"]

    dependencies = {
        "video_service": Provide(video_service),
    }

    @get(
        path="/",
        summary="Get a list of languages associated with videos and its count",
    )
    async def get_languages(self,
        video_service: VideoService,
    ) -> Response[JSON[list[LanguageWithVideoCount]]]:
        languages = await video_service.get_languages_associated_with_videos()
        return Response(JSON(languages))

import logging
import os
from uuid import UUID

from litestar import Controller, Response, delete, get, patch, post
from litestar.background_tasks import BackgroundTask
from litestar.datastructures import State
from litestar.di import Provide
from litestar.dto import DTOData
from litestar.exceptions import NotFoundException

from core.analysis import genai
from core.errors import ConflictError
from core.response import JSON, CursorJSON
from core.videos.claims.models import Claim, VideoClaims
from core.videos.claims.service import ClaimsService
from core.videos.models import AnalysedVideo, Video, VideoFilters, VideoPatch
from core.videos.service import VideoService
from core.videos.transcripts.models import Transcript, TranscriptSentence
from core.videos.transcripts.service import TranscriptService

log = logging.getLogger(__name__)


async def video_service(state: State) -> VideoService:
    return VideoService(state.connection_factory)


async def transcript_service(state: State) -> TranscriptService:
    return TranscriptService(state.connection_factory)


async def claims_service(state: State) -> ClaimsService:
    return ClaimsService(state.connection_factory)


async def extract_transcript_and_claims(
    video: Video, transcript_service: TranscriptService, claims_service: ClaimsService
) -> None:
    if "PYTEST_CURRENT_TEST" in os.environ:
        # We don't want to run this during tests
        return

    if video.platform.lower() != "youtube":
        return

    result = await genai.generate_transcript(video.source_url)
    sentences = [TranscriptSentence(**x.model_dump()) for x in result]
    claims = [
        Claim(**(x.model_dump() | {"claim": x.text})) for x in result if x.is_claim
    ]

    if sentences:
        transcript = Transcript(video_id=video.id, sentences=sentences)
        await transcript_service.add_transcript(video.id, transcript)

    if claims:
        video_claims = VideoClaims(video_id=video.id, claims=claims)
        await claims_service.add_claims(video.id, video_claims)

    log.info(
        f"finished processing {video.source_url}, got {len(sentences)} sentences and {len(claims)} claims."
    )


class VideoController(Controller):
    path = "/videos"
    tags = ["videos"]

    dependencies = {
        "video_service": Provide(video_service),
        "transcript_service": Provide(transcript_service),
        "claims_service": Provide(claims_service),
    }

    @post(
        path="/",
        summary="Add a new video",
        raises=[ConflictError],
    )
    async def add_video(
        self,
        video_service: VideoService,
        transcript_service: TranscriptService,
        claims_service: ClaimsService,
        data: Video,
    ) -> Response[JSON[Video]]:
        video = await video_service.add_video(data)
        return Response(
            JSON(video),
            background=BackgroundTask(
                extract_transcript_and_claims, video, transcript_service, claims_service
            ),
        )

    @get(
        path="/{video_id:uuid}",
        summary="Get a video by ID",
    )
    async def get_video(
        self,
        video_service: VideoService,
        transcript_service: TranscriptService,
        claims_service: ClaimsService,
        video_id: UUID,
    ) -> JSON[AnalysedVideo | None]:
        video = await video_service.get_video_by_id(video_id)
        if not video:
            raise NotFoundException()
        transcript = await transcript_service.get_transcript_for_video(video_id)
        claims = await claims_service.get_claims_for_video(video_id)

        return JSON(
            AnalysedVideo(
                **video.model_dump(),
                transcript=transcript,
                claims=claims,
            )
        )

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

    @post(
        path="/filter",
        summary="Get all or a filtered subset of videos",
        status_code=200,
    )
    async def filter_videos(
        self,
        video_service: VideoService,
        data: VideoFilters,
    ) -> CursorJSON[list[Video]]:
        videos = await video_service.filter_videos(data)
        cursor = videos[-1].id if videos else None
        return CursorJSON(data=videos, cursor=cursor)

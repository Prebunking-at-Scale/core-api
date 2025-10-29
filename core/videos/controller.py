import logging
import os
from uuid import UUID

import httpx
from harmful_claim_finder.transcript_inference import get_claims
from harmful_claim_finder.utils.models import (
    TranscriptSentence as HarmfulClaimFinderSentence,
)
from litestar import Controller, Response, delete, get, patch, post
from litestar.background_tasks import BackgroundTask
from litestar.datastructures import State
from litestar.di import Provide
from litestar.dto import DTOData
from litestar.exceptions import NotFoundException
from litestar.params import Parameter

from core.analysis import genai, language_id
from core.auth.guards import super_admin
from core.config import APP_BASE_URL, NARRATIVES_API_KEY, NARRATIVES_BASE_URL
from core.errors import ConflictError
from core.media_feeds.service import MediaFeedsService
from core.models import Claim, Transcript, TranscriptSentence, Video
from core.narratives.service import NarrativeService
from core.response import JSON, CursorJSON, PaginatedJSON
from core.videos.claims.models import VideoClaims
from core.videos.claims.service import ClaimsService
from core.videos.models import (
    AnalysedVideo,
    VideoFilters,
    VideoPatch,
)
from core.videos.service import VideoService
from core.videos.transcripts.service import TranscriptService

log = logging.getLogger(__name__)


async def video_service(state: State) -> VideoService:
    return VideoService(state.connection_factory)


async def transcript_service(state: State) -> TranscriptService:
    return TranscriptService(state.connection_factory)


async def claims_service(state: State) -> ClaimsService:
    return ClaimsService(state.connection_factory)


async def narrative_service(state: State) -> NarrativeService:
    return NarrativeService(state.connection_factory)


async def media_feeds_service(state: State) -> MediaFeedsService:
    return MediaFeedsService(state.connection_factory)


async def extract_transcript_and_claims(
    video: Video,
    video_service: VideoService,
    transcript_service: TranscriptService,
    claims_service: ClaimsService,
    media_feeds_service: MediaFeedsService,
) -> None:
    if "PYTEST_CURRENT_TEST" in os.environ:
        # We don't want to run this during tests
        return

    if video.platform.lower() != "youtube":
        return

    result = await genai.generate_transcript(video.source_url)
    sentences = [TranscriptSentence(**x.model_dump()) for x in result]
    all_text = " ".join([s.text for s in sentences])
    overall_language: str | None = (
        language_id.predict_language(all_text) if all_text.strip() else None
    )
    for sentence in sentences:
        sentence.metadata["language"] = language_id.predict_language(sentence.text)

    if sentences:
        transcript = Transcript(video_id=video.id, sentences=sentences)
        await transcript_service.add_transcript(video.id, transcript)

    if overall_language:
        video.metadata["language"] = overall_language
        await video_service.patch_video(video.id, video)

    orgs: list[str] = video.metadata.get("for_organisation", [])
    if not orgs:
        log.warning("could not find organisation list on video")
        return

    all_claims: list[Claim] = []
    for org in orgs:
        try:
            org_uuid = UUID(org)
            keyword_feeds = await media_feeds_service.get_keyword_feeds(org_uuid)
            keywords = {feed.topic: feed.keywords for feed in keyword_feeds}
            if not keywords:
                log.error(f"org {org} not found")
                continue

            claims = await get_claims(
                keywords=keywords,
                sentences=[
                    HarmfulClaimFinderSentence(
                        **(s.model_dump() | {"video_id": video.id})
                    )
                    for s in sentences
                ],
            )

            for claim in claims:
                formatted_claim: Claim = Claim(**claim.model_dump())
                formatted_claim.metadata["for_organisation"] = org
                all_claims.append(formatted_claim)

        except Exception as e:
            log.exception(e)

    if all_claims:
        await claims_service.add_claims(
            video.id, VideoClaims(video_id=video.id, claims=all_claims)
        )

    log.info(
        f"finished processing {video.source_url}, got {len(sentences)} sentences and {len(all_claims)} claims."
    )

    # Send video to narratives API for analysis
    if all_claims:
        await analyze_for_narratives(video, all_claims)


async def analyze_for_narratives(video: Video, video_claims: list[Claim]) -> None:
    """Send video claims to the narratives API for analysis."""

    if "PYTEST_CURRENT_TEST" in os.environ:
        # We don't want to run this during tests
        return

    if not NARRATIVES_BASE_URL or not NARRATIVES_API_KEY:
        log.warning("Narratives API configuration missing, skipping narrative analysis")
        return

    if not APP_BASE_URL:
        log.warning("APP_BASE_URL not configured, skipping narrative analysis")
        return

    claims_data = []
    for claim in video_claims:
        claims_data.append({
            "id": str(claim.id),
            "claim": claim.claim,
            "score": claim.metadata.get("score", 0),
            "video_id": str(video.id),
        })

    payload = {"claims": claims_data}

    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            url = f"{NARRATIVES_BASE_URL}/add-contents"
            response = await client.post(
                url, json=payload, headers={"X-API-TOKEN": NARRATIVES_API_KEY}
            )

            if response.status_code == 202:
                log.debug(
                    f"Successfully sent {len(claims_data)} claims to narratives API"
                )
            else:
                log.error(
                    f"Failed to analyze claims: {response.status_code} - {response.text}"
                )

        except Exception as e:
            log.error(f"Error sending claims to narratives API: {e}", exc_info=True)


class VideoController(Controller):
    path = "/videos"
    tags = ["videos"]

    dependencies = {
        "video_service": Provide(video_service),
        "transcript_service": Provide(transcript_service),
        "claims_service": Provide(claims_service),
        "narrative_service": Provide(narrative_service),
        "media_feeds_service": Provide(media_feeds_service),
    }

    @post(
        path="/",
        summary="Add a new video",
        raises=[ConflictError],
        guards=[super_admin],
    )
    async def add_video(
        self,
        video_service: VideoService,
        transcript_service: TranscriptService,
        claims_service: ClaimsService,
        media_feeds_service: MediaFeedsService,
        data: Video,
    ) -> Response[JSON[Video]]:
        video = await video_service.add_video(data)
        return Response(
            JSON(video),
            background=BackgroundTask(
                extract_transcript_and_claims,
                video,
                video_service,
                transcript_service,
                claims_service,
                media_feeds_service,
            ),
        )

    @get(
        path="/",
        summary="Get a paginated list of videos with optional filters",
    )
    async def list_videos(
        self,
        video_service: VideoService,
        transcript_service: TranscriptService,
        claims_service: ClaimsService,
        platform: str | None = Parameter(None, query="platform"),
        channel: str | None = Parameter(None, query="channel"),
        text: str | None = Parameter(None, query="text"),
        video_language: str | None = Parameter(None, query="video_language"),
        limit: int = Parameter(25, query="limit", gt=0, le=100),
        offset: int = Parameter(0, query="offset", ge=0),
    ) -> PaginatedJSON[list[AnalysedVideo]]:
        videos, total = await video_service.get_videos_paginated(
            limit=limit,
            offset=offset,
            platform=platform,
            channel=channel,
            text=text,
            video_language=video_language,
        )

        # Fetch claims and narratives for each video
        analysed_videos = []
        for video in videos:
            transcript = await transcript_service.get_transcript_for_video(video.id)
            claims = await claims_service.get_claims_for_video(video.id)
            narratives = await video_service.get_narratives_for_video(video.id)

            analysed_videos.append(
                AnalysedVideo(
                    **video.model_dump(),
                    transcript=transcript,
                    claims=claims,
                    narratives=narratives,
                )
            )

        page = (offset // limit) + 1 if limit > 0 else 1
        return PaginatedJSON(
            data=analysed_videos,
            total=total,
            page=page,
            size=limit,
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
        narratives = await video_service.get_narratives_for_video(video_id)

        return JSON(
            AnalysedVideo(
                **video.model_dump(),
                transcript=transcript,
                claims=claims,
                narratives=narratives,
            )
        )

    @patch(
        path="/{video_id:uuid}",
        summary="Update a video by ID",
        dto=VideoPatch,
        return_dto=None,
        guards=[super_admin],
    )
    async def patch_video(
        self, video_service: VideoService, video_id: UUID, data: DTOData[Video]
    ) -> JSON[Video]:
        return JSON(await video_service.patch_video(video_id, data))

    @delete(
        path="/{video_id:uuid}",
        summary="Delete a video by ID",
        guards=[super_admin],
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

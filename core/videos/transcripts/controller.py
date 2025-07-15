from typing import Any
from uuid import UUID

from litestar import Controller, delete, get, patch, post
from litestar.di import Provide
from litestar.dto import DTOData
from litestar.exceptions import NotFoundException

from core.response import JSON
from core.uow import ConnectionFactory
from core.videos.transcripts.models import (
    Transcript,
    TranscriptDTO,
)
from core.videos.transcripts.service import TranscriptService


async def transcript_service(
    connection_factory: ConnectionFactory,
) -> TranscriptService:
    return TranscriptService(connection_factory=connection_factory)


class TranscriptController(Controller):
    path = "/videos/{video_id:uuid}/transcript"
    tags = ["transcripts"]

    dependencies = {
        "transcript_service": Provide(transcript_service),
    }

    @post(
        path="/",
        summary="Add new transcript sentences for a video",
        dto=TranscriptDTO,
        return_dto=None,
    )
    async def add_sentences(
        self,
        transcript_service: TranscriptService,
        video_id: UUID,
        data: DTOData[Transcript],
    ) -> JSON[Transcript]:
        return JSON(await transcript_service.add_transcript(video_id, data))

    @get(
        path="/",
        summary="Get a transcript for the given video",
    )
    async def get_transcript(
        self, transcript_service: TranscriptService, video_id: UUID
    ) -> JSON[Transcript]:
        transcript = await transcript_service.get_transcript_for_video(video_id)
        if not transcript:
            raise NotFoundException()
        return JSON(transcript)

    @delete(
        path="/",
        summary="Delete all transcript sentences for the given video",
    )
    async def delete_transcript(
        self, transcript_service: TranscriptService, video_id: UUID
    ) -> None:
        await transcript_service.delete_transcript(video_id)

    @patch(
        path="/{sentence_id:uuid}/metadata",
        summary="Update the metadata for a transcript sentence",
    )
    async def patch_sentence_metadata(
        self,
        transcript_service: TranscriptService,
        sentence_id: UUID,
        data: dict[str, Any],
    ) -> JSON[dict[str, Any]]:
        return JSON(await transcript_service.update_metadata(sentence_id, data))

    @delete(
        path="/{sentence_id:uuid}",
        summary="Delete a specific transcript sentence",
    )
    async def delete_sentence(
        self,
        transcript_service: TranscriptService,
        sentence_id: UUID,
    ) -> None:
        await transcript_service.delete_sentence(sentence_id)

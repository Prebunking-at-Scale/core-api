from typing import Any, AsyncContextManager
from uuid import UUID

from litestar.dto import DTOData

from core.uow import ConnectionFactory, uow
from core.videos.transcripts.models import Transcript, TranscriptSentence
from core.videos.transcripts.repo import TranscriptRepository


class TranscriptService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[TranscriptRepository]:
        return uow(TranscriptRepository, self._connection_factory)

    async def add_transcript(
        self,
        video_id: UUID,
        transcript: Transcript | DTOData[Transcript],
    ) -> Transcript:
        if isinstance(transcript, DTOData):
            transcript = transcript.create_instance(video_id=video_id)

        async with self.repo() as repo:
            sentences = await repo.add_sentences(video_id, transcript.sentences)
        return Transcript(video_id=video_id, sentences=sentences)

    async def get_transcript_for_video(self, video_id: UUID) -> Transcript | None:
        async with self.repo() as repo:
            if not await repo.video_exists(video_id):
                return None
            sentences = await repo.get_transcript_for_video(video_id)
        return Transcript(video_id=video_id, sentences=sentences)

    async def delete_transcript(self, video_id: UUID) -> None:
        async with self.repo() as repo:
            await repo.delete_transcript(video_id)

    async def update_metadata(
        self, sentence_id: UUID, metadata: dict[str, Any]
    ) -> dict[str, Any]:
        async with self.repo() as repo:
            return await repo.update_sentence_metadata(sentence_id, metadata)

    async def delete_sentence(self, sentence_id: UUID) -> None:
        async with self.repo() as repo:
            return await repo.delete_sentence(sentence_id)

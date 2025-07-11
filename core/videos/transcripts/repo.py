from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.videos.transcripts.models import TranscriptSentence


class TranscriptRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def add_sentences(
        self, video_id: UUID, sentences: list[TranscriptSentence]
    ) -> list[TranscriptSentence]:
        await self._session.executemany(
            """
            INSERT INTO transcript_sentences (
                video_id, source, text, start_time_s, metadata
            ) VALUES (
                %(video_id)s, %(source)s, %(text)s, %(start_time_s)s, %(metadata)s
            )
            RETURNING *
            """,
            [
                x.model_dump()
                | {
                    "metadata": Jsonb(x.metadata),
                    "video_id": video_id,
                }
                for x in sentences
            ],
            returning=True,
        )

        added_sentences = []
        while True:
            row = await self._session.fetchone()
            if not row:
                break
            added_sentences.append(TranscriptSentence(**row))
            if not self._session.nextset():
                break

        return added_sentences

    async def get_transcript_for_video(
        self, video_id: UUID
    ) -> list[TranscriptSentence]:
        await self._session.execute(
            """
            SELECT * FROM transcript_sentences
            WHERE video_id = %(video_id)s
            ORDER BY start_time_s ASC
            """,
            {"video_id": video_id},
        )
        return [TranscriptSentence(**row) for row in await self._session.fetchall()]

    async def delete_transcript(self, video_id: UUID) -> None:
        await self._session.execute(
            """
            DELETE FROM transcript_sentences
            WHERE video_id = %(video_id)s
            """,
            {"video_id": video_id},
        )

    async def update_sentence_metadata(
        self, sentence_id: UUID, metadata: dict[str, Any]
    ) -> TranscriptSentence:
        await self._session.execute(
            """
            UPDATE transcript_sentences
            SET metadata = metadata || %(metadata)s
            WHERE id = %(sentence_id)s
            RETURNING *
            """,
            {"sentence_id": sentence_id, "metadata": Jsonb(metadata)},
        )
        row = await self._session.fetchone()
        if not row:
            raise ValueError("Sentence not found")
        return TranscriptSentence(**row)

    async def delete_sentence(self, sentence_id: UUID) -> None:
        await self._session.execute(
            """
            DELETE FROM transcript_sentences WHERE id = %(sentence_id)s
            """,
            {"sentence_id": sentence_id},
        )

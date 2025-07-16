from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.analysis import embedding
from core.errors import ConflictError
from core.videos.transcripts.models import TranscriptSentence


class TranscriptRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def add_sentences(
        self, video_id: UUID, sentences: list[TranscriptSentence]
    ) -> list[TranscriptSentence]:
        try:
            await self._session.executemany(
                """
                INSERT INTO transcript_sentences (
                    id, video_id, source, text, start_time_s, metadata, embedding
                ) VALUES (
                    %(id)s,
                    %(video_id)s,
                    %(source)s,
                    %(text)s,
                    %(start_time_s)s,
                    %(metadata)s,
                    %(embedding)s
                )
                RETURNING *, embedding::real[]
                """,
                [
                    x.model_dump()
                    | {
                        "metadata": Jsonb(x.metadata),
                        "video_id": video_id,
                        "embedding": list(embedding.encode(x.text)),
                    }
                    for x in sentences
                ],
                returning=True,
            )
        except psycopg.errors.UniqueViolation:
            raise ConflictError("video ids must be unique")

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
            SELECT *, embedding::real[] FROM transcript_sentences
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
    ) -> dict[str, Any]:
        await self._session.execute(
            """
            UPDATE transcript_sentences
            SET
                metadata = metadata || %(metadata)s,
                updated_at = now()
            WHERE id = %(sentence_id)s
            RETURNING metadata
            """,
            {"sentence_id": sentence_id, "metadata": Jsonb(metadata)},
        )
        row = await self._session.fetchone()
        if not row:
            raise ValueError("Sentence not found")
        return row["metadata"]

    async def delete_sentence(self, sentence_id: UUID) -> None:
        await self._session.execute(
            """
            DELETE FROM transcript_sentences WHERE id = %(sentence_id)s
            """,
            {"sentence_id": sentence_id},
        )

    async def video_exists(self, video_id: UUID) -> bool:
        await self._session.execute(
            """
            SELECT 1 FROM videos WHERE id = %(video_id)s
            """,
            {"video_id": video_id},
        )
        return (await self._session.fetchone()) is not None

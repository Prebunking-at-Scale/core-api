from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.analysis import embedding
from core.videos.claims.models import Claim


class ClaimRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def add_claims(self, video_id: UUID, claims: list[Claim]) -> list[Claim]:
        await self._session.executemany(
            """
            INSERT INTO video_claims (
                id, video_id, claim, start_time_s, metadata, embedding
            ) VALUES (
                %(id)s, %(video_id)s, %(claim)s, %(start_time_s)s, %(metadata)s, %(embedding)s
            )
            RETURNING *
            """,
            [
                x.model_dump()
                | {
                    "metadata": Jsonb(x.metadata),
                    "video_id": video_id,
                    "embedding": list(embedding.encode(x.claim)),
                }
                for x in claims
            ],
            returning=True,
        )

        added_claims = []
        while True:
            row = await self._session.fetchone()
            if not row:
                break
            added_claims.append(Claim(**row))
            if not self._session.nextset():
                break

        return added_claims

    async def get_claims_for_video(self, video_id: UUID) -> list[Claim]:
        await self._session.execute(
            """
            SELECT * FROM video_claims
            WHERE video_id = %(video_id)s
            ORDER BY start_time_s ASC
            """,
            {"video_id": video_id},
        )
        return [Claim(**row) for row in await self._session.fetchall()]

    async def delete_video_claims(self, video_id: UUID) -> None:
        await self._session.execute(
            """
            DELETE FROM video_claims
            WHERE video_id = %(video_id)s
            """,
            {"video_id": video_id},
        )

    async def update_claim_metadata(
        self, claim_id: UUID, metadata: dict[str, Any]
    ) -> dict[str, Any]:
        await self._session.execute(
            """
            UPDATE video_claims
            SET
                metadata = metadata || %(metadata)s,
                updated_at = now()
            WHERE id = %(claim_id)s
            RETURNING *
            """,
            {"claim_id": claim_id, "metadata": Jsonb(metadata)},
        )
        row = await self._session.fetchone()
        if not row:
            raise ValueError("claim not found")
        return row["metadata"]

    async def delete_claim(self, claim_id: UUID) -> None:
        await self._session.execute(
            """
            DELETE FROM video_claims WHERE id = %(claim_id)s
            """,
            {"claim_id": claim_id},
        )

    async def video_exists(self, video_id: UUID) -> bool:
        await self._session.execute(
            """
            SELECT 1 FROM videos WHERE id = %(video_id)s
            """,
            {"video_id": video_id},
        )
        return (await self._session.fetchone()) is not None

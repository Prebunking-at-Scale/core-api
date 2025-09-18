from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.analysis import embedding
from core.errors import ConflictError
from core.models import Claim, Entity, Narrative, Topic, Video
from core.videos.claims.models import EnrichedClaim


class ClaimRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def add_claims(self, video_id: UUID, claims: list[Claim]) -> list[Claim]:
        if not claims:
            return []

        try:
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
        except psycopg.errors.UniqueViolation:
            raise ConflictError("video ids must be unique")

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
            SELECT *
            FROM video_claims
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

    async def get_claims_by_topic(
        self, topic_id: UUID, limit: int = 100, offset: int = 0
    ) -> tuple[list[EnrichedClaim], int]:
        # Get total count
        await self._session.execute(
            """
            SELECT COUNT(DISTINCT c.id)
            FROM video_claims c
            JOIN claim_topics ct ON c.id = ct.claim_id
            WHERE ct.topic_id = %(topic_id)s
            """,
            {"topic_id": topic_id},
        )
        total_row = await self._session.fetchone()
        total = total_row["count"] if total_row else 0

        # Get claims
        await self._session.execute(
            """
            SELECT DISTINCT c.*
            FROM video_claims c
            JOIN claim_topics ct ON c.id = ct.claim_id
            WHERE ct.topic_id = %(topic_id)s
            ORDER BY c.created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"topic_id": topic_id, "limit": limit, "offset": offset},
        )

        claims = []
        for row in await self._session.fetchall():
            topics = await self._get_claim_topics(row["id"])
            entities = await self._get_claim_entities(row["id"])
            video = (
                await self._get_video_info(row["video_id"])
                if row.get("video_id")
                else None
            )
            narratives = await self._get_claim_narratives(row["id"])
            claims.append(
                EnrichedClaim(topics=topics, entities=entities, video=video, narratives=narratives, **row)
            )

        return claims, total

    async def get_claims_by_entity(
        self, entity_id: UUID, limit: int = 100, offset: int = 0
    ) -> tuple[list[EnrichedClaim], int]:
        # Get total count
        await self._session.execute(
            """
            SELECT COUNT(DISTINCT c.id)
            FROM video_claims c
            JOIN claim_entities ce ON c.id = ce.claim_id
            WHERE ce.entity_id = %(entity_id)s
            """,
            {"entity_id": entity_id},
        )
        total_row = await self._session.fetchone()
        total = total_row["count"] if total_row else 0

        # Get claims
        await self._session.execute(
            """
            SELECT DISTINCT c.*
            FROM video_claims c
            JOIN claim_entities ce ON c.id = ce.claim_id
            WHERE ce.entity_id = %(entity_id)s
            ORDER BY c.created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"entity_id": entity_id, "limit": limit, "offset": offset},
        )

        claims = []
        for row in await self._session.fetchall():
            topics = await self._get_claim_topics(row["id"])
            entities = await self._get_claim_entities(row["id"])
            video = (
                await self._get_video_info(row["video_id"])
                if row.get("video_id")
                else None
            )
            narratives = await self._get_claim_narratives(row["id"])
            claims.append(
                EnrichedClaim(topics=topics, entities=entities, video=video, narratives=narratives, **row)
            )

        return claims, total

    async def _get_claim_topics(self, claim_id: UUID) -> list[Topic]:
        await self._session.execute(
            """
            SELECT t.*
            FROM topics t
            JOIN claim_topics ct ON t.id = ct.topic_id
            WHERE ct.claim_id = %(claim_id)s
            ORDER BY t.topic
            """,
            {"claim_id": claim_id},
        )
        return [Topic(**row) for row in await self._session.fetchall()]

    async def _get_claim_entities(self, claim_id: UUID) -> list[Entity]:
        await self._session.execute(
            """
            SELECT e.*
            FROM entities e
            JOIN claim_entities ce ON e.id = ce.entity_id
            WHERE ce.claim_id = %(claim_id)s
            ORDER BY e.name
            """,
            {"claim_id": claim_id},
        )
        return [Entity(**row) for row in await self._session.fetchall()]

    async def _get_claim_narratives(self, claim_id: UUID) -> list[Narrative]:
        await self._session.execute(
            """
            SELECT n.id, n.title, n.description, n.metadata, n.created_at, n.updated_at
            FROM narratives n
            JOIN claim_narratives cn ON n.id = cn.narrative_id
            WHERE cn.claim_id = %(claim_id)s
            ORDER BY n.created_at DESC
            """,
            {"claim_id": claim_id},
        )
        return [Narrative(**row) for row in await self._session.fetchall()]

    async def _get_video_info(self, video_id: UUID) -> Video | None:
        await self._session.execute(
            """
            SELECT id, title, description, platform, source_url, channel,
                   uploaded_at, views, likes, comments, metadata
            FROM videos
            WHERE id = %(video_id)s
            """,
            {"video_id": video_id},
        )
        row = await self._session.fetchone()
        return Video(**row) if row else None

    async def get_all_claims(
        self, 
        limit: int = 100, 
        offset: int = 0, 
        topic_id: UUID | None = None,
        text: str | None = None,
        min_score: float | None = None,
        max_score: float | None = None
    ) -> tuple[list[EnrichedClaim], int]:
        # Build the query conditionally
        where_conditions = []
        params: dict[str, int | UUID | str | float] = {"limit": limit, "offset": offset}

        if topic_id:
            where_conditions.append("ct.topic_id = %(topic_id)s")
            params["topic_id"] = topic_id
            
        if text:
            where_conditions.append("LOWER(c.claim) LIKE LOWER(%(text)s)")
            params["text"] = f"%{text}%"
            
        if min_score is not None:
            where_conditions.append("(c.metadata->>'score')::float >= %(min_score)s")
            params["min_score"] = min_score
            
        if max_score is not None:
            where_conditions.append("(c.metadata->>'score')::float <= %(max_score)s")
            params["max_score"] = max_score
            
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

        # Get total count
        count_query = f"""
            SELECT COUNT(DISTINCT c.id)
            FROM video_claims c
            {"JOIN claim_topics ct ON c.id = ct.claim_id" if topic_id else ""}
            {where_clause}
        """
        await self._session.execute(count_query, params)
        total_row = await self._session.fetchone()
        total = total_row["count"] if total_row else 0

        # Get claims
        claims_query = f"""
            SELECT DISTINCT c.*
            FROM video_claims c
            {"JOIN claim_topics ct ON c.id = ct.claim_id" if topic_id else ""}
            {where_clause}
            ORDER BY c.created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        await self._session.execute(claims_query, params)

        claims = []
        for row in await self._session.fetchall():
            topics = await self._get_claim_topics(row["id"])
            entities = await self._get_claim_entities(row["id"])
            video = (
                await self._get_video_info(row["video_id"])
                if row.get("video_id")
                else None
            )
            narratives = await self._get_claim_narratives(row["id"])
            claims.append(
                EnrichedClaim(topics=topics, entities=entities, video=video, narratives=narratives, **row)
            )

        return claims, total

    async def associate_topics_with_claim(
        self, claim_id: UUID, topic_ids: list[UUID]
    ) -> None:
        # First, remove existing associations
        await self._session.execute(
            """
            DELETE FROM claim_topics
            WHERE claim_id = %(claim_id)s
            """,
            {"claim_id": claim_id},
        )

        # Then add new associations
        if topic_ids:
            await self._session.executemany(
                """
                INSERT INTO claim_topics (claim_id, topic_id)
                VALUES (%(claim_id)s, %(topic_id)s)
                """,
                [
                    {"claim_id": claim_id, "topic_id": topic_id}
                    for topic_id in topic_ids
                ],
            )

    async def get_claim_by_id(self, claim_id: UUID) -> EnrichedClaim | None:
        await self._session.execute(
            """
            SELECT * FROM video_claims
            WHERE id = %(claim_id)s
            """,
            {"claim_id": claim_id},
        )
        row = await self._session.fetchone()
        if not row:
            return None

        topics = await self._get_claim_topics(claim_id)
        entities = await self._get_claim_entities(claim_id)
        video = (
            await self._get_video_info(row["video_id"]) if row.get("video_id") else None
        )
        narratives = await self._get_claim_narratives(claim_id)
        return EnrichedClaim(topics=topics, entities=entities, video=video, narratives=narratives, **row)

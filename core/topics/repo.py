from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.errors import ConflictError
from core.topics.models import Topic, TopicWithStats


class TopicRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def create_topic(
        self, topic: str, metadata: dict[str, Any]
    ) -> Topic:
        try:
            await self._session.execute(
                """
                INSERT INTO topics (topic, metadata)
                VALUES (%(topic)s, %(metadata)s)
                RETURNING *
                """,
                {
                    "topic": topic,
                    "metadata": Jsonb(metadata),
                },
            )
        except psycopg.errors.UniqueViolation:
            raise ConflictError("topic already exists")

        row = await self._session.fetchone()
        if not row:
            raise ValueError("failed to create topic")

        return Topic(**row)

    async def get_topic(self, topic_id: UUID) -> Topic | None:
        await self._session.execute(
            """
            SELECT * FROM topics
            WHERE id = %(topic_id)s
            """,
            {"topic_id": topic_id},
        )
        row = await self._session.fetchone()
        if not row:
            return None
        return Topic(**row)

    async def get_topic_by_name(self, topic: str) -> Topic | None:
        await self._session.execute(
            """
            SELECT * FROM topics
            WHERE topic = %(topic)s
            """,
            {"topic": topic},
        )
        row = await self._session.fetchone()
        if not row:
            return None
        return Topic(**row)

    async def get_all_topics(
        self, limit: int = 100, offset: int = 0
    ) -> list[Topic]:
        await self._session.execute(
            """
            SELECT * FROM topics
            ORDER BY topic
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"limit": limit, "offset": offset},
        )
        return [Topic(**row) for row in await self._session.fetchall()]

    async def search_topics(self, query: str) -> list[Topic]:
        await self._session.execute(
            """
            SELECT * FROM topics
            WHERE topic ILIKE %(query)s
            ORDER BY topic
            LIMIT 50
            """,
            {"query": f"%{query}%"},
        )
        return [Topic(**row) for row in await self._session.fetchall()]

    async def update_topic(
        self,
        topic_id: UUID,
        topic: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Topic | None:
        updates = []
        params = {"topic_id": topic_id}

        if topic is not None:
            updates.append("topic = %(topic)s")
            params["topic"] = topic

        if metadata is not None:
            updates.append("metadata = metadata || %(metadata)s")
            params["metadata"] = Jsonb(metadata)

        if not updates:
            return await self.get_topic(topic_id)

        updates.append("updated_at = now()")
        
        try:
            await self._session.execute(
                f"""
                UPDATE topics
                SET {", ".join(updates)}
                WHERE id = %(topic_id)s
                RETURNING *
                """,
                params,
            )
        except psycopg.errors.UniqueViolation:
            raise ConflictError("topic already exists")

        row = await self._session.fetchone()
        if not row:
            return None
        return Topic(**row)

    async def delete_topic(self, topic_id: UUID) -> None:
        await self._session.execute(
            """
            DELETE FROM topics WHERE id = %(topic_id)s
            """,
            {"topic_id": topic_id},
        )

    async def get_topics_by_narrative(self, narrative_id: UUID) -> list[Topic]:
        await self._session.execute(
            """
            SELECT t.*
            FROM topics t
            JOIN narrative_topics nt ON t.id = nt.topic_id
            WHERE nt.narrative_id = %(narrative_id)s
            ORDER BY t.topic
            """,
            {"narrative_id": narrative_id},
        )
        return [Topic(**row) for row in await self._session.fetchall()]
    
    async def get_all_topics_with_stats(
        self, limit: int = 100, offset: int = 0
    ) -> tuple[list[TopicWithStats], int]:
        # Get total count
        await self._session.execute("SELECT COUNT(*) FROM topics")
        total_row = await self._session.fetchone()
        total = total_row["count"] if total_row else 0
        
        # Get topics with stats
        await self._session.execute(
            """
            SELECT 
                t.*,
                COUNT(DISTINCT nt.narrative_id) as narrative_count,
                COUNT(DISTINCT ct.claim_id) as claim_count
            FROM topics t
            LEFT JOIN narrative_topics nt ON t.id = nt.topic_id
            LEFT JOIN claim_topics ct ON t.id = ct.topic_id
            GROUP BY t.id
            ORDER BY t.topic
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"limit": limit, "offset": offset},
        )
        topics = [TopicWithStats(**row) for row in await self._session.fetchall()]
        return topics, total
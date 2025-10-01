from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.errors import ConflictError
from core.models import Topic
from core.topics.models import TopicWithStats


class TopicRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def create_topic(self, topic: str, metadata: dict[str, Any]) -> Topic:
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

    async def get_all_topics(self, limit: int = 100, offset: int = 0) -> list[Topic]:
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
        params: dict[str, Any] = {"topic_id": topic_id}

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
        self, limit: int = 100, offset: int = 0, start_date: str | None = None, end_date: str | None = None
    ) -> tuple[list[TopicWithStats], int]:
        """Can handle filtering by created_at date range"""
        # Get total count
        await self._session.execute("SELECT COUNT(*) FROM topics")
        total_row = await self._session.fetchone()
        total = total_row["count"] if total_row else 0

        params: dict[str, int | str] = {"limit": limit, "offset": offset}

        narrative_count_query = f"""SELECT COUNT(*) FROM narrative_topics nt join narratives n on nt.narrative_id = n.id WHERE nt.topic_id = t.id"""
        claim_count_query = f"""SELECT COUNT(*) from claim_topics ct join video_claims vc on ct.claim_id = vc.id WHERE ct.topic_id = t.id"""

        if start_date and end_date:
            # concatenate the two date filters to avoid SQL syntax errors
            narrative_count_query += " AND n.created_at BETWEEN %(start_date)s AND %(end_date)s"
            claim_count_query += " AND vc.created_at BETWEEN %(start_date)s AND %(end_date)s"
            params["start_date"] = start_date
            params["end_date"] = end_date
        elif start_date:
            narrative_count_query += " AND n.created_at >= %(start_date)s"
            claim_count_query += " AND vc.created_at >= %(start_date)s"
            params["start_date"] = start_date
        elif end_date:
            narrative_count_query += " AND n.created_at <= %(end_date)s"
            claim_count_query += " AND vc.created_at <= %(end_date)s"
            params["end_date"] = end_date

        # Get topics with stats
        await self._session.execute(
            f"""
            SELECT
                t.*,
                ({narrative_count_query}) AS narrative_count,
                ({claim_count_query}) AS claim_count
            FROM topics t
            ORDER BY t.topic
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            params,
        )
        topics = [TopicWithStats(**row) for row in await self._session.fetchall()]
        return topics, total

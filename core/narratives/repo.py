from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.errors import ConflictError
from core.models import Claim, Entity, Narrative, Topic, Video
from core.narratives.models import NarrativeSummary, TopicSummary, ViralNarrativeSummary


class NarrativeRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def create_narrative(
        self,
        title: str,
        description: str,
        claim_ids: list[UUID],
        topic_ids: list[UUID],
        metadata: dict[str, Any],
        entity_ids: list[UUID] | None = None,
    ) -> Narrative:
        try:
            await self._session.execute(
                """
                INSERT INTO narratives (
                    title, description, metadata
                ) VALUES (
                    %(title)s, %(description)s, %(metadata)s
                )
                RETURNING *
                """,
                {
                    "title": title,
                    "description": description,
                    "metadata": Jsonb(metadata),
                },
            )
        except psycopg.errors.UniqueViolation:
            raise ConflictError("narrative already exists")

        row = await self._session.fetchone()
        if not row:
            raise ValueError("failed to create narrative")

        narrative_id = row["id"]

        if claim_ids:
            await self._session.executemany(
                """
                INSERT INTO claim_narratives (claim_id, narrative_id)
                VALUES (%(claim_id)s, %(narrative_id)s)
                ON CONFLICT (claim_id, narrative_id) DO NOTHING
                """,
                [
                    {"claim_id": claim_id, "narrative_id": narrative_id}
                    for claim_id in claim_ids
                ],
            )

        if topic_ids:
            await self._session.executemany(
                """
                INSERT INTO narrative_topics (narrative_id, topic_id)
                VALUES (%(narrative_id)s, %(topic_id)s)
                ON CONFLICT (narrative_id, topic_id) DO NOTHING
                """,
                [
                    {"narrative_id": narrative_id, "topic_id": topic_id}
                    for topic_id in topic_ids
                ],
            )

        if entity_ids:
            await self._session.executemany(
                """
                INSERT INTO narrative_entities (narrative_id, entity_id)
                VALUES (%(narrative_id)s, %(entity_id)s)
                ON CONFLICT (narrative_id, entity_id) DO NOTHING
                """,
                [
                    {"narrative_id": narrative_id, "entity_id": entity_id}
                    for entity_id in entity_ids
                ],
            )

        claims = await self._get_narrative_claims(narrative_id)
        topics = await self._get_narrative_topics(narrative_id)
        entities = await self._get_narrative_entities(narrative_id)
        videos = await self._get_narrative_videos(narrative_id)

        return Narrative(
            **row, claims=claims, topics=topics, entities=entities, videos=videos
        )

    async def get_narrative(self, narrative_id: UUID) -> Narrative | None:
        await self._session.execute(
            """
            SELECT * FROM narratives
            WHERE id = %(narrative_id)s
            """,
            {"narrative_id": narrative_id},
        )
        row = await self._session.fetchone()
        if not row:
            return None

        claims = await self._get_narrative_claims(narrative_id)
        topics = await self._get_narrative_topics(narrative_id)
        entities = await self._get_narrative_entities(narrative_id)
        videos = await self._get_narrative_videos(narrative_id)
        return Narrative(
            **row, claims=claims, topics=topics, entities=entities, videos=videos
        )

    async def get_narratives_by_claim(self, claim_id: UUID) -> list[Narrative]:
        await self._session.execute(
            """
            SELECT * FROM narratives
            WHERE claim_id = %(claim_id)s
            ORDER BY created_at DESC
            """,
            {"claim_id": claim_id},
        )
        rows = await self._session.fetchall()

        narratives = []
        for row in rows:
            claims = await self._get_narrative_claims(row["id"])
            topics = await self._get_narrative_topics(row["id"])
            entities = await self._get_narrative_entities(row["id"])
            videos = await self._get_narrative_videos(row["id"])
            narratives.append(
                Narrative(
                    **row,
                    claims=claims,
                    topics=topics,
                    entities=entities,
                    videos=videos,
                )
            )

        return narratives

    async def get_all_narratives(
        self,
        limit: int = 100,
        offset: int = 0,
        topic_id: UUID | None = None,
        entity_id: UUID | None = None,
        text: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        first_content_start: datetime | None = None,
        first_content_end: datetime | None = None,
        language: str | None = None,
    ) -> list[Narrative]:
        query = """
            SELECT DISTINCT n.* FROM narratives n
        """

        where_statement, params = self._build_get_all_narratives_where_statement(
            topic_id=topic_id,
            entity_id=entity_id,
            language=language,
            text=text,
            start_date=start_date,
            end_date=end_date,
            first_content_start=first_content_start,
            first_content_end=first_content_end,
        )

        query += where_statement

        query += """
            ORDER BY n.created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        params["limit"] = limit
        params["offset"] = offset

        await self._session.execute(query, params)
        rows = await self._session.fetchall()

        narratives = []
        for row in rows:
            claims = await self._get_narrative_claims(row["id"])
            topics = await self._get_narrative_topics(row["id"])
            entities = await self._get_narrative_entities(row["id"])
            videos = await self._get_narrative_videos(row["id"])
            narratives.append(
                Narrative(
                    **row,
                    claims=claims,
                    topics=topics,
                    entities=entities,
                    videos=videos,
                )
            )

        return narratives

    async def count_all_narratives(
        self,
        topic_id: UUID | None = None,
        entity_id: UUID | None = None,
        language: str | None = None,
        text: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        first_content_start: datetime | None = None,
        first_content_end: datetime | None = None,
    ) -> int:
        query = """
            SELECT COUNT(DISTINCT n.id) FROM narratives n
        """
        where_statement, params = self._build_get_all_narratives_where_statement(
            topic_id=topic_id,
            entity_id=entity_id,
            language=language,
            text=text,
            start_date=start_date,
            end_date=end_date,
            first_content_start=first_content_start,
            first_content_end=first_content_end,
        )
        query += where_statement

        await self._session.execute(query, params)
        row = await self._session.fetchone()
        return row["count"] if row else 0

    def _build_get_all_narratives_where_statement(
        self,
        topic_id: UUID | None = None,
        entity_id: UUID | None = None,
        language: str | None = None,
        text: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        first_content_start: datetime | None = None,
        first_content_end: datetime | None = None,
    ) -> tuple[str, dict[str, int | UUID | str | datetime]]:
        query = ""
        where_conditions = []
        params: dict[str, int | UUID | str | datetime] = {}

        if topic_id:
            query += """
                INNER JOIN narrative_topics nt ON n.id = nt.narrative_id
            """
            where_conditions.append("nt.topic_id = %(topic_id)s")
            params["topic_id"] = topic_id
        if entity_id:
            query += """
                INNER JOIN narrative_entities ne ON n.id = ne.narrative_id
            """
            where_conditions.append("ne.entity_id = %(entity_id)s")
            params["entity_id"] = entity_id

        if language:
            query += """
                INNER JOIN claim_narratives cn ON n.id = cn.narrative_id
                INNER JOIN video_claims vc ON cn.claim_id = vc.id
            """
            where_conditions.append("vc.metadata->>'language' = %(language)s")
            params["language"] = language

        if text:
            where_conditions.append(
                "(LOWER(n.title) LIKE LOWER(%(text)s) OR LOWER(n.description) LIKE LOWER(%(text)s))"
            )
            params["text"] = f"%{text}%"

        if start_date:
            where_conditions.append("n.created_at >= %(start_date)s")
            params["start_date"] = start_date
        if end_date:
            where_conditions.append("n.created_at <= %(end_date)s")
            params["end_date"] = end_date

        if first_content_start or first_content_end:
            oldest_video_filter = """
                n.id IN (
                    SELECT narrative_id
                    FROM (
                        SELECT
                            cn.narrative_id,
                            v.uploaded_at,
                            ROW_NUMBER() OVER (PARTITION BY cn.narrative_id ORDER BY v.uploaded_at ASC) as rn
                        FROM claim_narratives cn
                        JOIN video_claims vc ON cn.claim_id = vc.id
                        JOIN videos v ON vc.video_id = v.id
                    ) oldest_videos
                    WHERE rn = 1
            """

            if first_content_start and first_content_end:
                oldest_video_filter += " AND uploaded_at BETWEEN %(first_content_start)s AND %(first_content_end)s"
                params["first_content_start"] = first_content_start
                params["first_content_end"] = first_content_end
            elif first_content_start:
                oldest_video_filter += " AND uploaded_at >= %(first_content_start)s"
                params["first_content_start"] = first_content_start
            elif first_content_end:
                oldest_video_filter += " AND uploaded_at <= %(first_content_end)s"
                params["first_content_end"] = first_content_end

            oldest_video_filter += ")"
            where_conditions.append(oldest_video_filter)

        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)

        return query, params

    async def update_narrative(
        self,
        narrative_id: UUID,
        title: str | None = None,
        description: str | None = None,
        claim_ids: list[UUID] | None = None,
        topic_ids: list[UUID] | None = None,
        metadata: dict[str, Any] | None = None,
        entity_ids: list[UUID] | None = None,
    ) -> Narrative | None:
        updates = []
        params: dict[str, Any] = {"narrative_id": narrative_id}

        if title is not None:
            updates.append("title = %(title)s")
            params["title"] = title

        if description is not None:
            updates.append("description = %(description)s")
            params["description"] = description

        if metadata is not None:
            updates.append("metadata = metadata || %(metadata)s")
            params["metadata"] = Jsonb(metadata)

        if updates:
            updates.append("updated_at = now()")
            await self._session.execute(
                f"""
                UPDATE narratives
                SET {", ".join(updates)}
                WHERE id = %(narrative_id)s
                RETURNING *
                """,
                params,
            )
            row = await self._session.fetchone()
            if not row:
                return None
        else:
            await self._session.execute(
                """
                SELECT * FROM narratives
                WHERE id = %(narrative_id)s
                """,
                {"narrative_id": narrative_id},
            )
            row = await self._session.fetchone()
            if not row:
                return None

        if claim_ids is not None:
            await self._session.execute(
                """
                DELETE FROM claim_narratives
                WHERE narrative_id = %(narrative_id)s
                """,
                {"narrative_id": narrative_id},
            )

            if claim_ids:
                await self._session.executemany(
                    """
                    INSERT INTO claim_narratives (claim_id, narrative_id)
                    VALUES (%(claim_id)s, %(narrative_id)s)
                    """,
                    [
                        {"claim_id": claim_id, "narrative_id": narrative_id}
                        for claim_id in claim_ids
                    ],
                )

        if topic_ids is not None:
            await self._session.execute(
                """
                DELETE FROM narrative_topics
                WHERE narrative_id = %(narrative_id)s
                """,
                {"narrative_id": narrative_id},
            )

            if topic_ids:
                await self._session.executemany(
                    """
                    INSERT INTO narrative_topics (narrative_id, topic_id)
                    VALUES (%(narrative_id)s, %(topic_id)s)
                    """,
                    [
                        {"narrative_id": narrative_id, "topic_id": topic_id}
                        for topic_id in topic_ids
                    ],
                )

        if entity_ids is not None:
            await self._session.execute(
                """
                DELETE FROM narrative_entities
                WHERE narrative_id = %(narrative_id)s
                """,
                {"narrative_id": narrative_id},
            )

            if entity_ids:
                await self._session.executemany(
                    """
                    INSERT INTO narrative_entities (narrative_id, entity_id)
                    VALUES (%(narrative_id)s, %(entity_id)s)
                    ON CONFLICT (narrative_id, entity_id) DO NOTHING
                    """,
                    [
                        {"narrative_id": narrative_id, "entity_id": entity_id}
                        for entity_id in entity_ids
                    ],
                )

        claims = await self._get_narrative_claims(narrative_id)
        topics = await self._get_narrative_topics(narrative_id)
        entities = await self._get_narrative_entities(narrative_id)
        videos = await self._get_narrative_videos(narrative_id)
        return Narrative(
            **row, claims=claims, topics=topics, entities=entities, videos=videos
        )

    async def delete_narrative(self, narrative_id: UUID) -> None:
        await self._session.execute(
            """
            DELETE FROM narratives WHERE id = %(narrative_id)s
            """,
            {"narrative_id": narrative_id},
        )

    async def _get_narrative_claims(self, narrative_id: UUID) -> list[Claim]:
        await self._session.execute(
            """
            SELECT c.id, c.video_id, c.claim, c.start_time_s, c.metadata, c.created_at, c.updated_at
            FROM video_claims c
            JOIN claim_narratives cn ON c.id = cn.claim_id
            WHERE cn.narrative_id = %(narrative_id)s
            ORDER BY c.start_time_s
            """,
            {"narrative_id": narrative_id},
        )
        claims = []
        for row in await self._session.fetchall():
            claim_data = dict(row)
            claims.append(Claim(**claim_data))
        return claims

    async def _get_narrative_topics(self, narrative_id: UUID) -> list[Topic]:
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

    async def _get_narrative_entities(self, narrative_id: UUID) -> list[Entity]:
        await self._session.execute(
            """
            SELECT e.*
            FROM entities e
            JOIN narrative_entities ne ON e.id = ne.entity_id
            WHERE ne.narrative_id = %(narrative_id)s
            ORDER BY e.name
            """,
            {"narrative_id": narrative_id},
        )
        return [Entity(**row) for row in await self._session.fetchall()]

    async def _get_narrative_videos(self, narrative_id: UUID) -> list[Video]:
        await self._session.execute(
            """
            SELECT DISTINCT v.id, v.title, v.description, v.platform, v.source_url,
                   v.destination_path, v.uploaded_at, v.views, v.likes, v.comments,
                   v.channel, v.channel_followers, v.scrape_topic, v.scrape_keyword,
                   v.metadata, v.created_at, v.updated_at
            FROM videos v
            JOIN video_claims c ON v.id = c.video_id
            JOIN claim_narratives cn ON c.id = cn.claim_id
            WHERE cn.narrative_id = %(narrative_id)s
            ORDER BY v.uploaded_at DESC
            """,
            {"narrative_id": narrative_id},
        )
        rows = await self._session.fetchall()
        videos = []
        for row in rows:
            video_data = dict(row)
            videos.append(Video(**video_data))
        return videos

    async def claims_exist(self, claim_ids: list[UUID]) -> bool:
        if not claim_ids:
            return True

        await self._session.execute(
            """
            SELECT COUNT(*) as count FROM video_claims WHERE id = ANY(%(claim_ids)s)
            """,
            {"claim_ids": claim_ids},
        )
        row = await self._session.fetchone()
        if not row:
            return False
        return row["count"] == len(claim_ids)

    async def find_by_narrative_id_in_metadata(
        self, narrative_id: str
    ) -> Narrative | None:
        await self._session.execute(
            """
            SELECT * FROM narratives
            WHERE metadata->>'narrative_id' = %(narrative_id)s
            """,
            {"narrative_id": narrative_id},
        )
        row = await self._session.fetchone()
        if not row:
            return None

        claims = await self._get_narrative_claims(row["id"])
        topics = await self._get_narrative_topics(row["id"])
        videos = await self._get_narrative_videos(row["id"])
        return Narrative(**row, claims=claims, topics=topics, videos=videos)

    async def find_by_title(self, title: str) -> Narrative | None:
        await self._session.execute(
            """
            SELECT * FROM narratives
            WHERE title = %(title)s
            """,
            {"title": title},
        )
        row = await self._session.fetchone()
        if not row:
            return None

        claims = await self._get_narrative_claims(row["id"])
        topics = await self._get_narrative_topics(row["id"])
        videos = await self._get_narrative_videos(row["id"])
        return Narrative(**row, claims=claims, topics=topics, videos=videos)

    async def get_narratives_by_topic(
        self, topic_id: UUID, limit: int = 100, offset: int = 0
    ) -> tuple[list[Narrative], int]:
        # Get total count
        await self._session.execute(
            """
            SELECT COUNT(DISTINCT n.id)
            FROM narratives n
            JOIN narrative_topics nt ON n.id = nt.narrative_id
            WHERE nt.topic_id = %(topic_id)s
            """,
            {"topic_id": topic_id},
        )
        total_row = await self._session.fetchone()
        total = total_row["count"] if total_row else 0

        # Get narratives
        await self._session.execute(
            """
            SELECT DISTINCT n.*
            FROM narratives n
            JOIN narrative_topics nt ON n.id = nt.narrative_id
            WHERE nt.topic_id = %(topic_id)s
            ORDER BY n.created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"topic_id": topic_id, "limit": limit, "offset": offset},
        )
        narratives = []
        for row in await self._session.fetchall():
            claims = await self._get_narrative_claims(row["id"])
            topics = await self._get_narrative_topics(row["id"])
            videos = await self._get_narrative_videos(row["id"])
            narratives.append(
                Narrative(**row, claims=claims, topics=topics, videos=videos)
            )

        return narratives, total

    async def get_viral_narratives(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[Narrative]:
        # Get narratives with claims from the specified time period, ordered by total video views
        await self._session.execute(
            """
            WITH recent_narrative_views AS (
                SELECT
                    n.id as narrative_id,
                    n.title,
                    n.description,
                    n.metadata,
                    n.created_at,
                    n.updated_at,
                    SUM(COALESCE(v.views, 0)) as total_views
                FROM narratives n
                JOIN claim_narratives cn ON n.id = cn.narrative_id
                JOIN video_claims c ON cn.claim_id = c.id
                JOIN videos v ON c.video_id = v.id
                WHERE %(hours)s IS NULL OR v.updated_at >= NOW() - (%(hours)s || ' hours')::INTERVAL
                GROUP BY n.id, n.title, n.description, n.metadata, n.created_at, n.updated_at
            )
            SELECT
                narrative_id as id,
                title,
                description,
                metadata,
                created_at,
                updated_at,
                total_views
            FROM recent_narrative_views
            ORDER BY total_views DESC
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"limit": limit, "offset": offset, "hours": hours},
        )
        rows = await self._session.fetchall()

        narratives = []
        for row in rows:
            narrative_data = dict(row)
            # Remove total_views from the dict as it's not part of the Narrative model
            narrative_data.pop("total_views", None)

            claims = await self._get_narrative_claims(narrative_data["id"])
            topics = await self._get_narrative_topics(narrative_data["id"])
            entities = await self._get_narrative_entities(narrative_data["id"])
            videos = await self._get_narrative_videos(narrative_data["id"])

            narratives.append(
                Narrative(
                    **narrative_data,
                    claims=claims,
                    topics=topics,
                    entities=entities,
                    videos=videos,
                )
            )

        return narratives

    async def get_prevalent_narratives(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[Narrative]:
        # Get narratives ordered by the count of associated videos within the specified time period
        await self._session.execute(
            """
            WITH narrative_video_counts AS (
                SELECT
                    n.id as narrative_id,
                    n.title,
                    n.description,
                    n.metadata,
                    n.created_at,
                    n.updated_at,
                    COUNT(DISTINCT v.id) as video_count
                FROM narratives n
                JOIN claim_narratives cn ON n.id = cn.narrative_id
                JOIN video_claims c ON cn.claim_id = c.id
                JOIN videos v ON c.video_id = v.id
                WHERE %(hours)s IS NULL OR v.updated_at >= NOW() - (%(hours)s || ' hours')::INTERVAL
                GROUP BY n.id, n.title, n.description, n.metadata, n.created_at, n.updated_at
            )
            SELECT
                narrative_id as id,
                title,
                description,
                metadata,
                created_at,
                updated_at,
                video_count
            FROM narrative_video_counts
            ORDER BY video_count DESC
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"limit": limit, "offset": offset, "hours": hours},
        )
        rows = await self._session.fetchall()

        narratives = []
        for row in rows:
            narrative_data = dict(row)
            # Remove video_count from the dict as it's not part of the Narrative model
            narrative_data.pop("video_count", None)

            claims = await self._get_narrative_claims(narrative_data["id"])
            topics = await self._get_narrative_topics(narrative_data["id"])
            entities = await self._get_narrative_entities(narrative_data["id"])
            videos = await self._get_narrative_videos(narrative_data["id"])

            narratives.append(
                Narrative(
                    **narrative_data,
                    claims=claims,
                    topics=topics,
                    entities=entities,
                    videos=videos,
                )
            )

        return narratives

    async def get_viral_narratives_summary(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[ViralNarrativeSummary]:
        """
        Get viral narratives with pre-aggregated stats in a single query.
        This is optimized for dashboard display and avoids N+1 queries.
        """
        await self._session.execute(
            """
            WITH relevant_narratives AS (
                SELECT
                    n.id,
                    n.title,
                    n.description,
                    n.metadata,
                    n.created_at,
                    n.updated_at,
                    SUM(COALESCE(v.views, 0)) as total_views
                FROM narratives n
                JOIN claim_narratives cn ON n.id = cn.narrative_id
                JOIN video_claims c ON cn.claim_id = c.id
                JOIN videos v ON c.video_id = v.id
                WHERE %(hours)s::integer is NULL OR v.updated_at >= NOW() - (%(hours)s  || ' hours')::interval
                group by n.id, n.title, n.description, n.metadata, n.created_at, n.updated_at
                order by SUM(COALESCE(v.views, 0)) desc
                LIMIT %(limit)s OFFSET %(offset)s
            ),
            video_narratives as (
                select
                    v.id as video_id,
                    n.id as narrative_id,
                    count(distinct c.id) as claim_count,
                    array_agg(DISTINCT c.metadata->>'language') FILTER (
                        WHERE c.metadata->>'language' IS NOT NULL
                        AND c.metadata->>'language' != ''
                    ) as languages
                from videos v
                JOIN video_claims c ON c.video_id = v.id
                JOIN claim_narratives cn ON cn.claim_id = c.id
                JOIN relevant_narratives n ON n.id = cn.narrative_id
                GROUP BY v.id, n.id
            ),
            narrative_topics AS (
                SELECT
                    nt.narrative_id,
                    JSON_AGG(
                        JSON_BUILD_OBJECT('id', t.id, 'topic', t.topic)
                        ORDER BY t.topic
                    ) as topics
                FROM narrative_topics nt
                JOIN topics t ON nt.topic_id = t.id
                GROUP BY nt.narrative_id
            )
            select
                n.id,
                n.title,
                n.description,
                n.metadata,
                n.created_at,
                n.updated_at,
                (select topics from narrative_topics where narrative_id = n.id) as topics,
                COUNT(DISTINCT v.id) as video_count,
                SUM(vn.claim_count) as claim_count,
                ARRAY_AGG(DISTINCT v.platform) FILTER (WHERE v.platform IS NOT NULL) as platforms,
                SUM(COALESCE(v.views, 0)) as total_views,
                SUM(COALESCE(v.likes, 0)) as total_likes,
                SUM(COALESCE(v.comments, 0)) as total_comments,
                count(distinct l.languages) as language_count
            FROM relevant_narratives n
            JOIN video_narratives vn ON vn.narrative_id = n.id
            JOIN videos v ON v.id = vn.video_id
            LEFT JOIN LATERAL (SELECT unnest(vn.languages) as languages) l on TRUE
            GROUP BY n.id, n.title, n.description, n.metadata, n.created_at, n.updated_at
            ORDER BY SUM(COALESCE(v.views, 0)) DESC
            """,
            {"limit": limit, "offset": offset, "hours": hours},
        )
        rows = await self._session.fetchall()

        summaries = []
        for row in rows:
            topics = [
                TopicSummary(id=t["id"], topic=t["topic"])
                for t in (row["topics"] or [])
            ]
            summaries.append(
                ViralNarrativeSummary(
                    id=row["id"],
                    title=row["title"],
                    topics=topics,
                    platforms=row["platforms"] or [],
                    total_views=row["total_views"] or 0,
                    total_likes=row["total_likes"] or 0,
                    total_comments=row["total_comments"] or 0,
                    claim_count=row["claim_count"] or 0,
                    video_count=row["video_count"] or 0,
                    language_count=row["language_count"] or 0,
                )
            )

        return summaries

    async def get_prevalent_narratives_summary(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[NarrativeSummary]:
        """
        Get prevalent narratives with pre-aggregated stats in a single query.
        Sorted by video count (most videos first).
        This is optimized for dashboard display and avoids N+1 queries.
        """
        await self._session.execute(
            """
            WITH relevant_narratives AS (
                SELECT
                    n.id,
                    n.title,
                    n.description,
                    n.metadata,
                    n.created_at,
                    n.updated_at,
                    SUM(COALESCE(v.views, 0)) as total_views
                FROM narratives n
                JOIN claim_narratives cn ON n.id = cn.narrative_id
                JOIN video_claims c ON cn.claim_id = c.id
                JOIN videos v ON c.video_id = v.id
                WHERE %(hours)s::integer is NULL OR v.updated_at >= NOW() - (%(hours)s  || ' hours')::interval
                group by n.id, n.title, n.description, n.metadata, n.created_at, n.updated_at
                order by count(distinct v.id) desc
                LIMIT %(limit)s OFFSET %(offset)s
            ),
            video_narratives as (
                select
                    v.id as video_id,
                    n.id as narrative_id,
                    count(distinct c.id) as claim_count,
                    array_agg(DISTINCT c.metadata->>'language') FILTER (
                        WHERE c.metadata->>'language' IS NOT NULL
                        AND c.metadata->>'language' != ''
                    ) as languages
                from videos v
                JOIN video_claims c ON c.video_id = v.id
                JOIN claim_narratives cn ON cn.claim_id = c.id
                JOIN relevant_narratives n ON n.id = cn.narrative_id
                GROUP BY v.id, n.id
            ),
            narrative_topics AS (
                SELECT
                    nt.narrative_id,
                    JSON_AGG(
                        JSON_BUILD_OBJECT('id', t.id, 'topic', t.topic)
                        ORDER BY t.topic
                    ) as topics
                FROM narrative_topics nt
                JOIN topics t ON nt.topic_id = t.id
                GROUP BY nt.narrative_id
            )
            SELECT
                n.id,
                n.title,
                n.description,
                n.metadata,
                n.created_at,
                n.updated_at,
                (SELECT topics from narrative_topics where narrative_id = n.id) as topics,
                COUNT(DISTINCT v.id) as video_count,
                SUM(vn.claim_count) as claim_count,
                ARRAY_AGG(DISTINCT v.platform) FILTER (WHERE v.platform IS NOT NULL) as platforms,
                SUM(COALESCE(v.views, 0)) as total_views,
                SUM(COALESCE(v.likes, 0)) as total_likes,
                SUM(COALESCE(v.comments, 0)) as total_comments,
                count(distinct l.languages) as language_count
            FROM relevant_narratives n
            JOIN video_narratives vn ON vn.narrative_id = n.id
            JOIN videos v ON v.id = vn.video_id
            LEFT JOIN LATERAL (SELECT unnest(vn.languages) as languages) l on TRUE
            GROUP BY n.id, n.title, n.description, n.metadata, n.created_at, n.updated_at
            ORDER BY COUNT(DISTINCT v.id) DESC
            """,
            {"limit": limit, "offset": offset, "hours": hours},
        )
        rows = await self._session.fetchall()

        summaries = []
        for row in rows:
            topics = [
                TopicSummary(id=t["id"], topic=t["topic"])
                for t in (row["topics"] or [])
            ]
            summaries.append(
                NarrativeSummary(
                    id=row["id"],
                    title=row["title"],
                    topics=topics,
                    platforms=row["platforms"] or [],
                    total_views=row["total_views"] or 0,
                    total_likes=row["total_likes"] or 0,
                    total_comments=row["total_comments"] or 0,
                    claim_count=row["claim_count"] or 0,
                    video_count=row["video_count"] or 0,
                    language_count=row["language_count"] or 0,
                )
            )

        return summaries

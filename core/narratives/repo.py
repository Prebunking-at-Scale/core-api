from datetime import date, datetime, timedelta
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.config import (
    VIRALITY_SCORE_COMMENTS_WEIGHT,
    VIRALITY_SCORE_LIKES_WEIGHT,
)
from core.errors import ConflictError
from core.models import Claim, Entity, Narrative, NarrativeSpreadPattern, Topic, Video
from core.narratives.models import (
    IndicatorPayload,
    NarrativeAnalysisIndicatorType,
    NarrativeDetail,
    NarrativeListItem,
    NarrativeStats,
    NarrativeStatsDataPoint,
    NarrativeStatsTotals,
    NarrativeSummary,
    NarrativeViralityScoreType,
    TopicSummary,
    ViralityScoreRank,
    ViralNarrativeSummary,
)


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
        narrative_context: str | None = None,
    ) -> Narrative:
        try:
            await self._session.execute(
                """
                INSERT INTO narratives (
                    title, description, narrative_context, metadata
                ) VALUES (
                    %(title)s, %(description)s, %(narrative_context)s, %(metadata)s
                )
                RETURNING *
                """,
                {
                    "title": title,
                    "description": description,
                    "narrative_context": narrative_context,
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
        spread_patterns: list[str] | None = None,
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
            spread_patterns=spread_patterns,
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
        spread_patterns: list[str] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        query = ""
        where_conditions = []
        params: dict[str, Any] = {}

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

        if spread_patterns:
            where_conditions.append("n.spread_pattern = ANY(%(spread_patterns)s)")
            params["spread_patterns"] = spread_patterns

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

    async def get_all_narratives_list(
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
        spread_patterns: list[str] | None = None,
        sort: str | None = None,
    ) -> list[NarrativeListItem]:
        """
        Get all narratives with pre-aggregated counts in a single query.

        `sort="composite"` / `sort="acceleration"` ranks results by each
        narrative's latest composite_virality / acceleration_rate indicator
        (highest first); otherwise newest first.
        """
        # Build filter conditions
        filter_joins = ""
        filter_conditions = []
        params: dict[str, Any] = {"limit": limit, "offset": offset}

        if topic_id:
            filter_joins += """
                INNER JOIN narrative_topics filter_nt ON n.id = filter_nt.narrative_id
            """
            filter_conditions.append("filter_nt.topic_id = %(topic_id)s")
            params["topic_id"] = topic_id

        if entity_id:
            filter_joins += """
                INNER JOIN narrative_entities filter_ne ON n.id = filter_ne.narrative_id
            """
            filter_conditions.append("filter_ne.entity_id = %(entity_id)s")
            params["entity_id"] = entity_id

        if language:
            filter_joins += """
                INNER JOIN claim_narratives filter_cn ON n.id = filter_cn.narrative_id
                INNER JOIN video_claims filter_vc ON filter_cn.claim_id = filter_vc.id
            """
            filter_conditions.append("filter_vc.metadata->>'language' = %(language)s")
            params["language"] = language

        if text:
            filter_conditions.append(
                "(LOWER(n.title) LIKE LOWER(%(text)s) OR LOWER(n.description) LIKE LOWER(%(text)s))"
            )
            params["text"] = f"%{text}%"

        if start_date:
            filter_conditions.append("n.created_at >= %(start_date)s")
            params["start_date"] = start_date

        if end_date:
            filter_conditions.append("n.created_at <= %(end_date)s")
            params["end_date"] = end_date

        if spread_patterns:
            filter_conditions.append("n.spread_pattern = ANY(%(spread_patterns)s)")
            params["spread_patterns"] = spread_patterns

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
            filter_conditions.append(oldest_video_filter)

        where_clause = ""
        if filter_conditions:
            where_clause = "WHERE " + " AND ".join(filter_conditions)

        # Optional ranking by latest composite virality score. Used by the
        # overview to surface the top-scoring narratives per spread pattern. The
        # LATERAL join picks each narrative's most recent indicator value.
        sort_indicators = {
            "composite": "composite_virality",
            "acceleration": "acceleration_rate",
        }
        sort_select = ""
        sort_join = ""
        inner_order = "n.created_at DESC"
        final_order = "fn.created_at DESC"
        if sort in sort_indicators:
            params["sort_indicator"] = sort_indicators[sort]
            sort_select = ", ci.indicator_value AS sort_score"
            sort_join = """
                LEFT JOIN LATERAL (
                    SELECT i.indicator_value
                    FROM narrative_analysis_indicators i
                    WHERE i.narrative_id = n.id
                      AND i.indicator_type = %(sort_indicator)s
                    ORDER BY i.calculated_at DESC
                    LIMIT 1
                ) ci ON TRUE
            """
            inner_order = "ci.indicator_value DESC NULLS LAST, n.created_at DESC"
            final_order = "fn.sort_score DESC NULLS LAST, fn.created_at DESC"

        query = f"""
            WITH filtered_narratives AS (
                SELECT DISTINCT n.id, n.title, n.description, n.created_at, n.updated_at, n.spread_pattern{sort_select}
                FROM narratives n
                {filter_joins}
                {sort_join}
                {where_clause}
                ORDER BY {inner_order}
                LIMIT %(limit)s OFFSET %(offset)s
            ),
            narrative_claims AS (
                SELECT
                    fn.id as narrative_id,
                    COUNT(DISTINCT cn.claim_id) as claim_count
                FROM filtered_narratives fn
                LEFT JOIN claim_narratives cn ON fn.id = cn.narrative_id
                GROUP BY fn.id
            ),
            distinct_narrative_videos AS (
                SELECT DISTINCT
                    fn.id as narrative_id,
                    v.id as video_id,
                    v.views,
                    v.likes,
                    v.comments,
                    v.platform
                FROM filtered_narratives fn
                LEFT JOIN claim_narratives cn ON fn.id = cn.narrative_id
                LEFT JOIN video_claims vc ON cn.claim_id = vc.id
                LEFT JOIN videos v ON vc.video_id = v.id
            ),
            narrative_videos AS (
                SELECT
                    narrative_id,
                    COUNT(video_id) as video_count,
                    COALESCE(SUM(views), 0) as total_views,
                    COALESCE(SUM(likes), 0) as total_likes,
                    COALESCE(SUM(comments), 0) as total_comments,
                    ARRAY_AGG(DISTINCT platform) FILTER (WHERE platform IS NOT NULL) as platforms
                FROM distinct_narrative_videos
                GROUP BY narrative_id
            ),
            narrative_languages AS (
                SELECT
                    fn.id as narrative_id,
                    COUNT(DISTINCT vc.metadata->>'language') FILTER (
                        WHERE vc.metadata->>'language' IS NOT NULL
                        AND vc.metadata->>'language' != ''
                    ) as language_count
                FROM filtered_narratives fn
                LEFT JOIN claim_narratives cn ON fn.id = cn.narrative_id
                LEFT JOIN video_claims vc ON cn.claim_id = vc.id
                GROUP BY fn.id
            ),
            narrative_entities AS (
                SELECT
                    fn.id as narrative_id,
                    COUNT(DISTINCT ne.entity_id) as entity_count
                FROM filtered_narratives fn
                LEFT JOIN narrative_entities ne ON fn.id = ne.narrative_id
                GROUP BY fn.id
            ),
            narrative_topics_agg AS (
                SELECT
                    nt.narrative_id,
                    JSON_AGG(
                        JSON_BUILD_OBJECT('id', t.id, 'topic', t.topic)
                        ORDER BY t.topic
                    ) as topics
                FROM narrative_topics nt
                JOIN topics t ON nt.topic_id = t.id
                JOIN filtered_narratives fn ON fn.id = nt.narrative_id
                GROUP BY nt.narrative_id
            ),
            narrative_ratings AS (
                SELECT
                    nf.narrative_id,
                    COUNT(*) as score_count,
                    AVG(nf.feedback_score)::float as average_score
                FROM narrative_feedback nf
                JOIN filtered_narratives fn ON fn.id = nf.narrative_id
                GROUP BY nf.narrative_id
            )
            SELECT
                fn.id,
                fn.title,
                fn.description,
                fn.created_at,
                fn.updated_at,
                fn.spread_pattern,
                COALESCE(nta.topics, '[]'::json) as topics,
                COALESCE(nc.claim_count, 0) as claim_count,
                COALESCE(nv.video_count, 0) as video_count,
                COALESCE(nv.total_views, 0) as total_views,
                COALESCE(nv.total_likes, 0) as total_likes,
                COALESCE(nv.total_comments, 0) as total_comments,
                COALESCE(nv.platforms, ARRAY[]::text[]) as platforms,
                COALESCE(nl.language_count, 0) as language_count,
                COALESCE(nen.entity_count, 0) as entity_count,
                COALESCE(nr.score_count, 0) as score_count,
                nr.average_score as average_score
            FROM filtered_narratives fn
            LEFT JOIN narrative_claims nc ON fn.id = nc.narrative_id
            LEFT JOIN narrative_videos nv ON fn.id = nv.narrative_id
            LEFT JOIN narrative_languages nl ON fn.id = nl.narrative_id
            LEFT JOIN narrative_entities nen ON fn.id = nen.narrative_id
            LEFT JOIN narrative_topics_agg nta ON fn.id = nta.narrative_id
            LEFT JOIN narrative_ratings nr ON fn.id = nr.narrative_id
            ORDER BY {final_order}
        """

        await self._session.execute(query, params)
        rows = await self._session.fetchall()

        summaries = []
        for row in rows:
            topics = [
                TopicSummary(id=t["id"], topic=t["topic"])
                for t in (row["topics"] if row["topics"] else [])
            ]
            summaries.append(
                NarrativeListItem(
                    id=row["id"],
                    title=row["title"],
                    description=row["description"] or "",
                    topics=topics,
                    platforms=row["platforms"] or [],
                    total_views=row["total_views"] or 0,
                    total_likes=row["total_likes"] or 0,
                    total_comments=row["total_comments"] or 0,
                    claim_count=row["claim_count"] or 0,
                    video_count=row["video_count"] or 0,
                    language_count=row["language_count"] or 0,
                    entity_count=row["entity_count"] or 0,
                    score_count=row["score_count"] or 0,
                    average_score=row["average_score"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                    spread_pattern=row["spread_pattern"],
                )
            )

        return summaries

    async def get_narratives_by_claim_list(self, claim_id: UUID) -> list[NarrativeListItem]:
        """
        Get narratives for a specific claim with pre-aggregated counts.
        """
        query = """
            WITH claim_narratives_filtered AS (
                SELECT DISTINCT n.id, n.title, n.description, n.created_at, n.updated_at
                FROM narratives n
                INNER JOIN claim_narratives cn ON n.id = cn.narrative_id
                WHERE cn.claim_id = %(claim_id)s
            ),
            narrative_claims AS (
                SELECT
                    fn.id as narrative_id,
                    COUNT(DISTINCT cn.claim_id) as claim_count
                FROM claim_narratives_filtered fn
                LEFT JOIN claim_narratives cn ON fn.id = cn.narrative_id
                GROUP BY fn.id
            ),
            distinct_narrative_videos AS (
                SELECT DISTINCT
                    fn.id as narrative_id,
                    v.id as video_id,
                    v.views,
                    v.likes,
                    v.comments,
                    v.platform
                FROM claim_narratives_filtered fn
                LEFT JOIN claim_narratives cn ON fn.id = cn.narrative_id
                LEFT JOIN video_claims vc ON cn.claim_id = vc.id
                LEFT JOIN videos v ON vc.video_id = v.id
            ),
            narrative_videos AS (
                SELECT
                    narrative_id,
                    COUNT(video_id) as video_count,
                    COALESCE(SUM(views), 0) as total_views,
                    COALESCE(SUM(likes), 0) as total_likes,
                    COALESCE(SUM(comments), 0) as total_comments,
                    ARRAY_AGG(DISTINCT platform) FILTER (WHERE platform IS NOT NULL) as platforms
                FROM distinct_narrative_videos
                GROUP BY narrative_id
            ),
            narrative_languages AS (
                SELECT
                    fn.id as narrative_id,
                    COUNT(DISTINCT vc.metadata->>'language') FILTER (
                        WHERE vc.metadata->>'language' IS NOT NULL
                        AND vc.metadata->>'language' != ''
                    ) as language_count
                FROM claim_narratives_filtered fn
                LEFT JOIN claim_narratives cn ON fn.id = cn.narrative_id
                LEFT JOIN video_claims vc ON cn.claim_id = vc.id
                GROUP BY fn.id
            ),
            narrative_entities AS (
                SELECT
                    fn.id as narrative_id,
                    COUNT(DISTINCT ne.entity_id) as entity_count
                FROM claim_narratives_filtered fn
                LEFT JOIN narrative_entities ne ON fn.id = ne.narrative_id
                GROUP BY fn.id
            ),
            narrative_topics_agg AS (
                SELECT
                    nt.narrative_id,
                    JSON_AGG(
                        JSON_BUILD_OBJECT('id', t.id, 'topic', t.topic)
                        ORDER BY t.topic
                    ) as topics
                FROM narrative_topics nt
                JOIN topics t ON nt.topic_id = t.id
                JOIN claim_narratives_filtered fn ON fn.id = nt.narrative_id
                GROUP BY nt.narrative_id
            )
            SELECT
                fn.id,
                fn.title,
                fn.description,
                fn.created_at,
                fn.updated_at,
                COALESCE(nta.topics, '[]'::json) as topics,
                COALESCE(nc.claim_count, 0) as claim_count,
                COALESCE(nv.video_count, 0) as video_count,
                COALESCE(nv.total_views, 0) as total_views,
                COALESCE(nv.total_likes, 0) as total_likes,
                COALESCE(nv.total_comments, 0) as total_comments,
                COALESCE(nv.platforms, ARRAY[]::text[]) as platforms,
                COALESCE(nl.language_count, 0) as language_count,
                COALESCE(nen.entity_count, 0) as entity_count
            FROM claim_narratives_filtered fn
            LEFT JOIN narrative_claims nc ON fn.id = nc.narrative_id
            LEFT JOIN narrative_videos nv ON fn.id = nv.narrative_id
            LEFT JOIN narrative_languages nl ON fn.id = nl.narrative_id
            LEFT JOIN narrative_entities nen ON fn.id = nen.narrative_id
            LEFT JOIN narrative_topics_agg nta ON fn.id = nta.narrative_id
            ORDER BY fn.created_at DESC
        """

        await self._session.execute(query, {"claim_id": claim_id})
        rows = await self._session.fetchall()

        summaries = []
        for row in rows:
            topics = [
                TopicSummary(id=t["id"], topic=t["topic"])
                for t in (row["topics"] if row["topics"] else [])
            ]
            summaries.append(
                NarrativeListItem(
                    id=row["id"],
                    title=row["title"],
                    description=row["description"] or "",
                    topics=topics,
                    platforms=row["platforms"] or [],
                    total_views=row["total_views"] or 0,
                    total_likes=row["total_likes"] or 0,
                    total_comments=row["total_comments"] or 0,
                    claim_count=row["claim_count"] or 0,
                    video_count=row["video_count"] or 0,
                    language_count=row["language_count"] or 0,
                    entity_count=row["entity_count"] or 0,
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            )

        return summaries

    async def update_narrative(
        self,
        narrative_id: UUID,
        title: str | None = None,
        description: str | None = None,
        narrative_context: str | None = None,
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

        if narrative_context is not None:
            updates.append("narrative_context = %(narrative_context)s")
            params["narrative_context"] = narrative_context

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

    async def get_narrative_detail(
        self,
        narrative_id: UUID,
        claims_limit: int = 10,
        videos_limit: int = 10,
    ) -> NarrativeDetail | None:
        """
        Get narrative with preview of claims/videos and aggregated stats.
        """
        query = """
            WITH narrative_base AS (
                SELECT id, title, description, narrative_context, metadata, created_at, updated_at, spread_pattern
                FROM narratives
                WHERE id = %(narrative_id)s
            ),
            claim_stats AS (
                SELECT COUNT(*) as claim_count
                FROM claim_narratives
                WHERE narrative_id = %(narrative_id)s
            ),
            video_stats AS (
                SELECT
                    COUNT(*) as video_count,
                    COALESCE(SUM(views), 0) as total_views,
                    COALESCE(SUM(likes), 0) as total_likes,
                    COALESCE(SUM(comments), 0) as total_comments,
                    ARRAY_AGG(DISTINCT platform) FILTER (WHERE platform IS NOT NULL) as platforms
                FROM (
                    SELECT DISTINCT v.id, v.views, v.likes, v.comments, v.platform
                    FROM videos v
                    JOIN video_claims vc ON v.id = vc.video_id
                    JOIN claim_narratives cn ON vc.id = cn.claim_id
                    WHERE cn.narrative_id = %(narrative_id)s
                ) distinct_videos
            ),
            language_stats AS (
                SELECT COUNT(DISTINCT vc.metadata->>'language') FILTER (
                    WHERE vc.metadata->>'language' IS NOT NULL
                    AND vc.metadata->>'language' != ''
                ) as language_count
                FROM video_claims vc
                JOIN claim_narratives cn ON vc.id = cn.claim_id
                WHERE cn.narrative_id = %(narrative_id)s
            )
            SELECT
                nb.id,
                nb.title,
                nb.description,
                nb.narrative_context,
                nb.metadata,
                nb.created_at,
                nb.updated_at,
                COALESCE(cs.claim_count, 0) as claim_count,
                COALESCE(vs.video_count, 0) as video_count,
                COALESCE(vs.total_views, 0) as total_views,
                COALESCE(vs.total_likes, 0) as total_likes,
                COALESCE(vs.total_comments, 0) as total_comments,
                COALESCE(vs.platforms, ARRAY[]::text[]) as platforms,
                COALESCE(ls.language_count, 0) as language_count,
                nb.spread_pattern
            FROM narrative_base nb
            CROSS JOIN claim_stats cs
            CROSS JOIN video_stats vs
            CROSS JOIN language_stats ls
        """

        await self._session.execute(query, {"narrative_id": narrative_id})
        row = await self._session.fetchone()
        if not row:
            return None

        # Fetch preview claims (limited)
        preview_claims = await self._get_narrative_claims_paginated(
            narrative_id, limit=claims_limit, offset=0
        )

        # Fetch preview videos (limited)
        preview_videos = await self._get_narrative_videos_paginated(
            narrative_id, limit=videos_limit, offset=0
        )

        # Fetch full topics and entities (usually small)
        topics = await self._get_narrative_topics(narrative_id)
        entities = await self._get_narrative_entities(narrative_id)

        return NarrativeDetail(
            id=row["id"],
            title=row["title"],
            description=row["description"] or "",
            narrative_context=row["narrative_context"],
            topics=topics,
            entities=entities,
            claims=preview_claims,
            claim_count=row["claim_count"],
            videos=preview_videos,
            video_count=row["video_count"],
            total_views=row["total_views"],
            total_likes=row["total_likes"],
            total_comments=row["total_comments"],
            platforms=row["platforms"] or [],
            language_count=row["language_count"],
            metadata=row["metadata"] or {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            spread_pattern=row["spread_pattern"],
        )

    async def _get_narrative_claims_paginated(
        self, narrative_id: UUID, limit: int, offset: int
    ) -> list[Claim]:
        """Get paginated claims for a narrative."""
        await self._session.execute(
            """
            SELECT c.id, c.video_id, c.claim, c.start_time_s, c.metadata,
                   c.created_at, c.updated_at
            FROM video_claims c
            JOIN claim_narratives cn ON c.id = cn.claim_id
            WHERE cn.narrative_id = %(narrative_id)s
            ORDER BY c.start_time_s
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"narrative_id": narrative_id, "limit": limit, "offset": offset},
        )
        claims = []
        for row in await self._session.fetchall():
            claim_data = dict(row)
            claims.append(Claim(**claim_data))
        return claims

    async def _get_narrative_videos_paginated(
        self, narrative_id: UUID, limit: int, offset: int
    ) -> list[Video]:
        """Get paginated videos for a narrative."""
        await self._session.execute(
            """
            SELECT DISTINCT v.id, v.title, v.description, v.platform, v.source_url,
                   v.destination_path, v.uploaded_at, v.views, v.likes, v.comments,
                   v.channel, v.channel_followers, v.scrape_topic, v.scrape_keyword,
                   v.metadata, v.created_at, v.updated_at
            FROM videos v
            JOIN video_claims vc ON v.id = vc.video_id
            JOIN claim_narratives cn ON vc.id = cn.claim_id
            WHERE cn.narrative_id = %(narrative_id)s
            ORDER BY v.uploaded_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"narrative_id": narrative_id, "limit": limit, "offset": offset},
        )
        videos = []
        for row in await self._session.fetchall():
            video_data = dict(row)
            videos.append(Video(**video_data))
        return videos

    async def get_narrative_claims(
        self, narrative_id: UUID, limit: int, offset: int
    ) -> tuple[list[Claim], int]:
        """Get paginated claims for a narrative with total count."""
        await self._session.execute(
            """
            SELECT COUNT(*) as count
            FROM claim_narratives
            WHERE narrative_id = %(narrative_id)s
            """,
            {"narrative_id": narrative_id},
        )
        count_row = await self._session.fetchone()
        total = count_row["count"] if count_row else 0

        claims = await self._get_narrative_claims_paginated(narrative_id, limit, offset)

        return claims, total

    async def get_narrative_videos(
        self, narrative_id: UUID, limit: int, offset: int
    ) -> tuple[list[Video], int]:
        """Get paginated videos for a narrative with total count."""
        await self._session.execute(
            """
            SELECT COUNT(DISTINCT v.id) as count
            FROM videos v
            JOIN video_claims vc ON v.id = vc.video_id
            JOIN claim_narratives cn ON vc.id = cn.claim_id
            WHERE cn.narrative_id = %(narrative_id)s
            """,
            {"narrative_id": narrative_id},
        )
        count_row = await self._session.fetchone()
        total = count_row["count"] if count_row else 0

        videos = await self._get_narrative_videos_paginated(narrative_id, limit, offset)

        return videos, total

    async def narrative_exists(self, narrative_id: UUID) -> bool:
        """Check if a narrative exists."""
        await self._session.execute(
            """
            SELECT EXISTS(SELECT 1 FROM narratives WHERE id = %(narrative_id)s) as exists
            """,
            {"narrative_id": narrative_id},
        )
        row = await self._session.fetchone()
        return row["exists"] if row else False

    async def get_narrative_stats(self, narrative_id: UUID) -> NarrativeStats | None:
        """
        Time series of cumulative views/likes/comments for the evolution chart.

        Built from video_stats snapshots (one row per video per day), not from
        videos.uploaded_at: we want the chart to reflect how engagement *grew*
        over time, not when each video was first published. A narrative with
        a single old video that just went viral now shows a recent ramp here,
        which is what virality scoring sees too.

        For each day in the snapshot history we take the latest snapshot per
        video and sum across the narrative — that's the cumulative engagement
        state at end of day. Per-day deltas are derived by LAG so the original
        response shape (cumulative_* + per-day delta fields) is preserved.
        """
        if not await self.narrative_exists(narrative_id):
            return None

        query = """
            WITH narrative_videos AS (
                SELECT DISTINCT v.id
                FROM videos v
                JOIN video_claims vc ON v.id = vc.video_id
                JOIN claim_narratives cn ON vc.id = cn.claim_id
                WHERE cn.narrative_id = %(narrative_id)s
            ),
            daily_latest AS (
                -- Latest snapshot per (video, day). DISTINCT ON gives one row
                -- per video per calendar day, taking the most recent recorded_at.
                SELECT DISTINCT ON (vs.video_id, vs.recorded_at::date)
                    vs.video_id,
                    vs.recorded_at::date AS day,
                    vs.views, vs.likes, vs.comments
                FROM video_stats vs
                JOIN narrative_videos nv ON vs.video_id = nv.id
                ORDER BY vs.video_id, vs.recorded_at::date, vs.recorded_at DESC
            ),
            change_days AS (
                -- The days on which the narrative's cumulative state actually changed,
                -- i.e. any video in it got a fresh snapshot. video_stats is scraped
                -- sparsely, so these are not contiguous calendar days.
                SELECT DISTINCT day FROM daily_latest
            ),
            carried AS (
                -- For each change-day, carry forward each video's latest snapshot
                -- recorded on or before that day. This is the real end-of-day state:
                -- a video keeps contributing its last-known counts on days when it
                -- wasn't re-snapshotted, instead of dropping to zero.
                SELECT cd.day, latest.video_id, latest.views, latest.likes, latest.comments
                FROM change_days cd
                JOIN LATERAL (
                    SELECT DISTINCT ON (dl.video_id)
                        dl.video_id, dl.views, dl.likes, dl.comments
                    FROM daily_latest dl
                    WHERE dl.day <= cd.day
                    ORDER BY dl.video_id, dl.day DESC
                ) latest ON TRUE
            ),
            per_day AS (
                -- End-of-day cumulative state across all videos in the narrative.
                SELECT
                    day,
                    COUNT(DISTINCT video_id) AS videos_with_stats,
                    COALESCE(SUM(views), 0)    AS cum_views,
                    COALESCE(SUM(likes), 0)    AS cum_likes,
                    COALESCE(SUM(comments), 0) AS cum_comments
                FROM carried
                GROUP BY day
            )
            SELECT
                day AS date,
                videos_with_stats AS video_count,
                GREATEST(cum_views    - LAG(cum_views,    1, 0::bigint) OVER (ORDER BY day), 0) AS views,
                GREATEST(cum_likes    - LAG(cum_likes,    1, 0::bigint) OVER (ORDER BY day), 0) AS likes,
                GREATEST(cum_comments - LAG(cum_comments, 1, 0::bigint) OVER (ORDER BY day), 0) AS comments,
                videos_with_stats AS cumulative_video_count,
                cum_views    AS cumulative_views,
                cum_likes    AS cumulative_likes,
                cum_comments AS cumulative_comments
            FROM per_day
            ORDER BY day
        """

        await self._session.execute(query, {"narrative_id": narrative_id})
        rows = await self._session.fetchall()

        time_series = []
        for row in rows:
            time_series.append(
                NarrativeStatsDataPoint(
                    date=row["date"],
                    views=row["views"],
                    likes=row["likes"],
                    comments=row["comments"],
                    cumulative_views=row["cumulative_views"],
                    cumulative_likes=row["cumulative_likes"],
                    cumulative_comments=row["cumulative_comments"],
                    video_count=row["video_count"],
                    cumulative_video_count=row["cumulative_video_count"],
                )
            )

        if time_series:
            last_point = time_series[-1]
            totals = NarrativeStatsTotals(
                views=last_point.cumulative_views,
                likes=last_point.cumulative_likes,
                comments=last_point.cumulative_comments,
                video_count=last_point.cumulative_video_count,
            )
        else:
            totals = NarrativeStatsTotals()

        return NarrativeStats(
            narrative_id=narrative_id,
            time_series=time_series,
            totals=totals,
        )

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
                WHERE %(hours)s::integer IS NULL OR v.updated_at >= NOW() - (%(hours)s || ' hours')::INTERVAL
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
                WHERE %(hours)s::integer IS NULL OR v.updated_at >= NOW() - (%(hours)s || ' hours')::INTERVAL
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
            ),
            narrative_entity_counts AS (
                SELECT
                    ne.narrative_id,
                    COUNT(DISTINCT ne.entity_id) as entity_count
                FROM narrative_entities ne
                JOIN relevant_narratives rn ON rn.id = ne.narrative_id
                GROUP BY ne.narrative_id
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
                count(distinct l.languages) as language_count,
                COALESCE((SELECT entity_count FROM narrative_entity_counts WHERE narrative_id = n.id), 0) as entity_count
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
                    description=row["description"] or "",
                    topics=topics,
                    platforms=row["platforms"] or [],
                    total_views=row["total_views"] or 0,
                    total_likes=row["total_likes"] or 0,
                    total_comments=row["total_comments"] or 0,
                    claim_count=row["claim_count"] or 0,
                    video_count=row["video_count"] or 0,
                    language_count=row["language_count"] or 0,
                    entity_count=row["entity_count"] or 0,
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
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
            ),
            narrative_entity_counts AS (
                SELECT
                    ne.narrative_id,
                    COUNT(DISTINCT ne.entity_id) as entity_count
                FROM narrative_entities ne
                JOIN relevant_narratives rn ON rn.id = ne.narrative_id
                GROUP BY ne.narrative_id
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
                count(distinct l.languages) as language_count,
                COALESCE((SELECT entity_count FROM narrative_entity_counts WHERE narrative_id = n.id), 0) as entity_count
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
                    description=row["description"] or "",
                    topics=topics,
                    platforms=row["platforms"] or [],
                    total_views=row["total_views"] or 0,
                    total_likes=row["total_likes"] or 0,
                    total_comments=row["total_comments"] or 0,
                    claim_count=row["claim_count"] or 0,
                    video_count=row["video_count"] or 0,
                    language_count=row["language_count"] or 0,
                    entity_count=row["entity_count"] or 0,
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            )

        return summaries

    async def get_all_virality_percentiles_for_date(
        self, calc_date: date
    ) -> dict[UUID, dict[str, ViralityScoreRank]]:
        """
        Get the percentile rank AND the raw score of every narrative's virality scores
        for a given date. Computes all percentiles in a single query.

        Returns a dict keyed by narrative_id, each value being a dict of
        score_type → (percentile, score). The raw score comes back alongside the rank
        because the detail view headlines the narrative's own magnitude — how many views
        it has — and only puts the rank on the line beneath it; a rank on its own cannot
        answer "how large is this", only "larger than whom".
        """
        await self._session.execute(
            """
            WITH day_scores AS (
                SELECT DISTINCT ON (narrative_id, score_type)
                    narrative_id,
                    score_type,
                    score_value
                FROM narrative_virality_scores
                WHERE calculated_at::date = %(calc_date)s
                ORDER BY narrative_id, score_type, calculated_at DESC
            ),
            ranked AS (
                SELECT
                    narrative_id,
                    score_type,
                    score_value,
                    PERCENT_RANK() OVER (
                        PARTITION BY score_type
                        ORDER BY score_value
                    ) AS percentile
                FROM day_scores
            )
            SELECT narrative_id, score_type, score_value, percentile
            FROM ranked
            """,
            {"calc_date": calc_date},
        )
        rows = await self._session.fetchall()
        result: dict[UUID, dict[str, ViralityScoreRank]] = {}
        for row in rows:
            result.setdefault(row["narrative_id"], {})[row["score_type"]] = ViralityScoreRank(
                percentile=row["percentile"],
                score=row["score_value"],
            )
        return result

    async def bulk_insert_narrative_analysis_indicators(
        self,
        records: list[tuple[UUID, float, NarrativeAnalysisIndicatorType, dict | None]],
        calc_date: date | None = None,
    ) -> None:
        """
        Bulk insert analysis indicators for multiple narratives at once.
        Each record is a (narrative_id, indicator_value, indicator_type) tuple.

        `calculated_at` records the day the indicator *describes*, not the moment the
        row was written, because that is how it is read back:
        get_bulk_analysis_indicators_for_date filters `calculated_at::date = calc_date`.
        Stamping NOW() instead only agrees with the reader when a run scores the same
        day it executes; once the pipeline began scoring the last *completed* day, a
        00:05 run wrote rows dated today and then asked for rows dated yesterday,
        silently classifying against the previous run's output.

        Mirrors insert_narrative_virality_score: the wall-clock time is kept so reruns
        on the same day still order newest-last. Defaults to the current timestamp.

        The time comes from statement_timestamp(), not LOCALTIME, because LOCALTIME is
        frozen at transaction start: two writes sharing a transaction would get an
        identical stamp, and get_bulk_analysis_indicators_for_date's
        `ORDER BY calculated_at DESC` has no tiebreaker to fall back on (the PK is a
        random uuid), so the winner would be whichever row the scan happened to reach
        first. statement_timestamp() advances per statement, so the later write is
        always the later stamp.
        """
        await self._session.executemany(
            """
            INSERT INTO narrative_analysis_indicators (narrative_id, indicator_value, indicator_type, metadata, calculated_at)
            VALUES (
                %(narrative_id)s, %(indicator_value)s, %(indicator_type)s, %(metadata)s,
                COALESCE(%(calc_date)s::date, CURRENT_DATE) + statement_timestamp()::time
            )
            """,
            [
                {
                    "narrative_id": narrative_id,
                    "indicator_value": indicator_value,
                    "indicator_type": indicator_type.value,
                    "metadata": Jsonb(metadata) if metadata is not None else None,
                    "calc_date": calc_date,
                }
                for narrative_id, indicator_value, indicator_type, metadata in records
            ],
        )

    async def get_composite_cohort(self, calc_date: date) -> list[dict]:
        """
        The VIRALITY-STATE cohort and its raw scores, in one pass (D0 + D3).

        Every narrative with at least one video that has ever been measured is in, with
        its state carried forward from the last snapshot on or before `calc_date`. A
        level does not become unknown just because we did not look today, so composite
        ranks over the whole corpus (~22k) rather than the ~2-3k the dashboard query
        happened to return. That also makes the rank stable: ranking only among today's
        revisited narratives made "how big am I" swing day to day depending on who else
        got scraped, which is scraper noise injected straight into the level axis.

        `reach_score` is the narrative's summed views, raw. It used to be
        `min(views / avg_views, 10) / 10`; both halves of that are gone, and the corpus
        average with them. The caller percent-ranks this column, and a rank does not
        change under a monotonic transform, so dividing by a per-run constant sorted
        nobody differently — while the clip flattened the entire top of the axis into a
        tie. Production had a second defect here that the removal also settles: it
        divided a video_stats numerator by a videos-table denominator, two sources that
        drift apart. There is now no denominator to get wrong.

        Returns one row per narrative with engagement_score and reach_score ready to be
        percent-ranked. Narratives with no snapshot on or before `calc_date` are absent,
        not zero — they are unmeasured, and ranking them at the bottom would be a claim
        we cannot back (C1).
        """
        await self._session.execute(
            """
            WITH links AS (
                SELECT DISTINCT cn.narrative_id, v.id AS video_id
                FROM videos v
                JOIN video_claims c ON c.video_id = v.id
                JOIN claim_narratives cn ON cn.claim_id = c.id
            ),
            latest AS (
                SELECT DISTINCT ON (l.narrative_id, l.video_id)
                    l.narrative_id,
                    l.video_id,
                    vs.views,
                    vs.likes,
                    vs.comments
                FROM links l
                JOIN video_stats vs ON vs.video_id = l.video_id
                WHERE vs.recorded_at::date <= %(calc_date)s
                ORDER BY l.narrative_id, l.video_id, vs.recorded_at DESC
            ),
            narrative_state AS (
                SELECT
                    narrative_id,
                    COUNT(*)::int              AS video_count,
                    COALESCE(SUM(views), 0)    AS views,
                    COALESCE(SUM(likes), 0)    AS likes,
                    COALESCE(SUM(comments), 0) AS comments
                FROM latest
                GROUP BY narrative_id
            )
            SELECT
                ns.narrative_id,
                ns.video_count,
                ns.views::bigint    AS views,
                ns.likes::bigint    AS likes,
                ns.comments::bigint AS comments,
                CASE WHEN ns.views > 0
                     THEN (ns.likes * %(likes_weight)s + ns.comments * %(comments_weight)s) / ns.views
                     ELSE 0.0
                END::float AS engagement_score,
                ns.views::float AS reach_score
            FROM narrative_state ns
            """,
            {
                "calc_date": calc_date,
                "likes_weight": VIRALITY_SCORE_LIKES_WEIGHT,
                "comments_weight": VIRALITY_SCORE_COMMENTS_WEIGHT,
            },
        )
        return await self._session.fetchall()

    async def get_acceleration_cohort(self, calc_date: date) -> list[dict]:
        """
        The CHANGE-IN-VIRALITY cohort and its per-day change components (D0 + D4).

        A rate needs two observations bracketing the interval, so only narratives we
        provably visited on `calc_date` are in. Visits are evidenced two ways, because
        neither alone suffices:

            a `video_stats` row on calc_date   -- proves a visit, append-only, permanent
            videos.updated_at::date = calc_date -- catches visits that changed nothing,
                                                   but is overwritten by the next scrape

        The union misses exactly one case: visited, nothing changed, and re-visited
        since. That is why a past date cannot be re-scored faithfully — the flat half of
        the cohort erodes while the mover half survives.

        Growth is per elapsed day (D4): each video's gain is divided by the number of
        days since *that video* was last fetched, so a video last seen four days ago
        contributes four days of growth as one day's worth, instead of ranking high for
        having gone unmeasured. Videos are aggregated the way that avoids averaging
        ratios:

            change_views = [ SUM(refreshed video's gain / that video's own gap)
                             + SUM(views of videos that arrived) ]
                           / TOTAL views yesterday

        The numerator is already per-day on both halves: a refreshed video's gain is
        divided by its own snapshot gap (D4), and an arrival's views accrued inside the
        single day between prev_date and calc_date, which is the whole window. The
        denominator is the narrative's entire previous state, not the slice that moved.
        The service does the division; this query supplies both halves, the previous
        state, and the mean gap that the engagement ratio needs.

        Two earlier designs are buried in that one line. It used to divide the refreshed
        videos' gain by the refreshed videos' own baseline, which is unbiased only if
        they are representative, and measured on 2026-08-12 they are not: median
        coverage is 5% of a narrative's views, while the median narrative *badged viral*
        sat at 0.74%, because a thin sample produces a large ratio and the ratio earns
        the badge. One narrative reported +53% daily growth from 2 videos of 63 covering
        0.62% of its views, against actual movement of +0.32%. Dividing by the whole
        previous state fixed that (O5.3/O5.6).

        It then held the video set FIXED across the two days, which zeroed every
        narrative whose only growth was a new video — and since the video-count term was
        removed at the same time, nothing was left to see it at all. Letting the current
        state include arrivals restores that signal in views rather than in video counts,
        which is the unit the axis is denominated in.

        A baseline's AGE does not disqualify it. Growth per day is the whole point of
        the per-day division: a video last seen twenty days ago contributes its twenty
        days of growth as one day's worth, which is the same treatment a four-day gap
        gets, and there is no age at which that stops being the right arithmetic. An
        earlier draft dropped baselines older than 14 days; that bound is gone, because
        it did not exclude the narrative — the LEFT JOIN below handed it a rate of
        exactly 0.0 instead, turning "the baseline is too old to say" into the positive
        claim "this stopped growing", which is the C1 error one level down.

        Narratives that had no videos the day before are excluded by the inner join to
        the previous day's state: a narrative with none was *created* on calc_date, and
        that is birth, not acceleration.
        """
        prev_date = calc_date - timedelta(days=1)
        await self._session.execute(
            """
            WITH links AS (
                SELECT DISTINCT cn.narrative_id, v.id AS video_id
                FROM videos v
                JOIN video_claims c ON c.video_id = v.id
                JOIN claim_narratives cn ON cn.claim_id = c.id
            ),
            visited AS (
                SELECT DISTINCT l.narrative_id
                FROM links l
                JOIN videos v ON v.id = l.video_id
                WHERE v.updated_at::date = %(calc_date)s
                UNION
                SELECT DISTINCT l.narrative_id
                FROM links l
                JOIN video_stats vs ON vs.video_id = l.video_id
                WHERE vs.recorded_at::date = %(calc_date)s
            ),
            cur AS (
                SELECT DISTINCT ON (l.narrative_id, l.video_id)
                    l.narrative_id,
                    l.video_id,
                    vs.views,
                    vs.likes,
                    vs.comments,
                    vs.recorded_at::date AS recorded_on
                FROM links l
                JOIN visited vi ON vi.narrative_id = l.narrative_id
                JOIN video_stats vs ON vs.video_id = l.video_id
                WHERE vs.recorded_at::date <= %(calc_date)s
                ORDER BY l.narrative_id, l.video_id, vs.recorded_at DESC
            ),
            prev AS (
                SELECT DISTINCT ON (l.narrative_id, l.video_id)
                    l.narrative_id,
                    l.video_id,
                    vs.views,
                    vs.likes,
                    vs.comments,
                    vs.recorded_at::date AS recorded_on
                FROM links l
                JOIN visited vi ON vi.narrative_id = l.narrative_id
                JOIN video_stats vs ON vs.video_id = l.video_id
                WHERE vs.recorded_at::date <= %(prev_date)s
                ORDER BY l.narrative_id, l.video_id, vs.recorded_at DESC
            ),
            -- The BALANCED PANEL: every linked video observed on both days. Videos that
            -- were not re-fetched carry forward, so they sit on both sides identically
            -- and move no ratio — they only enlarge the denominator, which is the point.
            -- Videos with no prev-side observation at all are excluded: they are brand
            -- new, and a growing footprint is what the video-count term measures.
            panel AS (
                SELECT
                    c.narrative_id,
                    c.video_id,
                    (c.recorded_on > p.recorded_on)        AS was_refreshed,
                    (c.recorded_on - p.recorded_on)::float AS gap_days,
                    c.views    AS cur_views,
                    p.views    AS prev_views,
                    c.likes    AS cur_likes,
                    p.likes    AS prev_likes,
                    c.comments AS cur_comments,
                    p.comments AS prev_comments
                FROM cur c
                JOIN prev p ON p.narrative_id = c.narrative_id AND p.video_id = c.video_id
            ),
            -- Videos actually re-fetched in the window: their latest snapshot as of
            -- calc_date is newer than their latest as of prev_date. They supply the
            -- NUMERATOR — all observed movement comes from them — while the panel above
            -- supplies the denominator.
            refreshed AS (
                SELECT * FROM panel WHERE was_refreshed
            ),
            -- Narrative-level state on each day, over EVERY linked video and not only
            -- the ones seen on both. `prev` is a subset of `cur` by construction — a
            -- video with a snapshot on or before prev_date has one on or before
            -- calc_date — so the difference between these two aggregates is exactly the
            -- movement of the refreshed videos PLUS the arrival of videos that were not
            -- linked to the narrative yesterday.
            --
            -- Counting arrivals as growth is deliberate. A narrative that spreads by
            -- spawning new videos has no other evidence of it: those videos are absent
            -- from the previous day by definition, so any comparison restricted to
            -- videos present on both days scores it at exactly zero. The cost is that a
            -- video newly *discovered* is indistinguishable from a video newly
            -- *uploaded* — an old video with a large lifetime view count arrives as one
            -- window's growth. That is an accepted trade rather than an oversight
            -- (2026-08-14): new to us is treated as new to the world.
            cur_totals AS (
                SELECT
                    narrative_id,
                    SUM(views)::float    AS cur_views_total,
                    SUM(likes)::float    AS cur_likes_total,
                    SUM(comments)::float AS cur_comments_total
                FROM cur
                GROUP BY narrative_id
            ),
            -- Videos linked to the narrative since prev_date: present in `cur`, absent
            -- from `prev`. Their views are growth, and they accrued inside the one-day
            -- window — there is no earlier observation to date them from, and the window
            -- between prev_date and calc_date is exactly one day. So unlike a refreshed
            -- video, whose gain is divided by its own snapshot gap, an arrival's views
            -- are already a per-day quantity.
            arrivals AS (
                SELECT
                    c.narrative_id,
                    COUNT(*)::int      AS new_videos,
                    SUM(c.views)::float AS new_video_views
                FROM cur c
                LEFT JOIN prev p ON p.narrative_id = c.narrative_id AND p.video_id = c.video_id
                WHERE p.video_id IS NULL
                GROUP BY c.narrative_id
            ),
            prev_totals AS (
                SELECT
                    narrative_id,
                    SUM(views)::float    AS prev_views_total,
                    SUM(likes)::float    AS prev_likes_total,
                    SUM(comments)::float AS prev_comments_total
                FROM prev
                GROUP BY narrative_id
            ),
            -- The clean aggregation: sum the per-day gains, sum the baselines they came
            -- from, divide once. Averaging per-video ratios would let a video with a
            -- 1-view baseline outvote the rest of the narrative.
            growth AS (
                SELECT
                    narrative_id,
                    COUNT(*)::int AS refreshed_videos,
                    SUM((cur_views - prev_views) / NULLIF(gap_days, 0))::float AS daily_view_gain,
                    SUM(prev_views)::float                                     AS baseline_views,
                    SUM(cur_views)::float                                      AS cur_views,
                    SUM(cur_likes)::float                                      AS cur_likes,
                    SUM(cur_comments)::float                                   AS cur_comments,
                    SUM(prev_likes)::float                                     AS prev_likes,
                    SUM(prev_comments)::float                                  AS prev_comments,
                    -- One narrative-level engagement ratio cannot carry per-video gaps,
                    -- so the engagement change is divided by the baseline-weighted mean
                    -- gap of the same videos. The design fixes the per-day rule but not
                    -- this weighting; it is chosen to match the views denominator.
                    CASE WHEN SUM(prev_views) > 0
                         THEN SUM(prev_views * gap_days) / SUM(prev_views)
                         ELSE AVG(gap_days)
                    END::float AS mean_gap_days
                FROM refreshed
                GROUP BY narrative_id
            ),
            -- Video COUNT is observed exactly on both days, so it needs no snapshot-gap
            -- normalisation: the window already is one day. Counted over every linked
            -- video, not only the refreshed ones -- the narrative's whole footprint.
            counts AS (
                SELECT
                    c.narrative_id,
                    COUNT(*)::float AS cur_videos,
                    COUNT(p.video_id)::float AS prev_videos
                FROM cur c
                LEFT JOIN prev p
                       ON p.narrative_id = c.narrative_id AND p.video_id = c.video_id
                GROUP BY c.narrative_id
            )
            SELECT
                co.narrative_id,
                co.cur_videos,
                co.prev_videos,
                COALESCE(g.refreshed_videos, 0)  AS refreshed_videos,
                COALESCE(g.daily_view_gain, 0.0) AS daily_view_gain,
                COALESCE(g.baseline_views, 0.0)  AS baseline_views,
                COALESCE(g.mean_gap_days, 0.0)   AS mean_gap_days,
                COALESCE(g.cur_views, 0.0)       AS cur_views,
                COALESCE(g.cur_likes, 0.0)       AS cur_likes,
                COALESCE(g.cur_comments, 0.0)    AS cur_comments,
                COALESCE(g.prev_likes, 0.0)      AS prev_likes,
                COALESCE(g.prev_comments, 0.0)   AS prev_comments,
                COALESCE(ct.cur_views_total, 0.0)     AS cur_views_total,
                COALESCE(ct.cur_likes_total, 0.0)     AS cur_likes_total,
                COALESCE(ct.cur_comments_total, 0.0)  AS cur_comments_total,
                COALESCE(pt.prev_views_total, 0.0)    AS prev_views_total,
                COALESCE(pt.prev_likes_total, 0.0)    AS prev_likes_total,
                COALESCE(pt.prev_comments_total, 0.0) AS prev_comments_total,
                COALESCE(a.new_videos, 0)             AS new_videos,
                COALESCE(a.new_video_views, 0.0)      AS new_video_views
            FROM counts co
            LEFT JOIN growth g       ON g.narrative_id = co.narrative_id
            LEFT JOIN cur_totals ct  ON ct.narrative_id = co.narrative_id
            LEFT JOIN prev_totals pt ON pt.narrative_id = co.narrative_id
            LEFT JOIN arrivals a     ON a.narrative_id = co.narrative_id
            WHERE co.prev_videos > 0
              -- AND SOMETHING WAS OBSERVED. `visited` is satisfied by a video row being
              -- touched, which is not the same as a video moving: a narrative can enter
              -- the cohort with every snapshot unchanged and nothing newly linked, in
              -- which case `cur` and `prev` are the same state and every component is
              -- exactly zero. Ranked, that zero lands at the bottom of the axis and the
              -- narrative collects `consolidated` — "large, and no longer growing" —
              -- asserted about a narrative nobody looked at. Measured on the 2026-08-13
              -- production cohort that was 8 narratives, 4 of them badged.
              --
              -- We only rank what we measured (C1), so they leave the cohort rather than
              -- being ranked at an honest-looking zero. Downstream they have no
              -- acceleration row, are unclassifiable, and lose the badge they held.
              AND (COALESCE(g.refreshed_videos, 0) > 0 OR COALESCE(a.new_videos, 0) > 0)
            """,
            {"calc_date": calc_date, "prev_date": prev_date},
        )
        return await self._session.fetchall()

    async def bulk_insert_narrative_virality_scores(
        self,
        records: list[tuple[UUID, float, NarrativeViralityScoreType, dict | None]],
        calc_date: date | None = None,
    ) -> None:
        """
        Bulk form of insert_narrative_virality_score, with the same `calculated_at`
        semantics: the row is stamped with the day it *describes*, keeping the wall-clock
        time so reruns on the same day still order newest-last. As in
        bulk_insert_narrative_analysis_indicators the time is statement_timestamp(),
        which advances per statement, rather than LOCALTIME, which is frozen for the
        whole transaction and would tie two writes that share one.

        The per-narrative form opened two nested transactions and issued three inserts
        each. At the composite cohort's real size (~22k narratives under D3, not the
        ~2k the dashboard query returned) that is ~44k transactions per run, which is
        what made the expanded pool unshippable.
        """
        await self._session.executemany(
            """
            INSERT INTO narrative_virality_scores (narrative_id, score_value, score_type, metadata, calculated_at)
            VALUES (
                %(narrative_id)s, %(score_value)s, %(score_type)s, %(metadata)s,
                COALESCE(%(calc_date)s::date, CURRENT_DATE) + statement_timestamp()::time
            )
            """,
            [
                {
                    "narrative_id": narrative_id,
                    "score_value": score_value,
                    "score_type": score_type.value,
                    "metadata": Jsonb(metadata) if metadata is not None else None,
                    "calc_date": calc_date,
                }
                for narrative_id, score_value, score_type, metadata in records
            ],
        )

    async def get_bulk_analysis_indicators_for_date(
        self, calc_date: date
    ) -> dict[UUID, dict[str, IndicatorPayload]]:
        """
        Get the latest composite_virality and acceleration_rate per narrative for a given date.
        Returns narrative_id → {indicator_type: {"value": float, "metadata": dict}}.

        The metadata is carried because both indicators are classified on their
        percentile rank within the run's cohort, which lives there rather than in
        indicator_value.
        """
        await self._session.execute(
            """
            SELECT DISTINCT ON (narrative_id, indicator_type)
                narrative_id,
                indicator_type,
                indicator_value,
                metadata
            FROM narrative_analysis_indicators
            WHERE calculated_at::date = %(calc_date)s
              AND indicator_type IN ('composite_virality', 'acceleration_rate')
            ORDER BY narrative_id, indicator_type, calculated_at DESC
            """,
            {"calc_date": calc_date},
        )
        rows = await self._session.fetchall()
        result: dict[UUID, dict[str, IndicatorPayload]] = {}
        for row in rows:
            result.setdefault(row["narrative_id"], {})[row["indicator_type"]] = {
                "value": row["indicator_value"],
                "metadata": row["metadata"] or {},
            }
        return result

    async def clear_spread_patterns_except(self, narrative_ids: list[UUID]) -> None:
        """
        Null out spread_pattern for every narrative outside `narrative_ids`.

        A narrative that could not be scored this run — missing either indicator —
        must not keep yesterday's badge. NULL means "not scoreable", which is distinct
        from the NONE pattern meaning "scored, nothing notable". Callers must not pass an
        empty list: that would clear every badge in the table, which is what a failed
        run looks like.
        """
        if not narrative_ids:
            return
        await self._session.execute(
            """
            UPDATE narratives
            SET spread_pattern = NULL, updated_at = NOW()
            WHERE spread_pattern IS NOT NULL
              AND NOT (id = ANY(%(narrative_ids)s))
            """,
            {"narrative_ids": narrative_ids},
        )

    async def bulk_update_narrative_spread_patterns(
        self, records: list[tuple[UUID, NarrativeSpreadPattern]]
    ) -> None:
        """
        Bulk update spread_pattern for multiple narratives.
        Each record is a (narrative_id, spread_pattern) tuple.
        """
        await self._session.executemany(
            """
            UPDATE narratives
            SET spread_pattern = %(spread_pattern)s, updated_at = NOW()
            WHERE id = %(narrative_id)s
            """,
            [
                {"narrative_id": narrative_id, "spread_pattern": spread_pattern.value}
                for narrative_id, spread_pattern in records
            ],
        )

    async def get_narrative_analysis_indicators(
        self, narrative_id: UUID, date_from: datetime | None = None, date_to: datetime | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"narrative_id": narrative_id}

        if date_from is None and date_to is None:
            query = """
                SELECT id, indicator_value, indicator_type, metadata, calculated_at
                FROM narrative_analysis_indicators
                WHERE narrative_id = %(narrative_id)s
                AND calculated_at::date = (
                    SELECT MAX(calculated_at::date)
                    FROM narrative_analysis_indicators
                    WHERE narrative_id = %(narrative_id)s
                )
                ORDER BY calculated_at DESC
            """
        else:
            query = """
                SELECT id, indicator_value, indicator_type, metadata, calculated_at
                FROM narrative_analysis_indicators
                WHERE narrative_id = %(narrative_id)s
            """
            if date_from:
                query += " AND calculated_at >= %(date_from)s"
                params["date_from"] = date_from
            if date_to:
                query += " AND calculated_at <= %(date_to)s"
                params["date_to"] = date_to
            query += " ORDER BY calculated_at DESC"

        await self._session.execute(query, params)
        rows = await self._session.fetchall()

        return [
            {
                "id": row["id"],
                "indicator_value": row["indicator_value"],
                "indicator_type": row["indicator_type"],
                "metadata": row["metadata"],
                "calculated_at": row["calculated_at"],
            }
            for row in rows
        ]

    async def delete_claim_from_narrative(self, narrative_id: UUID, claim_id: UUID) -> bool:
        """Remove association of a claim from a narrative."""
        await self._session.execute(
            """
            DELETE FROM claim_narratives
            WHERE narrative_id = %(narrative_id)s AND claim_id = %(claim_id)s
            """,
            {"narrative_id": narrative_id, "claim_id": claim_id},
        )
        return True

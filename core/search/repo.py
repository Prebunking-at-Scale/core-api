from typing import cast
from uuid import UUID

import psycopg
from psycopg.rows import DictRow

from core.narratives.models import TopicSummary
from core.search.models import (
    COUNT_CAP,
    ChannelOption,
    ClaimVideo,
    EntityRef,
    LanguageOption,
    MatchSource,
    NarrativeRef,
    SearchClaim,
    SearchFilters,
    SearchNarrative,
    SearchTab,
    SearchVideo,
)
from core.search.query import QUERIES

_ID_COLUMN = {"narratives": "n.id", "claims": "c.id", "videos": "v.id"}


class SearchRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def count(self, tab: SearchTab, filters: SearchFilters) -> tuple[int, bool]:
        """Matches for one tab, counted up to COUNT_CAP: (total, capped)."""
        query = QUERIES[tab](filters)
        await self._session.execute(
            f"""
            SELECT count(*) AS total FROM (
                SELECT 1 FROM {query.from_sql}
                WHERE {query.where_sql}
                LIMIT {COUNT_CAP + 1}
            ) matches
            """,
            query.params,
        )
        row = await self._session.fetchone()
        total = int(row["total"]) if row else 0
        return min(total, COUNT_CAP), total > COUNT_CAP

    async def page_ids(
        self, tab: SearchTab, filters: SearchFilters, limit: int, offset: int
    ) -> list[tuple[UUID, MatchSource]]:
        """The ids on one page, in order, each with how it matched."""
        query = QUERIES[tab](filters)
        await self._session.execute(
            f"""
            SELECT {_ID_COLUMN[tab]} AS id, {query.match_source_sql} AS match_source
            FROM {query.from_sql}
            WHERE {query.where_sql}
            ORDER BY {query.order_sql}
            LIMIT %(_limit)s OFFSET %(_offset)s
            """,
            {**query.params, "_limit": limit, "_offset": offset},
        )
        return [
            (row["id"], cast(MatchSource, row["match_source"]))
            for row in await self._session.fetchall()
        ]

    # Narratives ----------------------------------------------------------------

    async def narratives(self, page: list[tuple[UUID, MatchSource]]) -> list[SearchNarrative]:
        """The page's narratives with their sizes, as the narratives list gives them
        (core/narratives/repo.py, get_all_narratives_list), in the page's order."""
        if not page:
            return []
        ids = [narrative_id for narrative_id, _ in page]
        await self._session.execute(
            """
            WITH page AS (
                SELECT n.id, n.title, n.description, n.created_at, n.updated_at, n.spread_pattern
                FROM narratives n
                WHERE n.id = ANY(%(ids)s)
            ),
            claims AS (
                SELECT p.id AS narrative_id, COUNT(DISTINCT cn.claim_id) AS claim_count
                FROM page p
                LEFT JOIN claim_narratives cn ON cn.narrative_id = p.id
                GROUP BY p.id
            ),
            distinct_videos AS (
                SELECT DISTINCT p.id AS narrative_id, v.id AS video_id,
                       v.views, v.likes, v.comments, v.platform
                FROM page p
                JOIN claim_narratives cn ON cn.narrative_id = p.id
                JOIN video_claims vc ON vc.id = cn.claim_id
                JOIN videos v ON v.id = vc.video_id
            ),
            videos AS (
                SELECT narrative_id,
                       COUNT(video_id) AS video_count,
                       COALESCE(SUM(views), 0) AS total_views,
                       COALESCE(SUM(likes), 0) AS total_likes,
                       COALESCE(SUM(comments), 0) AS total_comments,
                       ARRAY_AGG(DISTINCT platform) FILTER (WHERE platform IS NOT NULL) AS platforms
                FROM distinct_videos
                GROUP BY narrative_id
            ),
            languages AS (
                SELECT p.id AS narrative_id,
                       COUNT(DISTINCT vc.metadata->>'language') FILTER (
                           WHERE vc.metadata->>'language' IS NOT NULL
                           AND vc.metadata->>'language' != ''
                       ) AS language_count
                FROM page p
                LEFT JOIN claim_narratives cn ON cn.narrative_id = p.id
                LEFT JOIN video_claims vc ON vc.id = cn.claim_id
                GROUP BY p.id
            ),
            entities AS (
                SELECT p.id AS narrative_id, COUNT(DISTINCT ne.entity_id) AS entity_count
                FROM page p
                LEFT JOIN narrative_entities ne ON ne.narrative_id = p.id
                GROUP BY p.id
            ),
            topics AS (
                SELECT nt.narrative_id,
                       JSON_AGG(JSON_BUILD_OBJECT('id', t.id, 'topic', t.topic) ORDER BY t.topic) AS topics
                FROM narrative_topics nt
                JOIN topics t ON t.id = nt.topic_id
                WHERE nt.narrative_id = ANY(%(ids)s)
                GROUP BY nt.narrative_id
            ),
            ratings AS (
                SELECT nf.narrative_id, COUNT(*) AS score_count,
                       AVG(nf.feedback_score)::float AS average_score
                FROM narrative_feedback nf
                WHERE nf.narrative_id = ANY(%(ids)s)
                GROUP BY nf.narrative_id
            )
            SELECT p.*,
                   COALESCE(t.topics, '[]'::json) AS topics,
                   COALESCE(c.claim_count, 0) AS claim_count,
                   COALESCE(v.video_count, 0) AS video_count,
                   COALESCE(v.total_views, 0) AS total_views,
                   COALESCE(v.total_likes, 0) AS total_likes,
                   COALESCE(v.total_comments, 0) AS total_comments,
                   COALESCE(v.platforms, ARRAY[]::text[]) AS platforms,
                   COALESCE(l.language_count, 0) AS language_count,
                   COALESCE(e.entity_count, 0) AS entity_count,
                   COALESCE(r.score_count, 0) AS score_count,
                   r.average_score
            FROM page p
            LEFT JOIN claims c ON c.narrative_id = p.id
            LEFT JOIN videos v ON v.narrative_id = p.id
            LEFT JOIN languages l ON l.narrative_id = p.id
            LEFT JOIN entities e ON e.narrative_id = p.id
            LEFT JOIN topics t ON t.narrative_id = p.id
            LEFT JOIN ratings r ON r.narrative_id = p.id
            """,
            {"ids": ids},
        )
        rows = {row["id"]: row for row in await self._session.fetchall()}
        out = []
        for narrative_id, match_source in page:
            row = rows.get(narrative_id)
            if not row:
                continue
            out.append(
                SearchNarrative(
                    **{
                        **row,
                        "description": row["description"] or "",
                        "topics": [TopicSummary(**t) for t in row["topics"] or []],
                        "platforms": row["platforms"] or [],
                    },
                    match_source=match_source,
                )
            )
        return out

    # Claims --------------------------------------------------------------------

    async def claims(
        self, page: list[tuple[UUID, MatchSource]], entity_ids: list[UUID]
    ) -> list[SearchClaim]:
        """The page's claims, each with its video, all its narratives and its own
        topics, loaded for the whole page at once."""
        if not page:
            return []
        ids = [claim_id for claim_id, _ in page]

        await self._session.execute(
            """
            SELECT c.id, c.video_id, c.claim, c.start_time_s, c.metadata, c.created_at, c.updated_at,
                   v.id AS v_id, v.title AS v_title, v.description AS v_description,
                   v.platform AS v_platform, v.source_url AS v_source_url, v.channel AS v_channel,
                   v.uploaded_at AS v_uploaded_at, v.views AS v_views, v.likes AS v_likes,
                   v.comments AS v_comments, v.metadata AS v_metadata
            FROM video_claims c
            LEFT JOIN videos v ON v.id = c.video_id
            WHERE c.id = ANY(%(ids)s)
            """,
            {"ids": ids},
        )
        rows = {row["id"]: row for row in await self._session.fetchall()}

        await self._session.execute(
            """
            SELECT cn.claim_id, n.id, n.title
            FROM claim_narratives cn
            JOIN narratives n ON n.id = cn.narrative_id
            WHERE cn.claim_id = ANY(%(ids)s)
            ORDER BY n.created_at DESC
            """,
            {"ids": ids},
        )
        narratives: dict[UUID, list[NarrativeRef]] = {}
        for row in await self._session.fetchall():
            narratives.setdefault(row["claim_id"], []).append(
                NarrativeRef(id=row["id"], title=row["title"])
            )

        # The claim's own topics are those the narratives service's classifier assigned
        # (claim_topics), never its narrative's.
        await self._session.execute(
            """
            SELECT ct.claim_id, t.id, t.topic
            FROM claim_topics ct
            JOIN topics t ON t.id = ct.topic_id
            WHERE ct.claim_id = ANY(%(ids)s)
            ORDER BY t.topic
            """,
            {"ids": ids},
        )
        topics: dict[UUID, list[TopicSummary]] = {}
        for row in await self._session.fetchall():
            topics.setdefault(row["claim_id"], []).append(TopicSummary(id=row["id"], topic=row["topic"]))

        via = await self._entities_via_narrative(ids, entity_ids) if entity_ids else {}

        out = []
        for claim_id, _ in page:
            claim_row = rows.get(claim_id)
            if not claim_row:
                continue
            row = claim_row
            own_topics = topics.get(claim_id, [])
            video = None
            if row["v_id"]:
                video = ClaimVideo(
                    id=row["v_id"],
                    title=row["v_title"],
                    description=row["v_description"] or "",
                    platform=row["v_platform"],
                    source_url=row["v_source_url"],
                    channel=row["v_channel"],
                    uploaded_at=row["v_uploaded_at"],
                    views=row["v_views"],
                    likes=row["v_likes"],
                    comments=row["v_comments"],
                    metadata=row["v_metadata"] or {},
                )
            via_entities = via.get(claim_id, [])
            out.append(
                SearchClaim(
                    id=row["id"],
                    video_id=row["video_id"],
                    claim=row["claim"],
                    start_time_s=row["start_time_s"],
                    metadata=row["metadata"] or {},
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                    topics=own_topics,
                    narratives=narratives.get(claim_id, []),
                    video=video,
                    match_source="narrative" if via_entities else "direct",
                    via_entities=via_entities,
                )
            )
        return out

    async def _entities_via_narrative(
        self, claim_ids: list[UUID], entity_ids: list[UUID]
    ) -> dict[UUID, list[EntityRef]]:
        """The filtered entities each claim has only through its narratives. A claim
        whose own text names one of them matched directly (a text match on the
        entity's name, until claims have entities of their own)."""
        await self._session.execute(
            """
            SELECT DISTINCT cn.claim_id, e.id, e.name,
                   strpos(normalize_text(c.claim), normalize_text(e.name)) > 0 AS named
            FROM claim_narratives cn
            JOIN narrative_entities ne ON ne.narrative_id = cn.narrative_id
            JOIN entities e ON e.id = ne.entity_id
            JOIN video_claims c ON c.id = cn.claim_id
            WHERE cn.claim_id = ANY(%(claim_ids)s) AND e.id = ANY(%(entity_ids)s)
            ORDER BY e.name
            """,
            {"claim_ids": claim_ids, "entity_ids": entity_ids},
        )
        found: dict[UUID, list[EntityRef]] = {}
        named: set[UUID] = set()
        for row in await self._session.fetchall():
            if row["named"]:
                named.add(row["claim_id"])
            found.setdefault(row["claim_id"], []).append(
                EntityRef(id=row["id"], name=row["name"])
            )
        return {claim_id: refs for claim_id, refs in found.items() if claim_id not in named}

    # Videos --------------------------------------------------------------------

    async def videos(self, page: list[tuple[UUID, MatchSource]]) -> list[SearchVideo]:
        if not page:
            return []
        await self._session.execute(
            """
            SELECT id, title, description, platform, source_url, destination_path,
                   uploaded_at, views, likes, comments, channel, channel_followers, metadata
            FROM videos
            WHERE id = ANY(%(ids)s)
            """,
            {"ids": [video_id for video_id, _ in page]},
        )
        rows = {row["id"]: row for row in await self._session.fetchall()}
        return [
            SearchVideo(**{**rows[video_id], "metadata": rows[video_id]["metadata"] or {}}, match_source=match_source)
            for video_id, match_source in page
            if video_id in rows
        ]

    # Filter options ------------------------------------------------------------

    async def own_channels(
        self, organisation_id: UUID, platforms: list[str]
    ) -> list[ChannelOption]:
        """The organisation's channel feeds: what "Ours" puts in the filter."""
        await self._session.execute(
            """
            SELECT channel, platform
            FROM channel_feeds
            WHERE organisation_id = %(organisation_id)s
              AND NOT is_archived
              AND (cardinality(%(platforms)s::text[]) = 0 OR platform = ANY(%(platforms)s))
            ORDER BY lower(channel)
            """,
            {"organisation_id": organisation_id, "platforms": platforms},
        )
        return [
            ChannelOption(channel=row["channel"], platform=row["platform"], is_own=True)
            for row in await self._session.fetchall()
        ]

    async def search_channels(
        self,
        text: str,
        platforms: list[str],
        organisation_id: UUID | None,
        limit: int,
    ) -> list[ChannelOption]:
        """Channels of collected videos whose name contains the text, ignoring case:
        the organisation's own first, then the ones with most videos."""
        pattern = "%" + text.lower().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        await self._session.execute(
            """
            WITH found AS (
                SELECT min(v.channel) AS channel, v.platform, count(*) AS videos
                FROM videos v
                WHERE v.channel IS NOT NULL AND v.channel <> ''
                  AND lower(v.channel) LIKE %(pattern)s
                  AND (cardinality(%(platforms)s::text[]) = 0 OR v.platform = ANY(%(platforms)s))
                GROUP BY v.platform, lower(v.channel)
            )
            SELECT f.channel, f.platform,
                   EXISTS (
                       SELECT 1 FROM channel_feeds cf
                       WHERE cf.organisation_id = %(organisation_id)s
                         AND NOT cf.is_archived
                         AND cf.platform = f.platform
                         AND lower(cf.channel) = lower(f.channel)
                   ) AS is_own
            FROM found f
            ORDER BY is_own DESC, f.videos DESC, lower(f.channel)
            LIMIT %(limit)s
            """,
            {
                "pattern": pattern,
                "platforms": platforms,
                "organisation_id": organisation_id,
                "limit": limit,
            },
        )
        return [ChannelOption(**row) for row in await self._session.fetchall()]

    async def claim_languages(self) -> list[LanguageOption]:
        """The languages claims are in, most common first."""
        await self._session.execute(
            """
            SELECT metadata->>'language' AS language, count(*) AS count
            FROM video_claims
            WHERE metadata->>'language' IS NOT NULL AND metadata->>'language' <> ''
            GROUP BY 1
            ORDER BY count(*) DESC, 1
            """
        )
        return [LanguageOption(**row) for row in await self._session.fetchall()]

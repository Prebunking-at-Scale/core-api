from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.analysis import embedding
from core.errors import ConflictError
from core.languages.models import LanguageWithVideoCount
from core.models import Narrative, Video, VideoStats
from core.videos.models import VideoFilters


class VideoRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def get_video_by_id(self, video_id: UUID) -> Video | None:
        await self._session.execute(
            """
            SELECT v.id, v.title, v.description, v.platform, v.source_url,
                   v.destination_path, v.uploaded_at, v.channel, v.scrape_topic,
                   v.scrape_keyword, v.metadata, v.created_at, v.updated_at,
                   vs.views, vs.likes, vs.comments, vs.channel_followers
            FROM videos v
            LEFT JOIN LATERAL (
                SELECT views, likes, comments, channel_followers
                FROM video_stats
                WHERE video_id = v.id
                ORDER BY recorded_at DESC
                LIMIT 1
            ) vs ON true
            WHERE v.id = %(video_id)s
            """,
            {"video_id": video_id},
        )
        row = await self._session.fetchone()
        if not row:
            return None
        return Video(**row)

    async def add_video(self, video: Video) -> Video:
        encoded = embedding.encode(video.title + " " + video.description)
        try:
            await self._session.execute(
                """
                INSERT INTO videos (
                    id,
                    title,
                    description,
                    platform,
                    source_url,
                    destination_path,
                    uploaded_at,
                    channel,
                    scrape_topic,
                    scrape_keyword,
                    metadata,
                    embedding
                )
                VALUES (
                    %(id)s,
                    %(title)s,
                    %(description)s,
                    %(platform)s,
                    %(source_url)s,
                    %(destination_path)s,
                    %(uploaded_at)s,
                    %(channel)s,
                    %(scrape_topic)s,
                    %(scrape_keyword)s,
                    %(metadata)s,
                    %(embedding)s
                )
                RETURNING *
                """,
                {
                    "id": video.id,
                    "title": video.title,
                    "description": video.description,
                    "platform": video.platform,
                    "source_url": video.source_url,
                    "destination_path": video.destination_path,
                    "uploaded_at": video.uploaded_at,
                    "channel": video.channel,
                    "scrape_topic": video.scrape_topic,
                    "scrape_keyword": video.scrape_keyword,
                    "metadata": Jsonb(video.metadata),
                    "embedding": list(encoded),
                },
            )
        except psycopg.errors.UniqueViolation:
            raise ConflictError("video ids must be unique")
        row = await self._session.fetchone()
        if not row:
            raise ValueError("Failed to insert video")

        await self._session.execute(
            """
            INSERT INTO video_stats (video_id, views, likes, comments, channel_followers)
            VALUES (%(video_id)s, %(views)s, %(likes)s, %(comments)s, %(channel_followers)s)
            """,
            {
                "video_id": video.id,
                "views": video.views,
                "likes": video.likes,
                "comments": video.comments,
                "channel_followers": video.channel_followers,
            },
        )

        return Video(
            **row,
            views=video.views,
            likes=video.likes,
            comments=video.comments,
            channel_followers=video.channel_followers,
        )

    async def update_video(self, video: Video) -> Video:
        await self._session.execute(
            """
            UPDATE videos
            SET
                title = %(title)s,
                description = %(description)s,
                metadata = metadata || %(metadata)s
            WHERE id = %(id)s
            RETURNING *
            """,
            {
                "id": video.id,
                "title": video.title,
                "description": video.description,
                "metadata": Jsonb(video.metadata),
            },
        )
        row = await self._session.fetchone()
        if not row:
            raise ValueError(f"Video with ID {video.id} not found")

        await self._session.execute(
            """
            INSERT INTO video_stats (video_id, views, likes, comments, channel_followers)
            VALUES (%(video_id)s, %(views)s, %(likes)s, %(comments)s, %(channel_followers)s)
            """,
            {
                "video_id": video.id,
                "views": video.views,
                "likes": video.likes,
                "comments": video.comments,
                "channel_followers": video.channel_followers,
            },
        )

        return Video(
            **row,
            views=video.views,
            likes=video.likes,
            comments=video.comments,
            channel_followers=video.channel_followers,
        )

    async def delete_video(self, video_id: UUID) -> None:
        await self._session.execute(
            """
            DELETE FROM videos WHERE id = %(video_id)s
            """,
            {"video_id": video_id},
        )
        if self._session.rowcount == 0:
            raise ValueError(f"Video with ID {video_id} not found")

    async def filter_videos(self, filters: VideoFilters) -> list[Video]:
        wheres = [sql.SQL("1=1")]
        additional_params = {}
        if filters.platform:
            wheres.append(sql.SQL("v.platform = ANY(%(platform)s)"))

        if filters.channel:
            wheres.append(sql.SQL("v.channel = ANY(%(channel)s)"))

        if filters.metadata:
            wheres.append(sql.SQL("v.metadata @@ %(metadata)s"))

        if filters.semantic:
            additional_params["encoded"] = list(embedding.encode(filters.semantic))
            wheres.append(sql.SQL("v.embedding <=> %(encoded)s::vector < 0.75"))

        if filters.cursor:
            wheres.append(
                sql.SQL("""
                v.created_at < (
                        SELECT created_at FROM videos WHERE id = %(cursor)s
                )
                """)
            )

        full_query = sql.SQL("""
            SELECT
                v.id,
                v.title,
                v.description,
                v.platform,
                v.source_url,
                v.destination_path,
                v.uploaded_at,
                v.channel,
                v.scrape_topic,
                v.scrape_keyword,
                v.metadata,
                v.updated_at,
                v.created_at,
                vs.views,
                vs.likes,
                vs.comments,
                vs.channel_followers
            FROM videos v
            LEFT JOIN LATERAL (
                SELECT views, likes, comments, channel_followers
                FROM video_stats
                WHERE video_id = v.id
                ORDER BY recorded_at DESC
                LIMIT 1
            ) vs ON true
            WHERE {wheres}
            ORDER BY v.created_at DESC
            LIMIT %(limit)s
            """).format(wheres=sql.Composed(wheres).join(" AND "))

        await self._session.execute(
            full_query, params=filters.model_dump() | additional_params
        )
        return [Video(**row) for row in await self._session.fetchall()]

    async def get_videos_paginated(
        self,
        limit: int,
        offset: int,
        platform: str | None = None,
        channel: str | None = None,
        text: str | None = None,
        language: str | None = None,
    ) -> tuple[list[Video], int]:
        wheres = [sql.SQL("1=1")]
        params: dict[str, Any] = {"limit": limit, "offset": offset}

        if platform:
            wheres.append(sql.SQL("LOWER(platform) LIKE LOWER(%(platform)s)"))
            params["platform"] = f"%{platform}%"

        if channel:
            wheres.append(sql.SQL("LOWER(channel) LIKE LOWER(%(channel)s)"))
            params["channel"] = f"%{channel}%"
            
        if text:
            wheres.append(sql.SQL("(LOWER(title) LIKE LOWER(%(text)s) OR LOWER(description) LIKE LOWER(%(text)s))"))
            params["text"] = f"%{text}%"

        if language:
            wheres.append(sql.SQL("metadata->>'language' = %(language)s"))
            params["language"] = language

        where_clause = sql.Composed(wheres).join(" AND ")

        # Get total count
        count_query = sql.SQL("""
            SELECT COUNT(*) FROM videos
            WHERE {wheres}
        """).format(wheres=where_clause)

        await self._session.execute(count_query, params)
        total = (await self._session.fetchone())["count"]  # type: ignore

        # Get paginated results
        data_query = sql.SQL("""
            SELECT v.id, v.title, v.description, v.platform, v.source_url,
                   v.destination_path, v.uploaded_at, v.channel, v.scrape_topic,
                   v.scrape_keyword, v.metadata, v.created_at, v.updated_at,
                   vs.views, vs.likes, vs.comments, vs.channel_followers
            FROM videos v
            LEFT JOIN LATERAL (
                SELECT views, likes, comments, channel_followers
                FROM video_stats
                WHERE video_id = v.id
                ORDER BY recorded_at DESC
                LIMIT 1
            ) vs ON true
            WHERE {wheres}
            ORDER BY v.created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """).format(wheres=where_clause)

        await self._session.execute(data_query, params)
        videos = [Video(**row) for row in await self._session.fetchall()]

        return videos, total

    async def get_narratives_for_video(self, video_id: UUID) -> list[Narrative]:
        """Get all narratives associated with a video through its claims"""
        await self._session.execute(
            """
            SELECT * FROM narratives n
			WHERE EXISTS (
				SELECT 1 FROM claim_narratives cn
				JOIN video_claims c ON cn.claim_id = c.id
				WHERE c.video_id = %(video_id)s AND cn.narrative_id = n.id
			)
			ORDER BY n.created_at DESC
            """,
            {"video_id": video_id},
        )
        return [Narrative(**row) for row in await self._session.fetchall()]

    async def get_languages_associated_with_videos(self) -> list[LanguageWithVideoCount]:
        await self._session.execute(
            """
            SELECT metadata->>'language' as language, count(*)
            FROM videos
            WHERE metadata ? 'language'
            GROUP BY metadata->>'language'
            ORDER BY count(*) DESC
            """,
        )
        rows = await self._session.fetchall()
        return [LanguageWithVideoCount(**row) for row in rows]

    async def get_video_stats_history(self, video_id: UUID) -> list[VideoStats]:
        """Get all historical stats records for a video, ordered by recorded_at descending"""
        await self._session.execute(
            """
            SELECT video_id, views, likes, comments, channel_followers, recorded_at
            FROM video_stats
            WHERE video_id = %(video_id)s
            ORDER BY recorded_at DESC
            """,
            {"video_id": video_id},
        )
        return [VideoStats(**row) for row in await self._session.fetchall()]

from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.analysis import embedding
from core.errors import ConflictError
from core.videos.models import Video, VideoFilters


class VideoRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def get_video_by_id(self, video_id: UUID) -> Video | None:
        await self._session.execute(
            """
            SELECT *, embedding::real[] FROM videos WHERE id = %(video_id)s
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
                    views,
                    likes,
                    comments,
                    channel,
                    channel_followers,
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
                    %(views)s,
                    %(likes)s,
                    %(comments)s,
                    %(channel)s,
                    %(channel_followers)s,
                    %(scrape_topic)s,
                    %(scrape_keyword)s,
                    %(metadata)s,
                    %(embedding)s
                )
                RETURNING *, embedding::real[]
                """,
                {
                    **video.model_dump(),
                    "metadata": Jsonb(video.metadata),
                    "embedding": list(encoded),
                },
            )
        except psycopg.errors.UniqueViolation:
            raise ConflictError("video ids must be unique")
        row = await self._session.fetchone()
        if not row:
            raise ValueError("Failed to insert video")
        return Video(**row)

    async def update_video(self, video: Video) -> Video:
        await self._session.execute(
            """
            UPDATE videos
            SET
                title = %(title)s,
                description = %(description)s,
                views = %(views)s,
                likes = %(likes)s,
                comments = %(comments)s,
                channel_followers = %(channel_followers)s,
                metadata = metadata || %(metadata)s
            WHERE id = %(id)s
            RETURNING *, embedding::real[]
            """,
            video.model_dump() | {"metadata": Jsonb(video.metadata)},
        )
        row = await self._session.fetchone()
        if not row:
            raise ValueError(f"Video with ID {video.id} not found")
        return Video(**row)

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
                v.views,
                v.likes,
                v.comments,
                v.channel,
                v.channel_followers,
                v.scrape_topic,
                v.scrape_keyword,
                v.embedding::real[],
                v.metadata,
                v.updated_at,
                v.created_at
            FROM videos v
            WHERE {wheres}
            ORDER BY v.created_at DESC
            LIMIT %(limit)s
            """).format(wheres=sql.Composed(wheres).join(" AND "))

        await self._session.execute(
            full_query, params=filters.model_dump() | additional_params
        )
        return [Video(**row) for row in await self._session.fetchall()]

    async def get_videos_paginated(
        self, limit: int, offset: int, platform: list[str] | None = None, channel: list[str] | None = None
    ) -> tuple[list[Video], int]:
        wheres = [sql.SQL("1=1")]
        params = {"limit": limit, "offset": offset}
        
        if platform:
            wheres.append(sql.SQL("platform = ANY(%(platform)s)"))
            params["platform"] = platform
        
        if channel:
            wheres.append(sql.SQL("channel = ANY(%(channel)s)"))
            params["channel"] = channel
        
        where_clause = sql.Composed(wheres).join(" AND ")
        
        # Get total count
        count_query = sql.SQL("""
            SELECT COUNT(*) FROM videos
            WHERE {wheres}
        """).format(wheres=where_clause)
        
        await self._session.execute(count_query, params)
        total = (await self._session.fetchone())["count"]
        
        # Get paginated results
        data_query = sql.SQL("""
            SELECT *, embedding::real[] FROM videos
            WHERE {wheres}
            ORDER BY created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """).format(wheres=where_clause)
        
        await self._session.execute(data_query, params)
        videos = [Video(**row) for row in await self._session.fetchall()]
        
        return videos, total
    
    async def get_narratives_for_video(self, video_id: UUID) -> list[dict]:
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
        return [dict(row) for row in await self._session.fetchall()]

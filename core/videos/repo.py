from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.videos.models import Video


class VideoRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def get_video_by_id(self, video_id: UUID) -> Video | None:
        await self._session.execute(
            """
            SELECT * FROM videos WHERE id = %(video_id)s
            """,
            {"video_id": video_id},
        )
        row = await self._session.fetchone()
        if not row:
            return None
        return Video(**row)

    async def add_video(self, video: Video) -> Video:
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
                metadata
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
                %(metadata)s
            )
            RETURNING *
            """,
            video.model_dump() | {"metadata": Jsonb(video.metadata)},
        )
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
                metadata = %(metadata)s
            WHERE id = %(id)s
            RETURNING *
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

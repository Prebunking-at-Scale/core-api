from typing import Any, AsyncContextManager
from uuid import UUID

from litestar.dto import DTOData

from core.models import Topic
from core.topics.models import TopicWithStats
from core.topics.repo import TopicRepository
from core.uow import ConnectionFactory, uow


class TopicService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[TopicRepository]:
        return uow(TopicRepository, self._connection_factory)

    async def create_topic(self, topic: Topic | DTOData[Topic]) -> Topic:
        if isinstance(topic, DTOData):
            topic = topic.create_instance()

        async with self.repo() as repo:
            return await repo.create_topic(
                topic=topic.topic,
                metadata=topic.metadata,
            )

    async def get_topic(self, topic_id: UUID) -> Topic | None:
        async with self.repo() as repo:
            return await repo.get_topic(topic_id)

    async def get_topic_by_name(self, topic: str) -> Topic | None:
        async with self.repo() as repo:
            return await repo.get_topic_by_name(topic)

    async def get_all_topics(self, limit: int = 100, offset: int = 0) -> list[Topic]:
        async with self.repo() as repo:
            return await repo.get_all_topics(limit=limit, offset=offset)

    async def search_topics(self, query: str) -> list[Topic]:
        async with self.repo() as repo:
            return await repo.search_topics(query)

    async def update_topic(
        self,
        topic_id: UUID,
        data: dict[str, Any] | DTOData[Topic],
    ) -> Topic | None:
        if isinstance(data, DTOData):
            data = data.as_builtins()

        async with self.repo() as repo:
            return await repo.update_topic(
                topic_id=topic_id,
                topic=data.get("topic"),  # type: ignore
                metadata=data.get("metadata"),  # type: ignore
            )

    async def delete_topic(self, topic_id: UUID) -> None:
        async with self.repo() as repo:
            await repo.delete_topic(topic_id)

    async def update_metadata(
        self, topic_id: UUID, metadata: dict[str, Any]
    ) -> dict[str, Any]:
        async with self.repo() as repo:
            updated = await repo.update_topic(
                topic_id=topic_id,
                metadata=metadata,
            )
            if not updated:
                raise ValueError("topic not found")
            return updated.metadata

    async def get_topics_by_narrative(self, narrative_id: UUID) -> list[Topic]:
        async with self.repo() as repo:
            return await repo.get_topics_by_narrative(narrative_id)

    async def get_all_topics_with_stats(
        self, limit: int = 100, offset: int = 0
    ) -> tuple[list[TopicWithStats], int]:
        async with self.repo() as repo:
            return await repo.get_all_topics_with_stats(limit=limit, offset=offset)

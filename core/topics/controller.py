from datetime import datetime
from typing import Any
from uuid import UUID

from litestar import Controller, delete, get, patch, post
from litestar.di import Provide
from litestar.dto import DTOData
from litestar.exceptions import NotFoundException

from core.errors import ConflictError
from core.models import Narrative, Topic
from core.narratives.service import NarrativeService
from core.response import JSON, PaginatedJSON
from core.topics.models import TopicDTO, TopicWithStats
from core.topics.service import TopicService
from core.uow import ConnectionFactory
from core.videos.claims.models import EnrichedClaim
from core.videos.claims.service import ClaimsService


async def topic_service(
    connection_factory: ConnectionFactory,
) -> TopicService:
    return TopicService(connection_factory=connection_factory)


async def narrative_service(
    connection_factory: ConnectionFactory,
) -> NarrativeService:
    return NarrativeService(connection_factory=connection_factory)


async def claims_service(
    connection_factory: ConnectionFactory,
) -> ClaimsService:
    return ClaimsService(connection_factory=connection_factory)


class TopicController(Controller):
    path = "/topics"
    tags = ["topics"]

    dependencies = {
        "topic_service": Provide(topic_service),
        "narrative_service": Provide(narrative_service),
        "claims_service": Provide(claims_service),
    }

    @post(
        path="/",
        summary="Create a new topic",
        dto=TopicDTO,
        return_dto=None,
        raises=[ConflictError],
    )
    async def create_topic(
        self,
        topic_service: TopicService,
        data: DTOData[Topic],
    ) -> JSON[Topic]:
        return JSON(await topic_service.create_topic(data))

    @get(
        path="/{topic_id:uuid}",
        summary="Get a specific topic",
    )
    async def get_topic(
        self, topic_service: TopicService, topic_id: UUID
    ) -> JSON[Topic]:
        topic = await topic_service.get_topic(topic_id)
        if not topic:
            raise NotFoundException()
        return JSON(topic)

    @get(
        path="/name/{topic:str}",
        summary="Get a topic by name",
    )
    async def get_topic_by_name(
        self, topic_service: TopicService, topic: str
    ) -> JSON[Topic]:
        result = await topic_service.get_topic_by_name(topic)
        if not result:
            raise NotFoundException()
        return JSON(result)

    @get(
        path="/",
        summary="Get all topics",
    )
    async def get_topics(
        self,
        topic_service: TopicService,
        limit: int = 100,
        offset: int = 0,
    ) -> JSON[list[Topic]]:
        return JSON(await topic_service.get_all_topics(limit=limit, offset=offset))

    @get(
        path="/search",
        summary="Search topics by query",
    )
    async def search_topics(
        self,
        topic_service: TopicService,
        query: str,
    ) -> JSON[list[Topic]]:
        return JSON(await topic_service.search_topics(query))

    @get(
        path="/stats",
        summary="Get all topics with narrative and claim counts",
    )
    async def get_topics_with_stats(
        self,
        topic_service: TopicService,
        limit: int = 20,
        offset: int = 0,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> PaginatedJSON[list[TopicWithStats]]:
        topics, total = await topic_service.get_all_topics_with_stats(
            limit=limit, offset=offset, start_date=start_date, end_date=end_date
        )
        page = (offset // limit) + 1 if limit > 0 else 1
        return PaginatedJSON(
            data=topics,
            total=total,
            page=page,
            size=limit,
        )

    @get(
        path="/narratives/{narrative_id:uuid}",
        summary="Get topics for a specific narrative",
    )
    async def get_topics_by_narrative(
        self, topic_service: TopicService, narrative_id: UUID
    ) -> JSON[list[Topic]]:
        return JSON(await topic_service.get_topics_by_narrative(narrative_id))

    @patch(
        path="/{topic_id:uuid}",
        summary="Update a topic",
        dto=TopicDTO,
        return_dto=None,
        raises=[ConflictError],
    )
    async def update_topic(
        self,
        topic_service: TopicService,
        topic_id: UUID,
        data: DTOData[Topic],
    ) -> JSON[Topic]:
        topic = await topic_service.update_topic(topic_id, data)
        if not topic:
            raise NotFoundException()
        return JSON(topic)

    @patch(
        path="/{topic_id:uuid}/metadata",
        summary="Update the metadata for a topic",
    )
    async def patch_topic_metadata(
        self,
        topic_service: TopicService,
        topic_id: UUID,
        data: dict[str, Any],
    ) -> JSON[dict[str, Any]]:
        return JSON(await topic_service.update_metadata(topic_id, data))

    @delete(
        path="/{topic_id:uuid}",
        summary="Delete a specific topic",
    )
    async def delete_topic(
        self,
        topic_service: TopicService,
        topic_id: UUID,
    ) -> None:
        await topic_service.delete_topic(topic_id)

    @get(
        path="/{topic_id:uuid}/narratives",
        summary="Get all narratives for a specific topic",
    )
    async def get_narratives_by_topic(
        self,
        narrative_service: NarrativeService,
        topic_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> PaginatedJSON[list[Narrative]]:
        narratives, total = await narrative_service.get_narratives_by_topic(
            topic_id, limit=limit, offset=offset
        )
        page = (offset // limit) + 1 if limit > 0 else 1
        return PaginatedJSON(
            data=narratives,
            total=total,
            page=page,
            size=limit,
        )

    @get(
        path="/{topic_id:uuid}/claims",
        summary="Get all claims for a specific topic",
    )
    async def get_claims_by_topic(
        self,
        claims_service: ClaimsService,
        topic_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> PaginatedJSON[list[EnrichedClaim]]:
        claims, total = await claims_service.get_claims_by_topic(
            topic_id, limit=limit, offset=offset
        )
        page = (offset // limit) + 1 if limit > 0 else 1
        return PaginatedJSON(
            data=claims,
            total=total,
            page=page,
            size=limit,
        )

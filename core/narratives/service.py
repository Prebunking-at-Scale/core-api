from datetime import datetime
from typing import Any, AsyncContextManager
from uuid import UUID

from core.entities.service import EntityService
from core.models import Claim, Narrative, Video
from core.narratives.models import (
    NarrativeDetail,
    NarrativeInput,
    NarrativeListItem,
    NarrativePatchInput,
    NarrativeStats,
    NarrativeSummary,
    ViralNarrativeSummary,
)
from core.narratives.repo import NarrativeRepository
from core.uow import ConnectionFactory, uow


class NarrativeService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[NarrativeRepository]:
        return uow(NarrativeRepository, self._connection_factory)

    async def create_narrative(self, narrative: NarrativeInput) -> Narrative:
        async with self.repo() as repo:
            if not await repo.claims_exist(narrative.claim_ids):
                raise ValueError("one or more claims not found")

            # Process entities first
            entity_ids = []
            if narrative.entities:
                entity_service = EntityService(self._connection_factory)
                entity_ids = await entity_service.process_entities(narrative.entities)

            # First check if a narrative with the same title exists
            existing_narrative = await repo.find_by_title(narrative.title)

            # If no narrative with the same title exists, check for narrative_id in metadata
            if not existing_narrative:
                narrative_id_in_metadata = narrative.metadata.get("narrative_id")
                if narrative_id_in_metadata:
                    existing_narrative = await repo.find_by_narrative_id_in_metadata(
                        narrative_id_in_metadata
                    )

            if existing_narrative:
                # Merge claim_ids and topic_ids with existing ones
                existing_claim_ids = [claim.id for claim in existing_narrative.claims]
                merged_claim_ids = list(set(existing_claim_ids + narrative.claim_ids))

                existing_topic_ids = [topic.id for topic in existing_narrative.topics]
                merged_topic_ids = list(set(existing_topic_ids + narrative.topic_ids))

                # Merge entity_ids with existing ones
                existing_entity_ids = [entity.id for entity in existing_narrative.entities]
                merged_entity_ids = list(set(existing_entity_ids + entity_ids))

                updated_narrative = await repo.update_narrative(
                    narrative_id=existing_narrative.id,
                    title=narrative.title,
                    description=narrative.description,
                    claim_ids=merged_claim_ids,
                    topic_ids=merged_topic_ids,
                    entity_ids=merged_entity_ids,
                    metadata=narrative.metadata,
                )
                if updated_narrative is None:
                    raise ValueError(f"Failed to update narrative with ID {existing_narrative.id}")
                return updated_narrative

            return await repo.create_narrative(
                title=narrative.title,
                description=narrative.description,
                claim_ids=narrative.claim_ids,
                topic_ids=narrative.topic_ids,
                entity_ids=entity_ids,
                metadata=narrative.metadata,
            )

    async def get_narrative(self, narrative_id: UUID) -> Narrative | None:
        async with self.repo() as repo:
            return await repo.get_narrative(narrative_id)

    async def get_narrative_detail(
        self,
        narrative_id: UUID,
        claims_limit: int = 10,
        videos_limit: int = 10,
    ) -> NarrativeDetail | None:
        async with self.repo() as repo:
            return await repo.get_narrative_detail(
                narrative_id, claims_limit=claims_limit, videos_limit=videos_limit
            )

    async def get_narrative_claims(
        self, narrative_id: UUID, limit: int, offset: int
    ) -> tuple[list[Claim], int]:
        async with self.repo() as repo:
            if not await repo.narrative_exists(narrative_id):
                raise ValueError("narrative not found")
            return await repo.get_narrative_claims(narrative_id, limit, offset)

    async def get_narrative_videos(
        self, narrative_id: UUID, limit: int, offset: int
    ) -> tuple[list[Video], int]:
        async with self.repo() as repo:
            if not await repo.narrative_exists(narrative_id):
                raise ValueError("narrative not found")
            return await repo.get_narrative_videos(narrative_id, limit, offset)

    async def get_narrative_stats(self, narrative_id: UUID) -> NarrativeStats | None:
        async with self.repo() as repo:
            return await repo.get_narrative_stats(narrative_id)

    async def get_narratives_by_claim(self, claim_id: UUID) -> list[Narrative]:
        async with self.repo() as repo:
            return await repo.get_narratives_by_claim(claim_id)

    async def get_narratives_by_claim_list(self, claim_id: UUID) -> list[NarrativeListItem]:
        async with self.repo() as repo:
            return await repo.get_narratives_by_claim_list(claim_id)

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
        first_content_end : datetime | None = None,
        language: str | None = None,
    ) -> tuple[list[Narrative], int]:
        async with self.repo() as repo:
            narratives = await repo.get_all_narratives(
                limit=limit,
                offset=offset,
                topic_id=topic_id,
                entity_id=entity_id,
                text=text,
                start_date=start_date,
                end_date=end_date,
                first_content_start=first_content_start,
                first_content_end=first_content_end,
                language=language
            )
            total = await repo.count_all_narratives(
                topic_id=topic_id,
                entity_id=entity_id,
                text=text,
                start_date=start_date,
                end_date=end_date,
                first_content_start=first_content_start,
                first_content_end=first_content_end,
                language=language
            )
            return narratives, total

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
    ) -> tuple[list[NarrativeListItem], int]:
        async with self.repo() as repo:
            narratives = await repo.get_all_narratives_list(
                limit=limit,
                offset=offset,
                topic_id=topic_id,
                entity_id=entity_id,
                text=text,
                start_date=start_date,
                end_date=end_date,
                first_content_start=first_content_start,
                first_content_end=first_content_end,
                language=language
            )
            total = await repo.count_all_narratives(
                topic_id=topic_id,
                entity_id=entity_id,
                text=text,
                start_date=start_date,
                end_date=end_date,
                first_content_start=first_content_start,
                first_content_end=first_content_end,
                language=language
            )
            return narratives, total

    async def get_narratives_by_entity(
        self, entity_id: UUID, limit: int = 100, offset: int = 0
    ) -> tuple[list[Narrative], int]:
        async with self.repo() as repo:
            narratives = await repo.get_all_narratives(
                limit=limit, offset=offset, entity_id=entity_id
            )
            total = await repo.count_all_narratives(entity_id=entity_id)
            return narratives, total

    async def update_narrative(
        self,
        narrative_id: UUID,
        data: NarrativePatchInput,
    ) -> Narrative | None:
        async with self.repo() as repo:

            existing_narrative = await repo.get_narrative(narrative_id)
            if not existing_narrative:
                return None

            merged_entity_ids = None
            if data.entities is not None:
                entity_service = EntityService(self._connection_factory)
                entity_ids = await entity_service.process_entities(data.entities)

                existing_entity_ids = [entity.id for entity in existing_narrative.entities]
                merged_entity_ids = list(set(existing_entity_ids + entity_ids))

            merged_claim_ids = data.claim_ids
            if data.claim_ids is not None:
                existing_claim_ids = [claim.id for claim in existing_narrative.claims]
                merged_claim_ids = list(set(existing_claim_ids + data.claim_ids))

            merged_topic_ids = data.topic_ids
            if data.topic_ids is not None:
                existing_topic_ids = [topic.id for topic in existing_narrative.topics]
                merged_topic_ids = list(set(existing_topic_ids + data.topic_ids))

            return await repo.update_narrative(
                narrative_id=narrative_id,
                title=data.title,
                description=data.description,
                claim_ids=merged_claim_ids,
                topic_ids=merged_topic_ids,
                entity_ids=merged_entity_ids,
                metadata=data.metadata,
            )

    async def delete_narrative(self, narrative_id: UUID) -> None:
        async with self.repo() as repo:
            await repo.delete_narrative(narrative_id)

    async def update_metadata(
        self, narrative_id: UUID, metadata: dict[str, Any]
    ) -> dict[str, Any]:
        async with self.repo() as repo:
            updated = await repo.update_narrative(
                narrative_id=narrative_id,
                metadata=metadata,
            )
            if not updated:
                raise ValueError("narrative not found")
            return updated.metadata

    async def get_narratives_by_topic(
        self, topic_id: UUID, limit: int = 100, offset: int = 0
    ) -> tuple[list[Narrative], int]:
        async with self.repo() as repo:
            return await repo.get_narratives_by_topic(
                topic_id, limit=limit, offset=offset
            )

    async def get_viral_narratives(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[Narrative]:
        async with self.repo() as repo:
            return await repo.get_viral_narratives(
                limit=limit, offset=offset, hours=hours
            )

    async def get_prevalent_narratives(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[Narrative]:
        async with self.repo() as repo:
            return await repo.get_prevalent_narratives(
                limit=limit, offset=offset, hours=hours
            )

    async def get_viral_narratives_summary(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[ViralNarrativeSummary]:
        async with self.repo() as repo:
            return await repo.get_viral_narratives_summary(
                limit=limit, offset=offset, hours=hours
            )

    async def get_prevalent_narratives_summary(
        self, limit: int = 100, offset: int = 0, hours: int | None = None
    ) -> list[NarrativeSummary]:
        async with self.repo() as repo:
            return await repo.get_prevalent_narratives_summary(
                limit=limit, offset=offset, hours=hours
            )

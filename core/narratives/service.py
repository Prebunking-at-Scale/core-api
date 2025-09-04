from typing import Any, AsyncContextManager
from uuid import UUID

from core.models import Narrative
from core.narratives.models import NarrativeInput, NarrativeUpdate
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
                    
                    updated_narrative = await repo.update_narrative(
                        narrative_id=existing_narrative.id,
                        title=narrative.title,
                        description=narrative.description,
                        claim_ids=merged_claim_ids,
                        topic_ids=merged_topic_ids,
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
                metadata=narrative.metadata,
            )

    async def get_narrative(self, narrative_id: UUID) -> Narrative | None:
        async with self.repo() as repo:
            return await repo.get_narrative(narrative_id)

    async def get_narratives_by_claim(self, claim_id: UUID) -> list[Narrative]:
        async with self.repo() as repo:
            return await repo.get_narratives_by_claim(claim_id)

    async def get_all_narratives(
        self, 
        limit: int = 100, 
        offset: int = 0,
        topic_id: UUID | None = None,
        text: str | None = None
    ) -> tuple[list[Narrative], int]:
        async with self.repo() as repo:
            narratives = await repo.get_all_narratives(
                limit=limit, 
                offset=offset, 
                topic_id=topic_id, 
                text=text
            )
            total = await repo.count_all_narratives(topic_id=topic_id, text=text)
            return narratives, total

    async def update_narrative(
        self,
        narrative_id: UUID,
        data: NarrativeUpdate,
    ) -> Narrative | None:
        async with self.repo() as repo:
            # Get existing narrative first to preserve unchanged fields
            existing = await repo.get_narrative(narrative_id)
            if not existing:
                return None
            
            # Build update params with only provided fields
            update_params = {"narrative_id": narrative_id}
            if data.title is not None:
                update_params["title"] = data.title
            if data.description is not None:
                update_params["description"] = data.description
            if data.claim_ids is not None:
                update_params["claim_ids"] = data.claim_ids
            if data.topic_ids is not None:
                update_params["topic_ids"] = data.topic_ids
            if data.metadata is not None:
                update_params["metadata"] = data.metadata
            
            return await repo.update_narrative(**update_params)

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
        self, limit: int = 100, offset: int = 0, hours: int = 24
    ) -> list[Narrative]:
        async with self.repo() as repo:
            return await repo.get_viral_narratives(
                limit=limit, offset=offset, hours=hours
            )

    async def get_prevalent_narratives(
        self, limit: int = 100, offset: int = 0, hours: int = 24
    ) -> list[Narrative]:
        async with self.repo() as repo:
            return await repo.get_prevalent_narratives(
                limit=limit, offset=offset, hours=hours
            )

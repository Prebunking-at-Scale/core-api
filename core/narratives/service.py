from typing import Any, AsyncContextManager
from uuid import UUID

from core.models import Narrative
from core.narratives.models import NarrativeInput
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
        self, limit: int = 100, offset: int = 0
    ) -> tuple[list[Narrative], int]:
        async with self.repo() as repo:
            narratives = await repo.get_all_narratives(limit=limit, offset=offset)
            total = await repo.count_all_narratives()
            return narratives, total

    async def update_narrative(
        self,
        narrative_id: UUID,
        data: NarrativeInput,
    ) -> Narrative | None:
        async with self.repo() as repo:
            return await repo.update_narrative(
                narrative_id=narrative_id,
                title=data.title,
                description=data.description,
                claim_ids=data.claim_ids,
                topic_ids=data.topic_ids,
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

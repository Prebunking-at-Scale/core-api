from typing import Any, AsyncContextManager
from uuid import UUID

from litestar.dto import DTOData

from core.entities.models import EntityInput
from core.entities.service import EntityService
from core.uow import ConnectionFactory, uow
from core.videos.claims.models import EnrichedClaim, VideoClaims
from core.videos.claims.repo import ClaimRepository


class ClaimsService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[ClaimRepository]:
        return uow(ClaimRepository, self._connection_factory)

    async def add_claims(
        self, video_id: UUID, claims: VideoClaims | DTOData[VideoClaims]
    ) -> VideoClaims:
        if isinstance(claims, DTOData):
            claims = claims.create_instance(video_id=video_id)

        claim_entities = []
        claims_to_add = []

        for claim in claims.claims:
            entities_for_claim = claim.entities if claim.entities else []
            claim_entities.append(entities_for_claim)

            claim_copy = claim.model_copy()
            claim_copy.entities = []
            claims_to_add.append(claim_copy)

        async with self.repo() as repo:
            added_claims = await repo.add_claims(video_id, claims_to_add)

        if any(claim_entities):

            entity_service = EntityService(self._connection_factory)

            for added_claim, entities in zip(added_claims, claim_entities):
                if entities:
                    entity_inputs = [
                        EntityInput(
                            wikidata_id=e.wikidata_id,
                            entity_name=e.name,
                            entity_type=e.metadata.get("entity_type", "") if e.metadata else "",
                            wikidata_info=e.metadata.get("wikidata_info", {}) if e.metadata else {}
                        )
                        for e in entities
                    ]

                    associated_entities = await entity_service.associate_entities_with_claim(added_claim.id, entity_inputs)

                    added_claim.entities = associated_entities

        return VideoClaims(video_id=video_id, claims=added_claims)

    async def get_claims_for_video(self, video_id: UUID) -> VideoClaims | None:
        async with self.repo() as repo:
            if not await repo.video_exists(video_id):
                return None
            claims = await repo.get_claims_for_video(video_id)

        entity_service = EntityService(self._connection_factory)

        for claim in claims:
            async with entity_service.repo() as entity_repo:
                claim.entities = await entity_repo.get_entities_for_claim(claim.id)

        return VideoClaims(video_id=video_id, claims=claims)

    async def delete_video_claims(self, video_id: UUID) -> None:
        async with self.repo() as repo:
            await repo.delete_video_claims(video_id)

    async def update_metadata(
        self, claim_id: UUID, metadata: dict[str, Any]
    ) -> dict[str, Any]:
        async with self.repo() as repo:
            return await repo.update_claim_metadata(claim_id, metadata)

    async def delete_claim(self, claim_id: UUID) -> None:
        async with self.repo() as repo:
            return await repo.delete_claim(claim_id)

    async def get_claims_by_topic(
        self, topic_id: UUID, limit: int = 100, offset: int = 0
    ) -> tuple[list[EnrichedClaim], int]:
        async with self.repo() as repo:
            return await repo.get_claims_by_topic(topic_id, limit=limit, offset=offset)

    async def get_claims_by_entity(
        self, entity_id: UUID, limit: int = 100, offset: int = 0
    ) -> tuple[list[EnrichedClaim], int]:
        async with self.repo() as repo:
            return await repo.get_claims_by_entity(entity_id, limit=limit, offset=offset)

    async def get_all_claims(
        self, 
        limit: int = 100, 
        offset: int = 0, 
        topic_id: UUID | None = None,
        text: str | None = None,
        language: str | None = None,
        min_score: float | None = None,
        max_score: float | None = None
    ) -> tuple[list[EnrichedClaim], int]:
        async with self.repo() as repo:
            return await repo.get_all_claims(
                limit=limit, 
                offset=offset, 
                topic_id=topic_id, 
                text=text,
                language=language,
                min_score=min_score,
                max_score=max_score
            )

    async def associate_topics_with_claim(
        self, claim_id: UUID, topic_ids: list[UUID]
    ) -> None:
        async with self.repo() as repo:
            await repo.associate_topics_with_claim(claim_id, topic_ids)

    async def update_claim_associations(
        self,
        claim_id: UUID,
        topic_ids: list[UUID] | None = None,
        entities: list[EntityInput] | None = None,
    ) -> EnrichedClaim:
        async with self.repo() as repo:

            claim = await repo.get_claim_by_id(claim_id)
            if not claim:
                raise ValueError(f"Claim with ID {claim_id} not found")

            if topic_ids is not None:
                await repo.associate_topics_with_claim(claim_id, topic_ids)

            if entities is not None:
                entity_service = EntityService(self._connection_factory)
                await entity_service.associate_entities_with_claim(claim_id, entities)

            claim = await repo.get_claim_by_id(claim_id)
            if not claim:
                raise ValueError(f"Claim with ID {claim_id} not found")
            return claim

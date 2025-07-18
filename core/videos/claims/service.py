from typing import Any, AsyncContextManager
from uuid import UUID

from litestar.dto import DTOData

from core.uow import ConnectionFactory, uow
from core.videos.claims.models import Claim, VideoClaims
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
        async with self.repo() as repo:
            added_claims = await repo.add_claims(video_id, claims.claims)
        return VideoClaims(video_id=video_id, claims=added_claims)

    async def get_claims_for_video(self, video_id: UUID) -> VideoClaims | None:
        async with self.repo() as repo:
            if not await repo.video_exists(video_id):
                return None
            claims = await repo.get_claims_for_video(video_id)
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
    ) -> tuple[list[Claim], int]:
        async with self.repo() as repo:
            return await repo.get_claims_by_topic(topic_id, limit=limit, offset=offset)
    
    async def get_all_claims(
        self, limit: int = 100, offset: int = 0, topic_id: UUID | None = None
    ) -> tuple[list[Claim], int]:
        async with self.repo() as repo:
            return await repo.get_all_claims(limit=limit, offset=offset, topic_id=topic_id)
    
    async def associate_topics_with_claim(
        self, claim_id: UUID, topic_ids: list[UUID]
    ) -> None:
        async with self.repo() as repo:
            await repo.associate_topics_with_claim(claim_id, topic_ids)
    
    async def update_claim_associations(
        self, claim_id: UUID, topic_ids: list[UUID], entity_ids: list[UUID] | None = None
    ) -> Claim:
        async with self.repo() as repo:
            # Check if claim exists
            claim = await repo.get_claim_by_id(claim_id)
            if not claim:
                raise ValueError(f"Claim with ID {claim_id} not found")
            
            # Update topic associations
            await repo.associate_topics_with_claim(claim_id, topic_ids)
            
            # TODO: When entities are implemented, update entity associations here
            # if entity_ids is not None:
            #     await repo.associate_entities_with_claim(claim_id, entity_ids)
            
            # Return updated claim with new associations
            return await repo.get_claim_by_id(claim_id)

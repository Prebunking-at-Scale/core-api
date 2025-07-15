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

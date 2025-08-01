from typing import Any
from uuid import UUID

from litestar import Controller, delete, get, patch, post
from litestar.di import Provide
from litestar.dto import DTOData
from litestar.exceptions import NotFoundException
from litestar.params import Parameter

from core.errors import ConflictError
from core.models import Claim
from core.response import JSON, PaginatedJSON
from core.uow import ConnectionFactory
from core.videos.claims.models import (
    ClaimUpdate,
    EnrichedClaim,
    VideoClaims,
    VideoClaimsDTO,
)
from core.videos.claims.service import ClaimsService


async def claims_service(
    connection_factory: ConnectionFactory,
) -> ClaimsService:
    return ClaimsService(connection_factory=connection_factory)


class ClaimController(Controller):
    path = "/videos/{video_id:uuid}/claims"
    tags = ["claims"]

    dependencies = {
        "claims_service": Provide(claims_service),
    }

    @post(
        path="/",
        summary="Add new claims for a video",
        dto=VideoClaimsDTO,
        return_dto=None,
        raises=[ConflictError],
    )
    async def add_claims(
        self,
        claims_service: ClaimsService,
        video_id: UUID,
        data: DTOData[VideoClaims],
    ) -> JSON[VideoClaims]:
        return JSON(await claims_service.add_claims(video_id, data))

    @get(
        path="/",
        summary="Get all claims for the given video",
    )
    async def get_claims(
        self, claims_service: ClaimsService, video_id: UUID
    ) -> JSON[VideoClaims]:
        claims = await claims_service.get_claims_for_video(video_id)
        if not claims:
            raise NotFoundException()
        return JSON(claims)

    @delete(
        path="/",
        summary="Delete all claims for the given video",
    )
    async def delete_claims(
        self, claims_service: ClaimsService, video_id: UUID
    ) -> None:
        await claims_service.delete_video_claims(video_id)

    @patch(
        path="/{claim_id:uuid}/metadata",
        summary="Update the metadata for a claim",
    )
    async def patch_claim_metadata(
        self,
        claims_service: ClaimsService,
        claim_id: UUID,
        data: dict[str, Any],
    ) -> JSON[dict[str, Any]]:
        return JSON(await claims_service.update_metadata(claim_id, data))

    @delete(
        path="/{claim_id:uuid}",
        summary="Delete a specific claim",
    )
    async def delete_claim(
        self,
        claims_service: ClaimsService,
        claim_id: UUID,
    ) -> None:
        await claims_service.delete_claim(claim_id)

    @patch(
        path="/{claim_id:uuid}",
        summary="Update claim associations (topics and entities)",
    )
    async def update_claim_associations(
        self,
        claims_service: ClaimsService,
        video_id: UUID,
        claim_id: UUID,
        data: ClaimUpdate,
    ) -> JSON[Claim]:
        try:
            updated_claim = await claims_service.update_claim_associations(
                claim_id=claim_id,
                topic_ids=data.topics,
                entity_ids=data.entities if data.entities else None,
            )
            return JSON(updated_claim)
        except ValueError:
            raise NotFoundException()


class RootClaimController(Controller):
    path = "/claims"
    tags = ["claims"]

    dependencies = {
        "claims_service": Provide(claims_service),
    }

    @get(
        path="/",
        summary="Get all claims with optional topic filter",
    )
    async def get_all_claims(
        self,
        claims_service: ClaimsService,
        topic_id: UUID | None = Parameter(None, query="topic_id"),
        limit: int = Parameter(100, query="limit", gt=0, le=1000),
        offset: int = Parameter(0, query="offset", ge=0),
    ) -> PaginatedJSON[list[EnrichedClaim]]:
        claims, total = await claims_service.get_all_claims(
            limit=limit, offset=offset, topic_id=topic_id
        )
        page = (offset // limit) + 1 if limit > 0 else 1
        return PaginatedJSON(
            data=claims,
            total=total,
            page=page,
            size=limit,
        )

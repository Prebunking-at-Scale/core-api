from typing import Any
from uuid import UUID

from litestar import Controller, delete, get, patch, post
from litestar.di import Provide
from litestar.dto import DTOData
from litestar.exceptions import NotFoundException

from core.errors import ConflictError
from core.response import JSON
from core.uow import ConnectionFactory
from core.videos.claims.models import (
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

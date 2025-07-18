from typing import Any
from uuid import UUID

from litestar import Controller, delete, get, patch, post
from litestar.di import Provide
from litestar.dto import DTOData
from litestar.exceptions import NotFoundException

from core.errors import ConflictError
from core.response import JSON
from core.uow import ConnectionFactory
from core.narratives.models import Narrative, NarrativeInput, NarrativeDTO
from core.narratives.service import NarrativeService


async def narrative_service(
    connection_factory: ConnectionFactory,
) -> NarrativeService:
    return NarrativeService(connection_factory=connection_factory)


class NarrativeController(Controller):
    path = "/narratives"
    tags = ["narratives"]

    dependencies = {
        "narrative_service": Provide(narrative_service),
    }

    @post(
        path="/",
        summary="Create a new narrative",
        dto=NarrativeDTO,
        return_dto=None,
        raises=[ConflictError],
    )
    async def create_narrative(
        self,
        narrative_service: NarrativeService,
        data: DTOData[NarrativeInput],
    ) -> JSON[Narrative]:
        return JSON(await narrative_service.create_narrative(data))

    @get(
        path="/{narrative_id:uuid}",
        summary="Get a specific narrative",
    )
    async def get_narrative(
        self, narrative_service: NarrativeService, narrative_id: UUID
    ) -> JSON[Narrative]:
        narrative = await narrative_service.get_narrative(narrative_id)
        if not narrative:
            raise NotFoundException()
        return JSON(narrative)

    @get(
        path="/",
        summary="Get all narratives",
    )
    async def get_narratives(
        self,
        narrative_service: NarrativeService,
        limit: int = 100,
        offset: int = 0,
    ) -> JSON[list[Narrative]]:
        return JSON(
            await narrative_service.get_all_narratives(limit=limit, offset=offset)
        )

    @get(
        path="/claims/{claim_id:uuid}",
        summary="Get all narratives for a specific claim",
    )
    async def get_narratives_by_claim(
        self, narrative_service: NarrativeService, claim_id: UUID
    ) -> JSON[list[Narrative]]:
        return JSON(await narrative_service.get_narratives_by_claim(claim_id))

    @patch(
        path="/{narrative_id:uuid}",
        summary="Update a narrative",
        dto=NarrativeDTO,
        return_dto=None,
    )
    async def update_narrative(
        self,
        narrative_service: NarrativeService,
        narrative_id: UUID,
        data: DTOData[NarrativeInput],
    ) -> JSON[Narrative]:
        narrative = await narrative_service.update_narrative(narrative_id, data)
        if not narrative:
            raise NotFoundException()
        return JSON(narrative)

    @patch(
        path="/{narrative_id:uuid}/metadata",
        summary="Update the metadata for a narrative",
    )
    async def patch_narrative_metadata(
        self,
        narrative_service: NarrativeService,
        narrative_id: UUID,
        data: dict[str, Any],
    ) -> JSON[dict[str, Any]]:
        return JSON(await narrative_service.update_metadata(narrative_id, data))

    @delete(
        path="/{narrative_id:uuid}",
        summary="Delete a specific narrative",
    )
    async def delete_narrative(
        self,
        narrative_service: NarrativeService,
        narrative_id: UUID,
    ) -> None:
        await narrative_service.delete_narrative(narrative_id)
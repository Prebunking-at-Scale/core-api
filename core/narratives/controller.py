from datetime import datetime
from typing import Any
from uuid import UUID

from litestar import Controller, delete, get, patch, post
from litestar.di import Provide
from litestar.exceptions import NotFoundException
from litestar.params import Parameter

from core.auth.guards import super_admin
from core.errors import ConflictError
from core.models import Narrative
from core.narratives.models import NarrativeInput, NarrativePatchInput
from core.narratives.service import NarrativeService
from core.response import JSON, PaginatedJSON
from core.uow import ConnectionFactory


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
        return_dto=None,
        raises=[ConflictError],
        guards=[super_admin],
    )
    async def create_narrative(
        self,
        narrative_service: NarrativeService,
        data: NarrativeInput,
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
        topic_id: UUID | None = None,
        entity_id: UUID | None = None,
        text: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        first_content_start: datetime | None = None,
        first_content_end: datetime | None = None,
        language: str | None = None,
    ) -> PaginatedJSON[list[Narrative]]:
        narratives, total = await narrative_service.get_all_narratives(
            limit=limit,
            offset=offset,
            topic_id=topic_id,
            entity_id=entity_id,
            text=text,
            start_date=start_date,
            end_date=end_date,
            first_content_start=first_content_start,
            first_content_end=first_content_end,
            language=language,
        )
        page = (offset // limit) + 1 if limit > 0 else 1
        return PaginatedJSON(
            data=narratives, total=total, page=page, size=len(narratives)
        )

    @get(
        path="/claims/{claim_id:uuid}",
        summary="Get all narratives for a specific claim",
    )
    async def get_narratives_by_claim(
        self, narrative_service: NarrativeService, claim_id: UUID
    ) -> JSON[list[Narrative]]:
        return JSON(await narrative_service.get_narratives_by_claim(claim_id))

    @get(
        path="/viral",
        summary="Get viral narratives from a specified time period sorted by views",
    )
    async def get_viral_narratives(
        self,
        narrative_service: NarrativeService,
        limit: int = 100,
        offset: int = 0,
        hours: int | None = None,
    ) -> JSON[list[Narrative]]:
        return JSON(
            await narrative_service.get_viral_narratives(
                limit=limit, offset=offset, hours=hours
            )
        )

    @get(
        path="/prevalent",
        summary="Get prevalent narratives sorted by video count in a specified time period",
    )
    async def get_prevalent_narratives(
        self,
        narrative_service: NarrativeService,
        limit: int = 100,
        offset: int = 0,
        hours: int | None = None,
    ) -> JSON[list[Narrative]]:
        return JSON(
            await narrative_service.get_prevalent_narratives(
                limit=limit, offset=offset, hours=hours
            )
        )

    @patch(
        path="/{narrative_id:uuid}",
        summary="Update a narrative",
        return_dto=None,
        guards=[super_admin],
    )
    async def update_narrative(
        self,
        narrative_service: NarrativeService,
        narrative_id: UUID,
        data: NarrativePatchInput,
    ) -> JSON[Narrative]:
        narrative = await narrative_service.update_narrative(narrative_id, data)
        if not narrative:
            raise NotFoundException()
        return JSON(narrative)

    @patch(
        path="/{narrative_id:uuid}/metadata",
        summary="Update the metadata for a narrative",
        guards=[super_admin],
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
        guards=[super_admin],
    )
    async def delete_narrative(
        self,
        narrative_service: NarrativeService,
        narrative_id: UUID,
    ) -> None:
        await narrative_service.delete_narrative(narrative_id)

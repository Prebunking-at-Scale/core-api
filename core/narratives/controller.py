from datetime import datetime
from typing import Any
from uuid import UUID

from litestar import Controller, Response, delete, get, patch, post
from litestar.di import Provide
from litestar.exceptions import NotFoundException, InternalServerException

from core.auth.guards import super_admin
from core.errors import ConflictError
from core.models import Claim, Narrative, Video
from core.narratives.models import (
    NarrativeDetail,
    NarrativeInput,
    NarrativePatchInput,
    NarrativeStats,
    NarrativeSummary,
    ViralNarrativeSummary,
)
from core.narratives.service import NarrativeService
from core.response import JSON, PaginatedJSON
from core.services.narratives_api import (
    delete_narrative_from_external_api,
    NarrativeNotFoundError,
    AuthenticationError,
    NarrativesAPIError,
)
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
        summary="Get a specific narrative with preview of claims/videos and counts",
    )
    async def get_narrative(
        self,
        narrative_service: NarrativeService,
        narrative_id: UUID,
        claims_limit: int = 10,
        videos_limit: int = 10,
    ) -> JSON[NarrativeDetail]:
        narrative = await narrative_service.get_narrative_detail(
            narrative_id, claims_limit=claims_limit, videos_limit=videos_limit
        )
        if not narrative:
            raise NotFoundException()
        return JSON(narrative)

    @get(
        path="/{narrative_id:uuid}/claims",
        summary="Get paginated claims for a narrative",
    )
    async def get_narrative_claims(
        self,
        narrative_service: NarrativeService,
        narrative_id: UUID,
        limit: int = 25,
        offset: int = 0,
    ) -> PaginatedJSON[list[Claim]]:
        try:
            claims, total = await narrative_service.get_narrative_claims(
                narrative_id, limit=limit, offset=offset
            )
        except ValueError:
            raise NotFoundException()
        page = (offset // limit) + 1 if limit > 0 else 1
        return PaginatedJSON(data=claims, total=total, page=page, size=len(claims))

    @get(
        path="/{narrative_id:uuid}/videos",
        summary="Get paginated videos for a narrative",
    )
    async def get_narrative_videos(
        self,
        narrative_service: NarrativeService,
        narrative_id: UUID,
        limit: int = 25,
        offset: int = 0,
    ) -> PaginatedJSON[list[Video]]:
        try:
            videos, total = await narrative_service.get_narrative_videos(
                narrative_id, limit=limit, offset=offset
            )
        except ValueError:
            raise NotFoundException()
        page = (offset // limit) + 1 if limit > 0 else 1
        return PaginatedJSON(data=videos, total=total, page=page, size=len(videos))

    @get(
        path="/{narrative_id:uuid}/stats",
        summary="Get time-series stats for a narrative (for evolution charts)",
    )
    async def get_narrative_stats(
        self,
        narrative_service: NarrativeService,
        narrative_id: UUID,
    ) -> JSON[NarrativeStats]:
        stats = await narrative_service.get_narrative_stats(narrative_id)
        if not stats:
            raise NotFoundException()
        return JSON(stats)

    @get(
        path="/",
        summary="Get all narratives with counts",
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
    ) -> PaginatedJSON[list[NarrativeSummary]]:
        narratives, total = await narrative_service.get_all_narratives_list(
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
        summary="Get all narratives for a specific claim with counts",
    )
    async def get_narratives_by_claim(
        self, narrative_service: NarrativeService, claim_id: UUID
    ) -> JSON[list[NarrativeSummary]]:
        return JSON(await narrative_service.get_narratives_by_claim_list(claim_id))

    @get(
        path="/viral",
        summary="Get viral narratives with counts sorted by views",
    )
    async def get_viral_narratives(
        self,
        narrative_service: NarrativeService,
        limit: int = 100,
        offset: int = 0,
        hours: int | None = None,
    ) -> JSON[list[NarrativeSummary]]:
        return JSON(
            await narrative_service.get_viral_narratives_summary(
                limit=limit, offset=offset, hours=hours
            )
        )

    @get(
        path="/viral/summary",
        summary="Get viral narrative summaries optimized for dashboard display",
    )
    async def get_viral_narratives_summary(
        self,
        narrative_service: NarrativeService,
        limit: int = 100,
        offset: int = 0,
        hours: int | None = None,
    ) -> JSON[list[ViralNarrativeSummary]]:
        return JSON(
            await narrative_service.get_viral_narratives_summary(
                limit=limit, offset=offset, hours=hours
            )
        )

    @get(
        path="/prevalent",
        summary="Get prevalent narratives with counts sorted by video count",
    )
    async def get_prevalent_narratives(
        self,
        narrative_service: NarrativeService,
        limit: int = 100,
        offset: int = 0,
        hours: int | None = None,
    ) -> JSON[list[NarrativeSummary]]:
        return JSON(
            await narrative_service.get_prevalent_narratives_summary(
                limit=limit, offset=offset, hours=hours
            )
        )

    @get(
        path="/prevalent/summary",
        summary="Get prevalent narrative summaries optimized for dashboard display",
    )
    async def get_prevalent_narratives_summary(
        self,
        narrative_service: NarrativeService,
        limit: int = 100,
        offset: int = 0,
        hours: int | None = None,
    ) -> JSON[list[NarrativeSummary]]:
        return JSON(
            await narrative_service.get_prevalent_narratives_summary(
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
    ) -> Response[None]:
        # First, get the narrative to validate it exists and retrieve metadata
        narrative = await narrative_service.get_narrative(narrative_id)
        if not narrative:
            raise NotFoundException(f"Narrative with ID {narrative_id} not found")

        # Check if there's a backend narrative_id in metadata
        backend_narrative_id = narrative.metadata.get("narrative_id") if narrative.metadata else None

        # If there's a backend ID, try to delete from external API
        if backend_narrative_id:
            try:
                await delete_narrative_from_external_api(backend_narrative_id)
            except NarrativeNotFoundError:
                # The narrative doesn't exist in external API, continue with local deletion
                pass
            except (AuthenticationError, NarrativesAPIError) as e:
                print(f"Warning: Failed to delete from external API: {e}")
                raise InternalServerException(detail=str(e))

        # Delete from local database
        await narrative_service.delete_narrative(narrative_id)
        return Response(content=None, status_code=204)

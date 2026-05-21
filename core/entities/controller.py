from uuid import UUID

import httpx
from litestar import Controller, get
from litestar.di import Provide
from litestar.exceptions import (
    InternalServerException,
    NotFoundException,
    ServiceUnavailableException,
    ValidationException,
)

from core.entities.graph_models import EntityGraph, EntitySearchResult
from core.entities.models import EnrichedEntity
from core.entities.service import EntityService
from core.models import Entity, Narrative
from core.narratives.api import NarrativesApiClient
from core.narratives.service import NarrativeService
from core.response import JSON, PaginatedJSON
from core.uow import ConnectionFactory
from core.videos.claims.models import EnrichedClaim
from core.videos.claims.service import ClaimsService


def _raise_for_narratives(response: httpx.Response) -> None:
    """Map a non-2xx narratives response onto the matching Litestar error.

    The narratives /graph/* endpoints return 404 for an unknown entity and
    503 when Neo4j is not reachable; anything else is treated as an upstream
    failure.
    """
    if response.is_success:
        return
    try:
        detail = response.json().get("detail")
    except Exception:
        detail = None
    if not isinstance(detail, str):
        detail = f"narratives returned {response.status_code}"
    if response.status_code == 404:
        raise NotFoundException(detail=detail)
    if response.status_code == 503:
        raise ServiceUnavailableException(detail=detail)
    raise InternalServerException(detail=f"narratives graph API error: {detail}")


async def entity_service(
    connection_factory: ConnectionFactory,
) -> EntityService:
    return EntityService(connection_factory=connection_factory)


async def narrative_service(
    connection_factory: ConnectionFactory,
) -> NarrativeService:
    return NarrativeService(connection_factory=connection_factory)


async def claims_service(
    connection_factory: ConnectionFactory,
) -> ClaimsService:
    return ClaimsService(connection_factory=connection_factory)


class EntityController(Controller):
    path = "/entities"
    tags = ["entities"]

    dependencies = {
        "entity_service": Provide(entity_service),
        "narrative_service": Provide(narrative_service),
        "claims_service": Provide(claims_service),
    }

    @get(
        path="/",
        summary="Get all entities",
    )
    async def get_entities(
        self,
        entity_service: EntityService,
        limit: int = 100,
        offset: int = 0,
        text: str | None = None,
        hours: int | None = None,
        language: str | None = None,
        narratives_min: int | None = None,
        narratives_max: int | None = None,
    ) -> PaginatedJSON[list[EnrichedEntity]]:
        if narratives_min is not None and narratives_min < 0:
            raise ValidationException("narratives_min must be >= 0")
        if narratives_max is not None and narratives_max < 0:
            raise ValidationException("narratives_max must be >= 0")
        if (
            narratives_min is not None
            and narratives_max is not None
            and narratives_min > narratives_max
        ):
            raise ValidationException("narratives_min must be <= narratives_max")

        entities, total = await entity_service.get_all_entities(
            limit=limit,
            offset=offset,
            text=text,
            hours=hours,
            language=language,
            narratives_min=narratives_min,
            narratives_max=narratives_max,
        )
        page = (offset // limit) + 1 if limit > 0 else 1
        return PaginatedJSON(
            data=entities, total=total, page=page, size=len(entities)
        )

    @get(
        path="/{entity_id:uuid}",
        summary="Get a specific entity",
    )
    async def get_entity(
        self, entity_service: EntityService, entity_id: UUID
    ) -> JSON[Entity]:
        entity = await entity_service.get_entity(entity_id)
        if not entity:
            raise NotFoundException()
        return JSON(entity)

    @get(
        path="/{entity_id:uuid}/narratives",
        summary="Get all narratives for a specific entity",
    )
    async def get_narratives_by_entity(
        self,
        narrative_service: NarrativeService,
        entity_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> PaginatedJSON[list[Narrative]]:
        narratives, total = await narrative_service.get_narratives_by_entity(
            entity_id, limit=limit, offset=offset
        )
        page = (offset // limit) + 1 if limit > 0 else 1
        return PaginatedJSON(
            data=narratives, total=total, page=page, size=len(narratives)
        )

    @get(
        path="/{entity_id:uuid}/claims",
        summary="Get all claims for a specific entity",
    )
    async def get_claims_by_entity(
        self,
        claims_service: ClaimsService,
        entity_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> PaginatedJSON[list[EnrichedClaim]]:
        claims, total = await claims_service.get_claims_by_entity(
            entity_id, limit=limit, offset=offset
        )
        page = (offset // limit) + 1 if limit > 0 else 1
        return PaginatedJSON(
            data=claims, total=total, page=page, size=len(claims)
        )

    # ── Knowledge-graph proxy ───────────────────────────────────────────
    # These proxy the prebunking-narratives /graph/* endpoints, which query
    # the Neo4j entity graph. The frontend never reaches narratives directly,
    # so the graph explorer goes frontend -> backend -> narratives.

    @get(
        path="/graph",
        summary="Entity knowledge-graph ego-network",
    )
    async def get_entity_graph(
        self, entity: str, depth: int = 1
    ) -> JSON[EntityGraph]:
        if not entity.strip():
            raise ValidationException("entity query parameter is required")
        if not NarrativesApiClient.is_configured():
            raise ServiceUnavailableException(
                detail="narratives service is not configured"
            )
        try:
            response = await NarrativesApiClient().get_entity_graph(
                entity=entity.strip(), depth=depth
            )
        except httpx.HTTPError as exc:
            raise ServiceUnavailableException(
                detail=f"narratives service unreachable: {exc}"
            )
        _raise_for_narratives(response)
        return JSON(EntityGraph.model_validate(response.json()))

    @get(
        path="/graph/search",
        summary="Search entities in the knowledge graph",
    )
    async def search_graph_entities(
        self, q: str = "", limit: int = 20
    ) -> JSON[list[EntitySearchResult]]:
        if not NarrativesApiClient.is_configured():
            raise ServiceUnavailableException(
                detail="narratives service is not configured"
            )
        try:
            response = await NarrativesApiClient().search_graph_entities(
                q=q, limit=limit
            )
        except httpx.HTTPError as exc:
            raise ServiceUnavailableException(
                detail=f"narratives service unreachable: {exc}"
            )
        _raise_for_narratives(response)
        results = [
            EntitySearchResult.model_validate(item) for item in response.json()
        ]
        return JSON(results)
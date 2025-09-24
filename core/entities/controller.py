from uuid import UUID

from litestar import Controller, get
from litestar.di import Provide
from litestar.exceptions import NotFoundException

from core.entities.service import EntityService
from core.models import Entity, Narrative
from core.narratives.service import NarrativeService
from core.response import JSON, PaginatedJSON
from core.uow import ConnectionFactory
from core.videos.claims.models import EnrichedClaim
from core.videos.claims.service import ClaimsService


async def entity_service(
    connection_factory: ConnectionFactory,
) -> EntityService:
    return EntityService(connection_factory=connection_factory)


class EntityController(Controller):
    path = "/entities"
    tags = ["entities"]

    dependencies = {
        "entity_service": Provide(entity_service),
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
    ) -> PaginatedJSON[list[Entity]]:
        entities, total = await entity_service.get_all_entities(
            limit=limit, offset=offset, text=text
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
        entity_service: EntityService,
        entity_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> PaginatedJSON[list[Narrative]]:
        narrative_service = NarrativeService(entity_service._connection_factory)
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
        entity_service: EntityService,
        entity_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> PaginatedJSON[list[EnrichedClaim]]:
        claims_service = ClaimsService(entity_service._connection_factory)
        claims, total = await claims_service.get_claims_by_entity(
            entity_id, limit=limit, offset=offset
        )
        page = (offset // limit) + 1 if limit > 0 else 1
        return PaginatedJSON(
            data=claims, total=total, page=page, size=len(claims)
        )
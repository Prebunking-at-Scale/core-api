from typing import AsyncContextManager
from uuid import UUID

from core.entities.models import EnrichedEntity, EntityInput
from core.entities.repo import EntityRepository
from core.models import Entity
from core.uow import ConnectionFactory, uow


class EntityService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[EntityRepository]:
        return uow(EntityRepository, self._connection_factory)

    async def process_entities(self, entities: list[EntityInput]) -> list[UUID]:
        """Process a list of entity inputs and return their IDs"""
        if not entities:
            return []
        
        entity_ids = []
        async with self.repo() as repo:
            for entity_input in entities:

                entity = await repo.get_or_create_entity(
                    wikidata_id=entity_input.wikidata_id,
                    name=entity_input.entity_name,
                    metadata={
                        "entity_type": entity_input.entity_type,
                        "wikidata_info": entity_input.wikidata_info
                    },
                )
                entity_ids.append(entity.id)
        
        return entity_ids

    async def associate_entities_with_claim(
        self, claim_id: UUID, entities: list[EntityInput]
    ) -> list[Entity]:
        """Process entities and associate them with a claim"""
        async with self.repo() as repo:

            entity_ids = []
            processed_entities = []
            
            for entity_input in entities:
                entity = await repo.get_or_create_entity(
                    wikidata_id=entity_input.wikidata_id,
                    name=entity_input.entity_name,
                    metadata={
                        "entity_type": entity_input.entity_type,
                        "wikidata_info": entity_input.wikidata_info
                    },
                )
                entity_ids.append(entity.id)
                processed_entities.append(entity)
            
            await repo.associate_entities_with_claim(claim_id, entity_ids)
            
            return processed_entities

    async def associate_entities_with_narrative(
        self, narrative_id: UUID, entities: list[EntityInput]
    ) -> list[Entity]:
        """Process entities and associate them with a narrative"""
        async with self.repo() as repo:

            entity_ids = []
            processed_entities = []
            
            for entity_input in entities:
                entity = await repo.get_or_create_entity(
                    wikidata_id=entity_input.wikidata_id,
                    name=entity_input.entity_name,
                    metadata={
                        "entity_type": entity_input.entity_type,
                        "wikidata_info": entity_input.wikidata_info
                    },
                )
                entity_ids.append(entity.id)
                processed_entities.append(entity)
            
            await repo.associate_entities_with_narrative(narrative_id, entity_ids)
            
            return processed_entities

    async def get_entities_for_claim(self, claim_id: UUID) -> list[Entity]:
        """Get all entities associated with a claim"""
        async with self.repo() as repo:
            return await repo.get_entities_for_claim(claim_id)

    async def get_entities_for_narrative(self, narrative_id: UUID) -> list[Entity]:
        """Get all entities associated with a narrative"""
        async with self.repo() as repo:
            return await repo.get_entities_for_narrative(narrative_id)

    async def get_entity(self, entity_id: UUID) -> Entity | None:
        """Get a single entity by ID"""
        async with self.repo() as repo:
            return await repo.get_entity(entity_id)

    async def get_all_entities(
        self,
        limit: int = 100,
        offset: int = 0,
        text: str | None = None,
        language: str | None = None,
        narratives_min: int | None = None,
        narratives_max: int | None = None
    ) -> tuple[list[EnrichedEntity], int]:
        """Get all enriched entities with pagination and optional filters"""
        async with self.repo() as repo:
            entities = await repo.get_all_enriched_entities(
                limit=limit,
                offset=offset,
                text=text,
                language=language,
                narratives_min=narratives_min,
                narratives_max=narratives_max
            )
            total = await repo.count_all_entities(
                text=text,
                language=language,
                narratives_min=narratives_min,
                narratives_max=narratives_max
            )
            return entities, total
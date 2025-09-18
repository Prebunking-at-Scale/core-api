from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.models import Entity


class EntityRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def get_or_create_entity(
        self, wikidata_id: str, name: str, metadata: dict[str, Any]
    ) -> Entity:
        """Get existing entity by wikidata_id or create a new one"""
        # First try to find existing entity
        await self._session.execute(
            """
            SELECT id, wikidata_id, name, metadata, created_at, updated_at
            FROM entities
            WHERE wikidata_id = %(wikidata_id)s
            """,
            {"wikidata_id": wikidata_id},
        )
        existing = await self._session.fetchone()
        
        if existing:
            return Entity(
                id=existing["id"],
                wikidata_id=existing["wikidata_id"],
                name=existing["name"],
                metadata=existing["metadata"],
                created_at=existing["created_at"],
                updated_at=existing["updated_at"],
            )
        
        # Create new entity if not found
        await self._session.execute(
            """
            INSERT INTO entities (wikidata_id, name, metadata)
            VALUES (%(wikidata_id)s, %(name)s, %(metadata)s)
            RETURNING id, wikidata_id, name, metadata, created_at, updated_at
            """,
            {"wikidata_id": wikidata_id, "name": name, "metadata": Jsonb(metadata)},
        )
        new_entity = await self._session.fetchone()
        
        return Entity(
            id=new_entity["id"],
            wikidata_id=new_entity["wikidata_id"],
            name=new_entity["name"],
            metadata=new_entity["metadata"],
            created_at=new_entity["created_at"],
            updated_at=new_entity["updated_at"],
        )

    async def get_entities_by_ids(self, entity_ids: list[UUID]) -> list[Entity]:
        """Get entities by their IDs"""
        if not entity_ids:
            return []
        
        await self._session.execute(
            """
            SELECT id, wikidata_id, name, metadata, created_at, updated_at
            FROM entities
            WHERE id = ANY(%(entity_ids)s)
            """,
            {"entity_ids": entity_ids},
        )
        rows = await self._session.fetchall()
        
        return [
            Entity(
                id=row["id"],
                wikidata_id=row["wikidata_id"],
                name=row["name"],
                metadata=row["metadata"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    async def associate_entities_with_claim(
        self, claim_id: UUID, entity_ids: list[UUID]
    ) -> None:
        """Associate entities with a claim, replacing existing associations"""

        await self._session.execute(
            "DELETE FROM claim_entities WHERE claim_id = %(claim_id)s",
            {"claim_id": claim_id},
        )
        
        if entity_ids:
            await self._session.executemany(
                """
                INSERT INTO claim_entities (claim_id, entity_id)
                VALUES (%(claim_id)s, %(entity_id)s)
                ON CONFLICT (claim_id, entity_id) DO NOTHING
                """,
                [{"claim_id": claim_id, "entity_id": entity_id} for entity_id in entity_ids],
            )

    async def associate_entities_with_narrative(
        self, narrative_id: UUID, entity_ids: list[UUID]
    ) -> None:
        """Associate entities with a narrative, replacing existing associations"""

        await self._session.execute(
            "DELETE FROM narrative_entities WHERE narrative_id = %(narrative_id)s",
            {"narrative_id": narrative_id},
        )
        
        if entity_ids:
            await self._session.executemany(
                """
                INSERT INTO narrative_entities (narrative_id, entity_id)
                VALUES (%(narrative_id)s, %(entity_id)s)
                ON CONFLICT (narrative_id, entity_id) DO NOTHING
                """,
                [{"narrative_id": narrative_id, "entity_id": entity_id} for entity_id in entity_ids],
            )

    async def get_entities_for_claim(self, claim_id: UUID) -> list[Entity]:
        """Get all entities associated with a claim"""
        await self._session.execute(
            """
            SELECT e.id, e.wikidata_id, e.name, e.metadata, e.created_at, e.updated_at
            FROM entities e
            JOIN claim_entities ce ON e.id = ce.entity_id
            WHERE ce.claim_id = %(claim_id)s
            """,
            {"claim_id": claim_id},
        )
        rows = await self._session.fetchall()
        
        return [
            Entity(
                id=row["id"],
                wikidata_id=row["wikidata_id"],
                name=row["name"],
                metadata=row["metadata"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    async def get_entities_for_narrative(self, narrative_id: UUID) -> list[Entity]:
        """Get all entities associated with a narrative"""
        await self._session.execute(
            """
            SELECT e.id, e.wikidata_id, e.name, e.metadata, e.created_at, e.updated_at
            FROM entities e
            JOIN narrative_entities ne ON e.id = ne.entity_id
            WHERE ne.narrative_id = %(narrative_id)s
            """,
            {"narrative_id": narrative_id},
        )
        rows = await self._session.fetchall()

        return [
            Entity(
                id=row["id"],
                wikidata_id=row["wikidata_id"],
                name=row["name"],
                metadata=row["metadata"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    async def get_entity(self, entity_id: UUID) -> Entity | None:
        """Get a single entity by ID"""
        await self._session.execute(
            """
            SELECT id, wikidata_id, name, metadata, created_at, updated_at
            FROM entities
            WHERE id = %(entity_id)s
            """,
            {"entity_id": entity_id},
        )
        row = await self._session.fetchone()

        if not row:
            return None

        return Entity(
            id=row["id"],
            wikidata_id=row["wikidata_id"],
            name=row["name"],
            metadata=row["metadata"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def get_all_entities(
        self,
        limit: int = 100,
        offset: int = 0,
        text: str | None = None
    ) -> list[Entity]:
        """Get all entities with pagination and optional text search"""
        query = """
            SELECT id, wikidata_id, name, metadata, created_at, updated_at
            FROM entities
        """
        params: dict = {"limit": limit, "offset": offset}

        if text:
            query += " WHERE name ILIKE %(text)s"
            params["text"] = f"%{text}%"

        query += " ORDER BY created_at DESC LIMIT %(limit)s OFFSET %(offset)s"

        await self._session.execute(query, params)
        rows = await self._session.fetchall()

        return [
            Entity(
                id=row["id"],
                wikidata_id=row["wikidata_id"],
                name=row["name"],
                metadata=row["metadata"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    async def count_all_entities(self, text: str | None = None) -> int:
        """Count total entities with optional text filter"""
        query = "SELECT COUNT(*) FROM entities"
        params: dict = {}

        if text:
            query += " WHERE name ILIKE %(text)s"
            params["text"] = f"%{text}%"

        await self._session.execute(query, params)
        row = await self._session.fetchone()
        return row["count"] if row else 0
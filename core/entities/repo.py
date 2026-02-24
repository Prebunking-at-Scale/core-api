from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.entities.models import EnrichedEntity
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
            return Entity(**existing)
        
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

        if not new_entity:
            raise RuntimeError("Failed to create entity")

        return Entity(**new_entity)

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

    async def count_all_entities(
        self,
        text: str | None = None,
        language: str | None = None,
        narratives_min: int | None = None,
        narratives_max: int | None = None
    ) -> int:
        """Count total entities with optional text, language, and narratives range filters"""
        params: dict = {}
        conditions = []
        
        # Determine if we need a subquery for HAVING clause
        needs_having = narratives_min is not None or narratives_max is not None
        needs_joins = language is not None or needs_having
        
        # Build base query
        if needs_having:
            query = "SELECT COUNT(*) FROM (\n    SELECT e.id\n    FROM entities e"
        else:
            query = "SELECT COUNT(DISTINCT e.id)\nFROM entities e"
        
        # Add JOINs if needed
        if needs_joins:
            query += """
            LEFT JOIN claim_entities ce ON e.id = ce.entity_id
            LEFT JOIN claim_narratives cn ON ce.claim_id = cn.claim_id
            LEFT JOIN video_claims c ON cn.claim_id = c.id
            LEFT JOIN videos v ON c.video_id = v.id
            LEFT JOIN narrative_entities ne ON e.id = ne.entity_id
            LEFT JOIN narratives n ON ne.narrative_id = n.id"""
        
        # Build WHERE conditions
        if text:
            conditions.append("e.name ILIKE %(text)s")
            params["text"] = f"%{text}%"
        
        if language:
            conditions.append("v.metadata->>'language' = %(language)s")
            params["language"] = language
        
        if conditions:
            query += "\n        WHERE " + " AND ".join(conditions)
        
        # Add HAVING clause if filtering by narratives
        if needs_having:
            query += "\n        GROUP BY e.id"
            
            # Build HAVING condition based on which parameters are provided
            if narratives_min is not None and narratives_max is not None:
                query += "\n        HAVING COUNT(DISTINCT n.id) BETWEEN %(narratives_min)s AND %(narratives_max)s"
                params["narratives_min"] = narratives_min
                params["narratives_max"] = narratives_max
            elif narratives_min is not None:
                query += "\n        HAVING COUNT(DISTINCT n.id) >= %(narratives_min)s"
                params["narratives_min"] = narratives_min
            elif narratives_max is not None:
                query += "\n        HAVING COUNT(DISTINCT n.id) <= %(narratives_max)s"
                params["narratives_max"] = narratives_max
            
            query += "\n) AS filtered_entities"

        await self._session.execute(query, params)
        row = await self._session.fetchone()
        return row["count"] if row else 0

    async def get_all_enriched_entities(
        self,
        limit: int = 100,
        offset: int = 0,
        text: str | None = None,
        language: str | None = None,
        narratives_min: int | None = None,
        narratives_max: int | None = None
    ) -> list[EnrichedEntity]:
        """Get all entities with statistics (claims, videos, platforms, languages, narratives)"""
        query = """
            SELECT 
                e.id, 
                e.wikidata_id, 
                e.name, 
                e.metadata, 
                e.created_at, 
                e.updated_at,
                COUNT(DISTINCT c.id) as total_claims, 
                COUNT(DISTINCT v.id) as total_videos,
                COUNT(DISTINCT n.id) as linked_narratives,
                COALESCE(
                    array_agg(DISTINCT v.platform) FILTER (WHERE v.platform IS NOT NULL),
                    ARRAY[]::text[]
                ) as platforms,
                COALESCE(
                    array_agg(DISTINCT v.metadata->>'language') FILTER (WHERE v.metadata->>'language' IS NOT NULL),
                    ARRAY[]::text[]
                ) as languages
            FROM entities e
            LEFT JOIN claim_entities ce ON e.id = ce.entity_id
            LEFT JOIN claim_narratives cn ON ce.claim_id = cn.claim_id
            LEFT JOIN video_claims c ON cn.claim_id = c.id
            LEFT JOIN videos v ON c.video_id = v.id
            LEFT JOIN narrative_entities ne ON e.id = ne.entity_id
            LEFT JOIN narratives n ON ne.narrative_id = n.id
        """
        params: dict = {"limit": limit, "offset": offset}
        conditions = []

        if text:
            conditions.append("e.name ILIKE %(text)s")
            params["text"] = f"%{text}%"
        
        if language:
            conditions.append("v.metadata->>'language' = %(language)s")
            params["language"] = language

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += """
            GROUP BY e.id, e.wikidata_id, e.name, e.metadata, e.created_at, e.updated_at
        """
        
        # Add HAVING clause for narratives filter
        if narratives_min is not None and narratives_max is not None:
            query += " HAVING COUNT(DISTINCT n.id) BETWEEN %(narratives_min)s AND %(narratives_max)s"
            params["narratives_min"] = narratives_min
            params["narratives_max"] = narratives_max
        elif narratives_min is not None:
            query += " HAVING COUNT(DISTINCT n.id) >= %(narratives_min)s"
            params["narratives_min"] = narratives_min
        elif narratives_max is not None:
            query += " HAVING COUNT(DISTINCT n.id) <= %(narratives_max)s"
            params["narratives_max"] = narratives_max
        
        query += """
            ORDER BY e.created_at DESC 
            LIMIT %(limit)s OFFSET %(offset)s
        """

        await self._session.execute(query, params)
        rows = await self._session.fetchall()

        return [
            EnrichedEntity(
                id=row["id"],
                wikidata_id=row["wikidata_id"],
                name=row["name"],
                metadata=row["metadata"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                total_claims=row["total_claims"],
                total_videos=row["total_videos"],
                linked_narratives=row["linked_narratives"],
                platforms=row["platforms"] or [],
                languages=row["languages"] or [],
            )
            for row in rows
        ]

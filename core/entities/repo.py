from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.entities.models import EnrichedEntity
from core.models import Entity


# Wikidata "instance of" (P31) values that mark an entity as a country-like
# thing, for which the flag (P41) is a better avatar than the generic image.
_COUNTRY_P31_IDS = {"Q3624078", "Q6256", "Q672729"}
_COUNTRY_LABEL_HINTS = ("country", "state", "republic")


def _pick_image_url(wikidata_info: dict | None) -> str | None:
    """Choose the best image URL from a Wikidata claims blob.

    Mirrors the frontend's getEntityImage() order: for countries prefer the
    flag (P41) over the image (P18); for everyone else prefer P18 (image)
    over P41 (flag) over P154 (logo).
    """
    if not wikidata_info:
        return None
    claims = wikidata_info.get("claims") or {}
    p31 = claims.get("P31") or []
    is_country = any(
        c.get("id") in _COUNTRY_P31_IDS
        or any(h in (c.get("label") or "").lower() for h in _COUNTRY_LABEL_HINTS)
        for c in p31
    )
    order = ("P41", "P18", "P154") if is_country else ("P18", "P41", "P154")
    for prop in order:
        for item in claims.get(prop) or []:
            url = item.get("url")
            if url:
                return url
    return None


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

    async def get_entity_images_by_wikidata_ids(
        self, wikidata_ids: list[str]
    ) -> dict[str, str | None]:
        """Resolve a batch of Wikidata Q-ids to their image URL.

        Used by the entity graph explorer to render each node's Wikidata photo
        inside its bubble. Returns the best URL we can find — P41 (flag) for
        countries, P18 (image), P154 (logo) — or None when the entity isn't in
        the local entities table or has no usable claim.
        """
        if not wikidata_ids:
            return {}
        await self._session.execute(
            """
            SELECT wikidata_id, metadata
            FROM entities
            WHERE wikidata_id = ANY(%(ids)s)
            """,
            {"ids": wikidata_ids},
        )
        rows = await self._session.fetchall()
        out: dict[str, str | None] = {qid: None for qid in wikidata_ids}
        for row in rows:
            info = (row["metadata"] or {}).get("wikidata_info") or {}
            out[row["wikidata_id"]] = _pick_image_url(info)
        return out

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

    _ENRICHED_CTES = """
        WITH entity_video_stats AS (
            SELECT
                ce.entity_id,
                COUNT(DISTINCT c.id) AS total_claims,
                COUNT(DISTINCT v.id) AS total_videos,
                COALESCE(
                    array_agg(DISTINCT v.platform) FILTER (WHERE v.platform IS NOT NULL),
                    ARRAY[]::text[]
                ) AS platforms,
                COALESCE(
                    array_agg(DISTINCT v.metadata->>'language')
                        FILTER (WHERE v.metadata->>'language' IS NOT NULL),
                    ARRAY[]::text[]
                ) AS languages
            FROM claim_entities ce
            LEFT JOIN video_claims c ON ce.claim_id = c.id
            LEFT JOIN videos v ON c.video_id = v.id
            GROUP BY ce.entity_id
        ),
        entity_narrative_stats AS (
            SELECT entity_id, COUNT(*) AS linked_narratives
            FROM narrative_entities
            GROUP BY entity_id
        )
    """

    _LATERAL_STATS_JOIN = """
        LEFT JOIN LATERAL (
            SELECT
                COUNT(DISTINCT c.id) AS total_claims,
                COUNT(DISTINCT v.id) AS total_videos,
                COALESCE(
                    array_agg(DISTINCT v.platform) FILTER (WHERE v.platform IS NOT NULL),
                    ARRAY[]::text[]
                ) AS platforms,
                COALESCE(
                    array_agg(DISTINCT v.metadata->>'language')
                        FILTER (WHERE v.metadata->>'language' IS NOT NULL),
                    ARRAY[]::text[]
                ) AS languages
            FROM claim_entities ce
            LEFT JOIN video_claims c ON ce.claim_id = c.id
            LEFT JOIN videos v ON c.video_id = v.id
            WHERE ce.entity_id = e.id
        ) evs ON true
        LEFT JOIN LATERAL (
            SELECT COUNT(*) AS linked_narratives
            FROM narrative_entities
            WHERE entity_id = e.id
        ) ens ON true
    """

    @staticmethod
    def _entity_only_filters(text: str | None, hours: int | None) -> tuple[str, dict]:
        params: dict = {}
        where: list[str] = []
        if hours is not None:
            where.append("e.updated_at >= NOW() - %(hours)s * INTERVAL '1 hour'")
            params["hours"] = hours
        if text:
            where.append("e.name ILIKE %(text)s")
            params["text"] = f"%{text}%"
        clause = " WHERE " + " AND ".join(where) if where else ""
        return clause, params

    @staticmethod
    def _build_enriched_filters(
        text: str | None,
        hours: int | None,
        language: str | None,
        narratives_min: int | None,
        narratives_max: int | None,
    ) -> tuple[str, dict]:
        params: dict = {}
        where: list[str] = []

        if hours is not None:
            where.append("e.updated_at >= NOW() - %(hours)s * INTERVAL '1 hour'")
            params["hours"] = hours
        if text:
            where.append("e.name ILIKE %(text)s")
            params["text"] = f"%{text}%"
        if language is not None:
            where.append("%(language)s = ANY(COALESCE(evs.languages, ARRAY[]::text[]))")
            params["language"] = language
        if narratives_min is not None:
            where.append("COALESCE(ens.linked_narratives, 0) >= %(narratives_min)s")
            params["narratives_min"] = narratives_min
        if narratives_max is not None:
            where.append("COALESCE(ens.linked_narratives, 0) <= %(narratives_max)s")
            params["narratives_max"] = narratives_max

        clause = ""
        if where:
            clause = " WHERE " + " AND ".join(where)
        return clause, params

    async def count_all_entities(
        self,
        text: str | None = None,
        hours: int | None = None,
        language: str | None = None,
        narratives_min: int | None = None,
        narratives_max: int | None = None,
    ) -> int:
        """Count total entities matching the same filters as get_all_enriched_entities."""
        # Fast path: no aggregate filter, count straight from entities.
        if language is None and narratives_min is None and narratives_max is None:
            where_clause, params = self._entity_only_filters(text, hours)
            query = "SELECT COUNT(*) AS count FROM entities e" + where_clause
            await self._session.execute(query, params)
            row = await self._session.fetchone()
            return row["count"] if row else 0

        where_clause, params = self._build_enriched_filters(
            text, hours, language, narratives_min, narratives_max,
        )
        query = (
            self._ENRICHED_CTES
            + """
            SELECT COUNT(*) AS count
            FROM entities e
            LEFT JOIN entity_video_stats evs ON e.id = evs.entity_id
            LEFT JOIN entity_narrative_stats ens ON e.id = ens.entity_id
            """
            + where_clause
        )

        await self._session.execute(query, params)
        row = await self._session.fetchone()
        return row["count"] if row else 0

    async def get_all_enriched_entities(
        self,
        limit: int = 100,
        offset: int = 0,
        text: str | None = None,
        hours: int | None = None,
        language: str | None = None,
        narratives_min: int | None = None,
        narratives_max: int | None = None,
    ) -> list[EnrichedEntity]:
        """Get all entities with statistics (claims, videos, platforms, languages, narratives)."""
        # Fast path: no filter depends on aggregated stats, so page entities first
        # and compute stats per row via LATERAL — keeps work O(limit) instead of O(N).
        if language is None and narratives_min is None and narratives_max is None:
            where_clause, params = self._entity_only_filters(text, hours)
            params["limit"] = limit
            params["offset"] = offset
            query = (
                """
                SELECT
                    e.id,
                    e.wikidata_id,
                    e.name,
                    e.metadata,
                    e.created_at,
                    e.updated_at,
                    COALESCE(evs.total_claims, 0) AS total_claims,
                    COALESCE(evs.total_videos, 0) AS total_videos,
                    COALESCE(ens.linked_narratives, 0) AS linked_narratives,
                    COALESCE(evs.platforms, ARRAY[]::text[]) AS platforms,
                    COALESCE(evs.languages, ARRAY[]::text[]) AS languages
                FROM (
                    SELECT id, wikidata_id, name, metadata, created_at, updated_at
                    FROM entities e
                """
                + where_clause
                + """
                    ORDER BY created_at DESC
                    LIMIT %(limit)s OFFSET %(offset)s
                ) e
                """
                + self._LATERAL_STATS_JOIN
                + """
                ORDER BY e.created_at DESC
                """
            )
        else:
            where_clause, params = self._build_enriched_filters(
                text, hours, language, narratives_min, narratives_max,
            )
            params["limit"] = limit
            params["offset"] = offset
            query = (
                self._ENRICHED_CTES
                + """
                SELECT
                    e.id,
                    e.wikidata_id,
                    e.name,
                    e.metadata,
                    e.created_at,
                    e.updated_at,
                    COALESCE(evs.total_claims, 0) AS total_claims,
                    COALESCE(evs.total_videos, 0) AS total_videos,
                    COALESCE(ens.linked_narratives, 0) AS linked_narratives,
                    COALESCE(evs.platforms, ARRAY[]::text[]) AS platforms,
                    COALESCE(evs.languages, ARRAY[]::text[]) AS languages
                FROM entities e
                LEFT JOIN entity_video_stats evs ON e.id = evs.entity_id
                LEFT JOIN entity_narrative_stats ens ON e.id = ens.entity_id
                """
                + where_clause
                + """
                ORDER BY e.created_at DESC
                LIMIT %(limit)s OFFSET %(offset)s
                """
            )

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

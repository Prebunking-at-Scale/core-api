from uuid import UUID

import psycopg
from psycopg.rows import DictRow

from core.saved_selections.models import SavedSelection, SavedSelectionError


class SavedSelectionRepository:
    """A person's own selections. Every query is scoped to both the organisation and
    the person: nobody sees or deletes anyone else's."""

    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def list_own(
        self, organisation_id: UUID, user_id: UUID, kind: str
    ) -> list[SavedSelection]:
        await self._session.execute(
            """
            SELECT id, kind, name, values, created_at
            FROM saved_selections
            WHERE organisation_id = %(organisation_id)s
              AND user_id = %(user_id)s
              AND kind = %(kind)s
            ORDER BY lower(name)
            """,
            {"organisation_id": organisation_id, "user_id": user_id, "kind": kind},
        )
        return [
            SavedSelection(**{**row, "id": str(row["id"])})
            for row in await self._session.fetchall()
        ]

    async def create(
        self,
        organisation_id: UUID,
        user_id: UUID,
        kind: str,
        name: str,
        values: list[str],
    ) -> SavedSelection:
        try:
            await self._session.execute(
                """
                INSERT INTO saved_selections (organisation_id, user_id, kind, name, values)
                VALUES (%(organisation_id)s, %(user_id)s, %(kind)s, %(name)s, %(values)s)
                RETURNING id, kind, name, values, created_at
                """,
                {
                    "organisation_id": organisation_id,
                    "user_id": user_id,
                    "kind": kind,
                    "name": name,
                    "values": values,
                },
            )
        except psycopg.errors.UniqueViolation:
            raise SavedSelectionError(detail="name_taken")
        row = await self._session.fetchone()
        if not row:
            raise ValueError("failed to save the selection")
        return SavedSelection(**{**row, "id": str(row["id"])})

    async def delete(
        self, organisation_id: UUID, user_id: UUID, selection_id: UUID
    ) -> bool:
        await self._session.execute(
            """
            DELETE FROM saved_selections
            WHERE id = %(id)s
              AND organisation_id = %(organisation_id)s
              AND user_id = %(user_id)s
            """,
            {"id": selection_id, "organisation_id": organisation_id, "user_id": user_id},
        )
        return self._session.rowcount > 0

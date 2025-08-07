from datetime import datetime
from uuid import UUID

import psycopg
from psycopg.rows import DictRow

from core.auth.models import Organisation, User
from core.errors import (
    ConflictError,
    InvalidInviteError,
    NotAuthorizedError,
    NotFoundError,
)


class AuthRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def add_user(self, user: User) -> User:
        try:
            await self._session.execute(
                """
                INSERT INTO users (display_name, email)
                VALUES (%(display_name)s, %(email)s)
                RETURNING *
                """,
                user.model_dump(),
            )
        except psycopg.errors.UniqueViolation:
            raise ConflictError("email address is already in use")

        row = await self._session.fetchone()
        if not row:
            raise ValueError("could not add user for unknown reason")
        return User(**row)

    async def update_user(self, user: User) -> User:
        await self._session.execute(
            """
            UPDATE users SET
                display_name = %(display_name)s,
                updated_at = now()
            WHERE id = %(id)s
            RETURNING *
            """,
            user.model_dump(),
        )
        row = await self._session.fetchone()
        if not row:
            raise ValueError("user not found")
        return User(**row)

    async def get_user_by_id(self, id: UUID) -> User | None:
        await self._session.execute(
            """
            SELECT * FROM users WHERE id = %(id)s
            """,
            {"id": id},
        )
        row = await self._session.fetchone()
        if not row:
            return None
        return User(**row)

    async def get_user_by_email(self, email: str) -> User | None:
        await self._session.execute(
            """
            SELECT * FROM users WHERE lower(email) = lower(%(email)s)
            """,
            {"email": email},
        )
        row = await self._session.fetchone()
        if not row:
            return None
        return User(**row)

    async def verify_login(self, email: str, password_hash: bytes) -> User:
        await self._session.execute(
            """
            SELECT *
            FROM users
            WHERE email = %(email)s
            AND password_hash = %(password_hash)s
            """,
            {"email": email, "password_hash": password_hash},
        )
        row = await self._session.fetchone()
        if not row:
            raise NotAuthorizedError()
        return User(**row)

    async def get_password_hash(self, email: str) -> bytes:
        await self._session.execute(
            """
            SELECT password_hash FROM users
            WHERE email = %(email)s
            """,
            {
                "email": email,
            },
        )
        row = await self._session.fetchone()
        if not row:
            raise NotAuthorizedError()
        return row["password_hash"]

    async def update_password_hash(
        self,
        user_id: UUID,
        new_hash: bytes,
        last_updated_before: datetime | None = None,
    ) -> None:
        await self._session.execute(
            """
            UPDATE users SET
                password_hash = %(new_hash)s,
                password_last_updated = now()
            WHERE
                id = %(id)s
                AND (
                    password_last_updated < %(last_updated_before)s
                    OR password_last_updated IS NULL
                    OR %(last_updated_before)s IS NULL
                )
            """,
            {
                "id": user_id,
                "new_hash": new_hash,
                "last_updated_before": last_updated_before,
            },
        )
        if not self._session.rowcount:
            raise NotAuthorizedError("password could not be changed")

    async def add_organisation(self, organisation: Organisation) -> Organisation:
        try:
            await self._session.execute(
                """
                INSERT INTO organisations (
                    id, display_name, short_name, language, country_codes
                ) VALUES (
                    %(id)s, %(display_name)s, %(short_name)s, %(language)s, %(country_codes)s
                )
                RETURNING *
                """,
                organisation.model_dump(),
            )
        except psycopg.errors.UniqueViolation:
            raise ConflictError("short name is already in use")

        row = await self._session.fetchone()
        if not row:
            raise ValueError("could not add organisation for unknown reason")
        return Organisation(**row)

    async def update_organisation(self, organisation: Organisation) -> Organisation:
        await self._session.execute(
            """
            UPDATE organisations SET
                display_name = %(display_name)s,
                country_codes = %(country_codes)s,
                language = %(language)s,
                updated_at = now()
            WHERE id = %(id)s
            RETURNING *
            """,
            organisation.model_dump(),
        )
        row = await self._session.fetchone()
        if not row:
            raise ValueError("organisation not found")
        return Organisation(**row)

    async def deactivate_organisation(self, organisation_id: UUID):
        await self._session.execute(
            """
            UPDATE organisations SET
                deactivated = now()
            WHERE
                id = %(organisation_id)s
                AND deactivated IS NULL
            """,
            {"organisation_id": organisation_id},
        )
        if not self._session.rowcount:
            raise NotFoundError("Organisation not found or already deactivated")

    async def get_organisation(self, id: UUID | str) -> Organisation:
        await self._session.execute(
            """
            SELECT * FROM organisations WHERE id = %(id)s
            """,
            {"id": id},
        )
        row = await self._session.fetchone()
        if not row:
            raise NotFoundError("organisation not found")
        return Organisation(**row)

    async def invite_user_to_organisation(
        self,
        user_id: UUID,
        organisation_id: UUID,
        auto_accept: bool = False,
        admin: bool = False,
    ) -> None:
        await self._session.execute(
            """
            INSERT INTO organisation_users (
                organisation_id, user_id, accepted, is_admin
            ) VALUES (
                %(organisation_id)s,
                %(user_id)s,
                CASE WHEN %(auto_accept)s THEN now() END,
                %(is_admin)s
            )
            ON CONFLICT (organisation_id, user_id)
            DO UPDATE SET
                invited = now(),
                accepted = CASE WHEN %(auto_accept)s THEN now() ELSE NULL END,
                deactivated = NULL,
                is_admin = %(is_admin)s
            WHERE
                organisation_users.accepted IS NOT NULL
                AND organisation_users.deactivated IS NOT NULL
            """,
            {
                "organisation_id": organisation_id,
                "user_id": user_id,
                "auto_accept": auto_accept,
                "is_admin": admin,
            },
        )
        if not self._session.rowcount:
            raise ConflictError(
                "user is already part of organisation or has valid invite"
            )

    async def accept_invite(self, user_id: UUID, organisation_id: UUID) -> None:
        await self._session.execute(
            """
            UPDATE organisation_users SET
                accepted = now()
            WHERE
                user_id = %(user_id)s
                AND organisation_id = %(organisation_id)s
                AND accepted IS NULL
                AND deactivated IS NULL
            """,
            {
                "organisation_id": organisation_id,
                "user_id": user_id,
            },
        )
        if not self._session.rowcount:
            raise InvalidInviteError()

    async def deactivate_user(self, user_id: UUID, organisation_id: UUID) -> None:
        await self._session.execute(
            """
            UPDATE organisation_users SET
                deactivated = now()
            WHERE
                user_id = %(user_id)s
                AND organisation_id = %(organisation_id)s
                AND deactivated IS NULL
            """,
            {
                "organisation_id": organisation_id,
                "user_id": user_id,
            },
        )
        if not self._session.rowcount:
            raise NotFoundError("user not found or already deactivated")

    async def set_admin(self, user_id: UUID, organisation_id: UUID, is_admin: bool):
        await self._session.execute(
            """
            UPDATE organisation_users SET
                is_admin = %(is_admin)s
            WHERE
                user_id = %(user_id)s
                AND organisation_id = %(organisation_id)s
                AND deactivated IS NULL
            """,
            {
                "organisation_id": organisation_id,
                "user_id": user_id,
                "is_admin": is_admin,
            },
        )
        if not self._session.rowcount:
            raise NotFoundError("user not found")

    async def organisation_and_role(
        self, user_id: UUID, organisation_id: UUID
    ) -> tuple[Organisation, bool]:
        await self._session.execute(
            """
                WITH is_super_admin AS (
                    SELECT is_super_admin FROM users
                    WHERE id = %(user_id)s
                ), is_organisation_admin AS (
                    SELECT is_admin
                    FROM organisation_users ou
                    WHERE
                        organisation_id = %(organisation_id)s
                        AND user_id = %(user_id)s
                        AND deactivated IS NULL
                        AND accepted IS NOT NULL
                )
                SELECT *, (is_admin or is_super_admin) as is_admin
                FROM organisations o
                LEFT OUTER JOIN is_organisation_admin ON TRUE
                LEFT OUTER JOIN is_super_admin ON TRUE
                WHERE
                    o.id = %(organisation_id)s
                    AND o.deactivated IS NULL
                    AND (is_admin is NOT NULL OR is_super_admin)
            """,
            {
                "organisation_id": organisation_id,
                "user_id": user_id,
            },
        )

        row = await self._session.fetchone()
        if not row:
            raise NotAuthorizedError()
        return Organisation(**row), row["is_admin"]

    async def organisation_memberships(
        self,
        user_id: UUID,
    ) -> tuple[list[Organisation], list[bool]]:
        """returns a list of organisations the user is a member of, and their admin
        admin status"""
        await self._session.execute(
            """
                WITH is_super_admin AS (
                    SELECT is_super_admin FROM users
                    WHERE id = %(user_id)s
                ), is_organisation_admin AS (
                    SELECT organisation_id, is_admin
                    FROM organisation_users ou
                    WHERE
                        user_id = %(user_id)s
                        AND deactivated IS NULL
                        AND accepted IS NOT NULL
                )
                SELECT *, (is_admin or is_super_admin) as is_admin
                FROM organisations o
                LEFT OUTER JOIN is_organisation_admin oa ON oa.organisation_id = o.id
                LEFT OUTER JOIN is_super_admin sa ON TRUE
                WHERE (is_admin is NOT NULL OR is_super_admin)
                AND o.deactivated IS NULL
            """,
            {"user_id": user_id},
        )

        organisations = []
        is_admin = []
        for row in await self._session.fetchall():
            organisations.append(Organisation(**row))
            is_admin.append(row["is_admin"])
        return organisations, is_admin

    async def organisation_users(
        self,
        organisation_id: UUID,
    ) -> list[User]:
        await self._session.execute(
            """
                SELECT *
                FROM users u
                JOIN organisation_users ou ON ou.user_id = u.id
                WHERE
                    ou.organisation_id = %(organisation_id)s
                    AND ou.deactivated IS NULL
                    AND ou.accepted IS NOT NULL
            """,
            {
                "organisation_id": organisation_id,
            },
        )

        return [User(**row) for row in await self._session.fetchall()]

import secrets
from datetime import datetime, timedelta
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.errors import NotFoundError
from core.integrations.slack.models import SlackInstallation, SlackOAuthState


class SlackRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def issue_state(
        self, organisation_id: UUID, expiration_seconds: int = 300
    ) -> str:
        """
        Generate and store a new OAuth state for CSRF protection.

        Args:
            organisation_id: The organisation UUID to associate with this state
            expiration_seconds: Number of seconds until the state expires (default: 300)

        Returns:
            The generated state string
        """
        state = secrets.token_urlsafe(32)
        expires_at = datetime.now() + timedelta(seconds=expiration_seconds)

        await self._session.execute(
            """
            INSERT INTO slack_oauth_states (state, organisation_id, expires_at)
            VALUES (%(state)s, %(organisation_id)s, %(expires_at)s)
            """,
            {
                "state": state,
                "organisation_id": organisation_id,
                "expires_at": expires_at,
            },
        )

        return state

    async def consume_state(self, state: str) -> UUID | None:
        """
        Validate and consume an OAuth state. Returns the organisation_id if the state
        is valid and not expired, None otherwise. Valid states are deleted after consumption.

        Args:
            state: The state string to validate

        Returns:
            The organisation UUID if the state is valid and not expired, None otherwise
        """
        await self._session.execute(
            """
            DELETE FROM slack_oauth_states
            WHERE state = %(state)s AND expires_at > now()
            RETURNING organisation_id
            """,
            {"state": state},
        )

        row = await self._session.fetchone()
        return row["organisation_id"] if row else None

    async def cleanup_expired_states(self) -> int:
        """
        Delete expired OAuth states from the database.

        Returns:
            Number of states deleted
        """
        await self._session.execute(
            """
            DELETE FROM slack_oauth_states
            WHERE expires_at <= now()
            """
        )
        # Note: rowcount is not available with AsyncCursor in all cases
        # This is primarily for maintenance/cleanup
        return 0

    async def save_installation(
        self, installation: SlackInstallation
    ) -> SlackInstallation:
        """
        Save or update a Slack installation for an organisation.
        If an installation with the same organisation_id and team_id exists,
        it will be updated.

        Args:
            installation: The installation to save

        Returns:
            The saved installation with timestamps populated
        """
        data = installation.model_dump()
        # Convert metadata dict to JSONB
        data["metadata"] = Jsonb(data.get("metadata", {}))

        await self._session.execute(
            """
            INSERT INTO slack_installations (
                organisation_id,
                team_id,
                team_name,
                enterprise_id,
                enterprise_name,
                enterprise_url,
                app_id,
                bot_token,
                bot_id,
                bot_user_id,
                bot_scopes,
                user_id,
                user_token,
                user_scopes,
                incoming_webhook_url,
                incoming_webhook_channel,
                incoming_webhook_channel_id,
                incoming_webhook_configuration_url,
                is_enterprise_install,
                token_type,
                metadata
            ) VALUES (
                %(organisation_id)s,
                %(team_id)s,
                %(team_name)s,
                %(enterprise_id)s,
                %(enterprise_name)s,
                %(enterprise_url)s,
                %(app_id)s,
                %(bot_token)s,
                %(bot_id)s,
                %(bot_user_id)s,
                %(bot_scopes)s,
                %(user_id)s,
                %(user_token)s,
                %(user_scopes)s,
                %(incoming_webhook_url)s,
                %(incoming_webhook_channel)s,
                %(incoming_webhook_channel_id)s,
                %(incoming_webhook_configuration_url)s,
                %(is_enterprise_install)s,
                %(token_type)s,
                %(metadata)s
            )
            ON CONFLICT (organisation_id, team_id)
            DO UPDATE SET
                team_name = EXCLUDED.team_name,
                enterprise_id = EXCLUDED.enterprise_id,
                enterprise_name = EXCLUDED.enterprise_name,
                enterprise_url = EXCLUDED.enterprise_url,
                app_id = EXCLUDED.app_id,
                bot_token = EXCLUDED.bot_token,
                bot_id = EXCLUDED.bot_id,
                bot_user_id = EXCLUDED.bot_user_id,
                bot_scopes = EXCLUDED.bot_scopes,
                user_id = EXCLUDED.user_id,
                user_token = EXCLUDED.user_token,
                user_scopes = EXCLUDED.user_scopes,
                incoming_webhook_url = EXCLUDED.incoming_webhook_url,
                incoming_webhook_channel = EXCLUDED.incoming_webhook_channel,
                incoming_webhook_channel_id = EXCLUDED.incoming_webhook_channel_id,
                incoming_webhook_configuration_url = EXCLUDED.incoming_webhook_configuration_url,
                is_enterprise_install = EXCLUDED.is_enterprise_install,
                token_type = EXCLUDED.token_type,
                metadata = EXCLUDED.metadata,
                updated_at = now()
            RETURNING *
            """,
            data,
        )

        row = await self._session.fetchone()
        if not row:
            raise ValueError("could not save installation")

        return SlackInstallation(**row)

    async def find_bot(
        self, enterprise_id: str | None = None, team_id: str | None = None
    ) -> SlackInstallation | None:
        """
        Find a Slack installation by enterprise_id and/or team_id.
        This mirrors the slack_sdk InstallationStore.find_bot() interface.

        Args:
            enterprise_id: Slack enterprise ID (optional)
            team_id: Slack team/workspace ID (optional)

        Returns:
            The installation if found, None otherwise
        """
        if team_id:
            # Search by team_id (and enterprise_id if provided)
            query = """
                SELECT * FROM slack_installations
                WHERE team_id = %(team_id)s
            """
            params = {"team_id": team_id, "enterprise_id": enterprise_id}

            if enterprise_id:
                query += " AND enterprise_id = %(enterprise_id)s"
        elif enterprise_id:
            # Search only by enterprise_id
            query = """
                SELECT * FROM slack_installations
                WHERE enterprise_id = %(enterprise_id)s
            """
            params = {"enterprise_id": enterprise_id}
        else:
            # No search criteria provided
            return None

        await self._session.execute(query, params)
        row = await self._session.fetchone()

        if not row:
            return None

        return SlackInstallation(**row)

    async def find_installations_by_organisation(
        self, organisation_id: UUID
    ) -> list[SlackInstallation]:
        """
        Find all Slack installations for an organisation.
        An organisation can have multiple installations (one per channel).

        Args:
            organisation_id: The organisation UUID

        Returns:
            List of installations (empty list if none found)
        """
        await self._session.execute(
            """
            SELECT * FROM slack_installations
            WHERE organisation_id = %(organisation_id)s
            ORDER BY created_at DESC
            """,
            {"organisation_id": organisation_id},
        )

        rows = await self._session.fetchall()
        return [SlackInstallation(**row) for row in rows]

    async def find_installation_by_channel_id(
        self, channel_id: str
    ) -> SlackInstallation | None:
        """
        Find a Slack installation by channel ID.
        
        Args:
            channel_id: The Slack channel ID (e.g., C0AMLPMBTGR)
        
        Returns:
            The SlackInstallation if found, None otherwise
        """
        await self._session.execute(
            """
            SELECT * FROM slack_installations
            WHERE incoming_webhook_channel_id = %(channel_id)s
            LIMIT 1
            """,
            {"channel_id": channel_id},
        )
        
        row = await self._session.fetchone()
        
        if not row:
            return None
        
        return SlackInstallation(**row)

    async def delete_installation(
        self, organisation_id: UUID, team_id: str
    ) -> None:
        """
        Delete a Slack installation.

        Args:
            organisation_id: The organisation UUID
            team_id: The Slack team/workspace ID
        """
        await self._session.execute(
            """
            DELETE FROM slack_installations
            WHERE organisation_id = %(organisation_id)s AND team_id = %(team_id)s
            """,
            {"organisation_id": organisation_id, "team_id": team_id},
        )

        if self._session.rowcount == 0:
            raise NotFoundError("installation not found")

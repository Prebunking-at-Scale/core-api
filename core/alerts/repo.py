from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.alerts.models import Alert, AlertExecution, AlertScope, AlertTriggered, AlertType
from core.errors import ConflictError


class AlertRepository:
    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    async def create_alert(
        self,
        user_id: UUID,
        organisation_id: UUID,
        name: str,
        alert_type: AlertType,
        scope: AlertScope,
        narrative_id: UUID | None = None,
        threshold: int | None = None,
        topic_id: UUID | None = None,
        keyword: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Alert:
        try:
            await self._session.execute(
                """
                INSERT INTO alerts (
                    user_id, organisation_id, name, alert_type, scope,
                    narrative_id, threshold, topic_id, keyword, metadata
                ) VALUES (
                    %(user_id)s, %(organisation_id)s, %(name)s, %(alert_type)s, %(scope)s,
                    %(narrative_id)s, %(threshold)s, %(topic_id)s, %(keyword)s, %(metadata)s
                )
                RETURNING *
                """,
                {
                    "user_id": user_id,
                    "organisation_id": organisation_id,
                    "name": name,
                    "alert_type": alert_type.value,
                    "scope": scope.value,
                    "narrative_id": narrative_id,
                    "threshold": threshold,
                    "topic_id": topic_id,
                    "keyword": keyword,
                    "metadata": Jsonb(metadata or {}),
                },
            )
        except psycopg.errors.UniqueViolation:
            raise ConflictError("Alert already exists")

        row = await self._session.fetchone()
        if not row:
            raise ValueError("Failed to create alert")

        return Alert(**row)

    async def get_alert(self, alert_id: UUID) -> Alert | None:
        await self._session.execute(
            "SELECT * FROM alerts WHERE id = %(alert_id)s",
            {"alert_id": alert_id},
        )
        row = await self._session.fetchone()
        return Alert(**row) if row else None

    async def get_user_alerts(
        self,
        user_id: UUID,
        organisation_id: UUID,
        enabled_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[Alert], int]:
        base_params = {"user_id": user_id, "organisation_id": organisation_id}
        
        # Build WHERE clause conditionally
        where_conditions = ["user_id = %(user_id)s", "organisation_id = %(organisation_id)s"]
        if enabled_only:
            where_conditions.append("enabled = TRUE")
        where_clause = " AND ".join(where_conditions)

        # Get total count
        await self._session.execute(
            f"SELECT COUNT(*) FROM alerts WHERE {where_clause}",
            base_params,
        )
        total = (await self._session.fetchone())["count"]

        # Get paginated results
        await self._session.execute(
            f"""
            SELECT * FROM alerts
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {**base_params, "limit": limit, "offset": offset},
        )
        rows = await self._session.fetchall()
        alerts = [Alert(**row) for row in rows]

        return alerts, total

    async def update_alert(
        self,
        alert_id: UUID,
        name: str | None = None,
        enabled: bool | None = None,
        threshold: int | None = None,
        keyword: str | None = None,
    ) -> Alert | None:
        updates = []
        params = {"alert_id": alert_id}

        if name is not None:
            updates.append("name = %(name)s")
            params["name"] = name

        if enabled is not None:
            updates.append("enabled = %(enabled)s")
            params["enabled"] = enabled

        if threshold is not None:
            updates.append("threshold = %(threshold)s")
            params["threshold"] = threshold

        if keyword is not None:
            updates.append("keyword = %(keyword)s")
            params["keyword"] = keyword

        if not updates:
            return await self.get_alert(alert_id)

        updates.append("updated_at = CURRENT_TIMESTAMP")

        await self._session.execute(
            f"""
            UPDATE alerts
            SET {', '.join(updates)}
            WHERE id = %(alert_id)s
            RETURNING *
            """,
            params,
        )
        row = await self._session.fetchone()
        return Alert(**row) if row else None

    async def delete_alert(self, alert_id: UUID) -> bool:
        await self._session.execute(
            "DELETE FROM alerts WHERE id = %(alert_id)s",
            {"alert_id": alert_id},
        )
        return self._session.rowcount > 0

    async def get_active_alerts(self) -> list[Alert]:
        await self._session.execute(
            """
            SELECT * FROM alerts
            WHERE enabled = TRUE
            ORDER BY created_at
            """
        )
        rows = await self._session.fetchall()
        return [Alert(**row) for row in rows]

    async def record_alert_trigger(
        self,
        alert_id: UUID,
        narrative_id: UUID,
        trigger_value: int | None = None,
        threshold_crossed: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> AlertTriggered | None:
        try:
            await self._session.execute(
                """
                INSERT INTO alerts_triggered (
                    alert_id, narrative_id, trigger_value, threshold_crossed, metadata
                ) VALUES (
                    %(alert_id)s, %(narrative_id)s, %(trigger_value)s, %(threshold_crossed)s, %(metadata)s
                )
                ON CONFLICT (alert_id, narrative_id, threshold_crossed) DO NOTHING
                RETURNING *
                """,
                {
                    "alert_id": alert_id,
                    "narrative_id": narrative_id,
                    "trigger_value": trigger_value,
                    "threshold_crossed": threshold_crossed,
                    "metadata": Jsonb(metadata or {}),
                },
            )
            row = await self._session.fetchone()
            return AlertTriggered(**row) if row else None
        except psycopg.errors.UniqueViolation:
            return None  # Alert already triggered for this combination

    async def mark_notification_sent(self, triggered_id: UUID) -> bool:
        await self._session.execute(
            """
            UPDATE alerts_triggered
            SET notification_sent = TRUE
            WHERE id = %(triggered_id)s
            """,
            {"triggered_id": triggered_id},
        )
        return self._session.rowcount > 0

    async def get_pending_notifications(
        self, since: datetime | None = None
    ) -> list[AlertTriggered]:
        where_clause = "WHERE notification_sent = FALSE"
        params = {}

        if since:
            where_clause += " AND triggered_at >= %(since)s"
            params["since"] = since

        await self._session.execute(
            f"""
            SELECT * FROM alerts_triggered
            {where_clause}
            ORDER BY triggered_at
            """,
            params,
        )
        rows = await self._session.fetchall()
        return [AlertTriggered(**row) for row in rows]

    async def record_execution(
        self,
        alerts_checked: int,
        alerts_triggered: int,
        emails_sent: int,
        metadata: dict[str, Any] | None = None,
    ) -> AlertExecution:
        await self._session.execute(
            """
            INSERT INTO alert_executions (
                alerts_checked, alerts_triggered, emails_sent, metadata
            ) VALUES (
                %(alerts_checked)s, %(alerts_triggered)s, %(emails_sent)s, %(metadata)s
            )
            RETURNING *
            """,
            {
                "alerts_checked": alerts_checked,
                "alerts_triggered": alerts_triggered,
                "emails_sent": emails_sent,
                "metadata": Jsonb(metadata or {}),
            },
        )
        row = await self._session.fetchone()
        if not row:
            raise ValueError("Failed to record execution")

        return AlertExecution(**row)

    async def get_last_execution(self) -> AlertExecution | None:
        await self._session.execute(
            """
            SELECT * FROM alert_executions
            ORDER BY executed_at DESC
            LIMIT 1
            """
        )
        row = await self._session.fetchone()
        return AlertExecution(**row) if row else None

    async def check_narrative_stats_alerts(
        self, since: datetime | None = None
    ) -> list[tuple[Alert, UUID, int]]:
        """Check for narrative stats alerts that should be triggered.
        Returns list of (alert, narrative_id, current_value) tuples."""
        
        # For general alerts, check all narratives
        # For specific alerts, only check the specified narrative
        query = """
            WITH narrative_stats AS (
                SELECT 
                    n.id AS narrative_id,
                    SUM(COALESCE(v.views, 0)) AS total_views,
                    COUNT(DISTINCT cn.claim_id) AS claims_count,
                    COUNT(DISTINCT v.id) AS videos_count
                FROM narratives n
                LEFT JOIN claim_narratives cn ON n.id = cn.narrative_id
                LEFT JOIN video_claims c ON cn.claim_id = c.id
                LEFT JOIN videos v ON c.video_id = v.id
                WHERE TRUE
                GROUP BY n.id
            ),
            relevant_alerts AS (
                SELECT
                    a.id,
                    a.user_id,
                    a.organisation_id,
                    a.name,
                    a.alert_type,
                    a.scope,
                    a.narrative_id AS specific_narrative,
                    a.threshold,
                    a.topic_id,
                    a.keyword,
                    a.enabled,
                    a.metadata,
                    a.created_at,
                    a.updated_at
                FROM alerts a
                WHERE
                    a.enabled = TRUE
                    AND a.alert_type IN (
                        'narrative_views',
                        'narrative_claims_count',
                        'narrative_videos_count'
                    )
            )
            SELECT
                ra.*,
                ns.narrative_id,
                CASE ra.alert_type
                    WHEN 'narrative_views' THEN ns.total_views
                    WHEN 'narrative_claims_count' THEN ns.claims_count
                    WHEN 'narrative_videos_count' THEN ns.videos_count
                END AS current_value
            FROM relevant_alerts ra
            -- join general alerts to all narratives
            LEFT JOIN narrative_stats ns
                ON ra.scope = 'general'
            -- join specific alerts only to their narrative
                OR (ra.scope = 'specific' 
                    AND ra.specific_narrative = ns.narrative_id)
            WHERE
                CASE ra.alert_type
                    WHEN 'narrative_views' THEN ns.total_views
                    WHEN 'narrative_claims_count' THEN ns.claims_count
                    WHEN 'narrative_videos_count' THEN ns.videos_count
                END >= ra.threshold
        """
        
        await self._session.execute(query, {"since": since})
        rows = await self._session.fetchall()
        
        results = []
        for row in rows:
            alert = Alert(
                id=row["id"],
                user_id=row["user_id"],
                organisation_id=row["organisation_id"],
                name=row.get("name", "Unnamed Alert"),
                alert_type=row["alert_type"],
                scope=row["scope"],
                narrative_id=row["specific_narrative"],
                threshold=row["threshold"],
                topic_id=row["topic_id"],
                keyword=row["keyword"],
                enabled=row["enabled"],
                metadata=row["metadata"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            results.append((alert, row["narrative_id"], row["current_value"]))
        
        return results

    async def check_topic_alerts(
        self, since: datetime | None = None
    ) -> list[tuple[Alert, UUID]]:
        """Check for new narratives with tracked topics.
        Returns list of (alert, narrative_id) tuples."""
        
        query = """
            SELECT DISTINCT a.id, a.user_id, a.organisation_id, a.name, a.alert_type, 
                   a.scope, a.threshold, a.topic_id, a.keyword, a.enabled, 
                   a.metadata, a.created_at, a.updated_at, nt.narrative_id
            FROM alerts a
            JOIN narrative_topics nt ON a.topic_id = nt.topic_id
            JOIN narratives n ON nt.narrative_id = n.id
            WHERE a.enabled = TRUE
            AND a.alert_type = 'narrative_with_topic'
            AND (%(since)s IS NULL OR n.created_at >= %(since)s)
        """
        
        await self._session.execute(query, {"since": since})
        rows = await self._session.fetchall()
        
        results = []
        for row in rows:
            alert = Alert(
                id=row["id"],
                user_id=row["user_id"],
                organisation_id=row["organisation_id"],
                name=row.get("name", "Unnamed Alert"),
                alert_type=row["alert_type"],
                scope=row["scope"],
                narrative_id=None,
                threshold=row["threshold"],
                topic_id=row["topic_id"],
                keyword=row["keyword"],
                enabled=row["enabled"],
                metadata=row["metadata"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            results.append((alert, row["narrative_id"]))
        
        return results

    async def check_keyword_alerts(
        self, since: datetime | None = None
    ) -> list[tuple[Alert, UUID]]:
        """Check for narratives containing tracked keywords.
        Returns list of (alert, narrative_id) tuples."""
        
        query = """
            WITH recent_narratives AS (
                SELECT id, title, description 
                FROM narratives 
                WHERE (%(since)s IS NULL OR created_at >= %(since)s)
            )
            SELECT DISTINCT a.id, a.user_id, a.organisation_id, a.name, a.alert_type,
                   a.scope, a.threshold, a.topic_id, a.keyword, a.enabled,
                   a.metadata, a.created_at, a.updated_at, n.id as narrative_id
            FROM alerts a
            JOIN recent_narratives n ON (
                LOWER(n.title) LIKE LOWER('%%' || a.keyword || '%%')
                OR LOWER(n.description) LIKE LOWER('%%' || a.keyword || '%%')
            )
            WHERE a.enabled = TRUE
            AND a.alert_type = 'keyword'
        """
        
        await self._session.execute(query, {"since": since})
        rows = await self._session.fetchall()
        
        results = []
        for row in rows:
            alert = Alert(
                id=row["id"],
                user_id=row["user_id"],
                organisation_id=row["organisation_id"],
                name=row.get("name", "Unnamed Alert"),
                alert_type=row["alert_type"],
                scope=row["scope"],
                narrative_id=None,
                threshold=row["threshold"],
                topic_id=row["topic_id"],
                keyword=row["keyword"],
                enabled=row["enabled"],
                metadata=row["metadata"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            results.append((alert, row["narrative_id"]))
        
        return results
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import DictRow

from core.alerts.models import Alert, AlertExecution, AlertScope, AlertType
from core.alerts.repo import AlertRepository
from core.auth.models import Identity
from core.auth.repo import AuthRepository
from core.email import get_emailer
from core.email.messages import alerts_message
from core.errors import NotAuthorizedError, NotFoundError
from core.narratives.repo import NarrativeRepository


class AlertService:
    def __init__(
        self,
        connection_factory: Callable[[], AsyncConnection[DictRow]],
    ) -> None:
        self._connection_factory = connection_factory

    async def create_alert(
        self,
        identity: Identity,
        name: str,
        alert_type: AlertType,
        scope: AlertScope,
        narrative_id: UUID | None = None,
        threshold: int | None = None,
        topic_id: UUID | None = None,
        keyword: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Alert:
        if not identity.organisation:
            raise NotAuthorizedError("User must belong to an organisation")

        # Validate alert parameters
        if alert_type in [
            AlertType.NARRATIVE_VIEWS,
            AlertType.NARRATIVE_CLAIMS_COUNT,
            AlertType.NARRATIVE_VIDEOS_COUNT,
        ]:
            if threshold is None:
                raise ValueError(f"Threshold is required for {alert_type.value} alerts")

        if alert_type == AlertType.NARRATIVE_WITH_TOPIC and topic_id is None:
            raise ValueError("Topic ID is required for topic alerts")

        if alert_type == AlertType.KEYWORD and not keyword:
            raise ValueError("Keyword is required for keyword alerts")

        if scope == AlertScope.SPECIFIC and narrative_id is None:
            raise ValueError("Narrative ID is required for specific alerts")

        if scope == AlertScope.GENERAL and narrative_id is not None:
            raise ValueError("Narrative ID should not be provided for general alerts")

        async with self._connection_factory() as conn:
            async with conn.cursor() as cur:
                alert_repo = AlertRepository(cur)
                return await alert_repo.create_alert(
                    user_id=identity.user.id,
                    organisation_id=identity.organisation.id,
                    name=name,
                    alert_type=alert_type,
                    scope=scope,
                    narrative_id=narrative_id,
                    threshold=threshold,
                    topic_id=topic_id,
                    keyword=keyword,
                    metadata=metadata,
                )

    async def get_alert(self, alert_id: UUID, identity: Identity) -> Alert:
        async with self._connection_factory() as conn:
            async with conn.cursor() as cur:
                alert_repo = AlertRepository(cur)
                alert = await alert_repo.get_alert(alert_id)
                
                if not alert:
                    raise NotFoundError("Alert not found")

                if not identity.organisation or alert.organisation_id != identity.organisation.id:
                    raise NotAuthorizedError("Not authorized to view this alert")

                # If alert is for a specific narrative, include narrative details
                if alert.narrative_id:
                    narrative_repo = NarrativeRepository(cur)
                    narrative = await narrative_repo.get_narrative(alert.narrative_id)
                    if narrative:
                        alert.narrative = {
                            "id": str(narrative.id),
                            "title": narrative.title,
                            "description": narrative.description,
                            "created_at": narrative.created_at.isoformat() if narrative.created_at else None,
                        }

        return alert

    async def get_user_alerts(
        self,
        identity: Identity,
        enabled_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[Alert], int]:
        if not identity.organisation:
            raise NotAuthorizedError("User must belong to an organisation")

        async with self._connection_factory() as conn:
            async with conn.cursor() as cur:
                alert_repo = AlertRepository(cur)
                alerts, total = await alert_repo.get_user_alerts(
                    user_id=identity.user.id,
                    organisation_id=identity.organisation.id,
                    enabled_only=enabled_only,
                    limit=limit,
                    offset=offset,
                )
                
                # Add narrative details for specific alerts
                narrative_repo = NarrativeRepository(cur)
                narrative_ids = {alert.narrative_id for alert in alerts if alert.narrative_id}
                
                if narrative_ids:
                    narratives = {}
                    for narrative_id in narrative_ids:
                        narrative = await narrative_repo.get_narrative(narrative_id)
                        if narrative:
                            narratives[narrative_id] = {
                                "id": str(narrative.id),
                                "title": narrative.title,
                                "description": narrative.description,
                                "created_at": narrative.created_at.isoformat() if narrative.created_at else None,
                            }
                    
                    for alert in alerts:
                        if alert.narrative_id and alert.narrative_id in narratives:
                            alert.narrative = narratives[alert.narrative_id]
                
                return alerts, total

    async def update_alert(
        self,
        alert_id: UUID,
        identity: Identity,
        name: str | None = None,
        enabled: bool | None = None,
        threshold: int | None = None,
        keyword: str | None = None,
    ) -> Alert:
        alert = await self.get_alert(alert_id, identity)

        async with self._connection_factory() as conn:
            async with conn.cursor() as cur:
                alert_repo = AlertRepository(cur)
                updated = await alert_repo.update_alert(
                    alert_id=alert_id,
                    name=name,
                    enabled=enabled,
                    threshold=threshold,
                    keyword=keyword,
                )
                
        if not updated:
            raise NotFoundError("Alert not found")

        return updated

    async def delete_alert(self, alert_id: UUID, identity: Identity) -> bool:
        await self.get_alert(alert_id, identity)

        async with self._connection_factory() as conn:
            async with conn.cursor() as cur:
                alert_repo = AlertRepository(cur)
                return await alert_repo.delete_alert(alert_id)

    async def process_alerts(self) -> AlertExecution:
        """Main method to process all alerts - called by CLI command."""
        
        async with self._connection_factory() as conn:
            async with conn.cursor() as cur:
                alert_repo = AlertRepository(cur)
                auth_repo = AuthRepository(cur)
                narrative_repo = NarrativeRepository(cur)
                
                # Get last execution time
                last_execution = await alert_repo.get_last_execution()
                since = last_execution.executed_at if last_execution else datetime.now(timezone.utc) - timedelta(hours=1)

                alerts_checked = 0
                alerts_triggered = 0
                triggered_alerts = []

                # Check narrative stats alerts (views, claims count, videos count)
                stats_alerts = await alert_repo.check_narrative_stats_alerts(since)
                for alert, narrative_id, current_value in stats_alerts:
                    alerts_checked += 1
                    
                    # Record the trigger (will return None if already triggered)
                    # Use the threshold as the unique key, not the actual value
                    triggered = await alert_repo.record_alert_trigger(
                        alert_id=alert.id,
                        narrative_id=narrative_id,
                        trigger_value=current_value,  # Store actual value for logging
                        threshold_crossed=alert.threshold,  # Use threshold for uniqueness
                        metadata={"alert_type": alert.alert_type.value},
                    )
                    
                    if triggered:
                        alerts_triggered += 1
                        triggered_alerts.append((alert, triggered, narrative_id))

                # Check topic alerts
                topic_alerts = await alert_repo.check_topic_alerts(since)
                for alert, narrative_id in topic_alerts:
                    alerts_checked += 1
                    
                    # For topic alerts, we use narrative_id as the unique identifier
                    # This ensures one alert per narrative-topic combination
                    triggered = await alert_repo.record_alert_trigger(
                        alert_id=alert.id,
                        narrative_id=narrative_id,
                        trigger_value=None,
                        threshold_crossed=None,  # No threshold for topic alerts
                        metadata={"alert_type": "topic", "topic_id": str(alert.topic_id)},
                    )
                    
                    if triggered:
                        alerts_triggered += 1
                        triggered_alerts.append((alert, triggered, narrative_id))

                # Check keyword alerts
                keyword_alerts = await alert_repo.check_keyword_alerts(since)
                for alert, narrative_id in keyword_alerts:
                    alerts_checked += 1
                    
                    # For keyword alerts, we use narrative_id as the unique identifier
                    # This ensures one alert per narrative-keyword combination
                    triggered = await alert_repo.record_alert_trigger(
                        alert_id=alert.id,
                        narrative_id=narrative_id,
                        trigger_value=None,
                        threshold_crossed=None,  # No threshold for keyword alerts
                        metadata={"alert_type": "keyword", "keyword": alert.keyword},
                    )
                    
                    if triggered:
                        alerts_triggered += 1
                        triggered_alerts.append((alert, triggered, narrative_id))

                # Send notifications
                emails_sent = await self._send_alert_notifications(
                    triggered_alerts, alert_repo, auth_repo, narrative_repo
                )

                # Record execution
                execution = await alert_repo.record_execution(
                    alerts_checked=alerts_checked,
                    alerts_triggered=alerts_triggered,
                    emails_sent=emails_sent,
                    metadata={
                        "since": since.isoformat(),
                        "completed_at": datetime.now(timezone.utc).isoformat(),
                    },
                )

                return execution

    async def _send_alert_notifications(
        self, 
        triggered_alerts: list[tuple[Alert, Any, UUID]],
        alert_repo: AlertRepository,
        auth_repo: AuthRepository,
        narrative_repo: NarrativeRepository,
    ) -> int:
        """Send email notifications for triggered alerts."""
        
        user_alerts = defaultdict(list)
        
        for alert, triggered, narrative_id in triggered_alerts:
            key = (alert.user_id, alert.organisation_id)
            user_alerts[key].append((alert, triggered, narrative_id))

        emails_sent = 0
        
        for (user_id, org_id), alerts in user_alerts.items():
            user = await auth_repo.get_user_by_id(user_id)
            org = await auth_repo.get_organisation_by_id(org_id)
            
            if not user or not org:
                continue

            alert_details = []
            for alert, triggered, narrative_id in alerts:
                narrative = await narrative_repo.get_narrative(narrative_id)
                if narrative:
                    alert_details.append({
                        "alert_type": alert.alert_type.value,
                        "narrative_title": narrative.title,
                        "narrative_id": str(narrative_id),
                        "trigger_value": triggered.trigger_value,
                        "threshold": alert.threshold,
                        "keyword": alert.keyword,
                        "triggered_at": triggered.triggered_at,
                    })

            if alert_details:
                subject, body = alerts_message(
                    organisation_name=org.display_name,
                    alerts=alert_details,
                    locale=org.language,
                )
                
                # Get emailer and send immediately (in CLI context)
                # In API context, this could be done with BackgroundTask
                emailer = await get_emailer()
                emailer.send(
                    to=user.email,
                    subject=subject,
                    html=body,
                )
                
                emails_sent += 1
                
                for _, triggered, _ in alerts:
                    await alert_repo.mark_notification_sent(triggered.id)

        return emails_sent
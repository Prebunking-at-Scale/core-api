from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import DictRow

from core.alerts.models import Alert, AlertExecution, AlertScope, AlertType, ChannelType
from core.alerts.repo import AlertRepository
from core.auth.models import Identity
from core.auth.repo import AuthRepository
from core.email import get_emailer
from core.email.messages import alerts_message
from core.errors import NotAuthorizedError, NotFoundError
from core.narratives.repo import NarrativeRepository
from core.config import APP_BASE_URL

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
        channels: list[dict[str, Any]] | None = None,
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
                    channels=channels,
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
                return await alert_repo.get_user_alerts(
                    user_id=identity.user.id,
                    organisation_id=identity.organisation.id,
                    enabled_only=enabled_only,
                    limit=limit,
                    offset=offset,
                )

    async def update_alert(
        self,
        alert_id: UUID,
        identity: Identity,
        name: str | None = None,
        enabled: bool | None = None,
        threshold: int | None = None,
        keyword: str | None = None,
        channels: list[dict[str, Any]] | None = None,
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
                    channels=channels,
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

                stats_alerts = await alert_repo.check_narrative_stats_alerts(since)
                for alert, narrative_id, current_value in stats_alerts:
                    alerts_checked += 1
                    
                    triggered = await alert_repo.record_alert_trigger(
                        alert_id=alert.id,
                        narrative_id=narrative_id,
                        trigger_value=current_value, 
                        threshold_crossed=alert.threshold, 
                        metadata={"alert_type": alert.alert_type.value},
                    )
                    
                    if triggered:
                        alerts_triggered += 1
                        triggered_alerts.append((alert, triggered, narrative_id))

                topic_alerts = await alert_repo.check_topic_alerts(since)
                for alert, narrative_id in topic_alerts:
                    alerts_checked += 1
                    
                    triggered = await alert_repo.record_alert_trigger(
                        alert_id=alert.id,
                        narrative_id=narrative_id,
                        trigger_value=None,
                        threshold_crossed=None,
                        metadata={"alert_type": "topic", "topic_id": str(alert.topic_id)},
                    )
                    
                    if triggered:
                        alerts_triggered += 1
                        triggered_alerts.append((alert, triggered, narrative_id))

                keyword_alerts = await alert_repo.check_keyword_alerts(since)
                for alert, narrative_id in keyword_alerts:
                    alerts_checked += 1
                    
                    triggered = await alert_repo.record_alert_trigger(
                        alert_id=alert.id,
                        narrative_id=narrative_id,
                        trigger_value=None,
                        threshold_crossed=None,
                        metadata={"alert_type": "keyword", "keyword": alert.keyword},
                    )
                    
                    if triggered:
                        alerts_triggered += 1
                        triggered_alerts.append((alert, triggered, narrative_id))

                emails_sent, slack_messages_sent = await self._send_alert_notifications(
                    triggered_alerts, alert_repo, auth_repo, narrative_repo
                )
                
                notifications_sent = emails_sent + slack_messages_sent

                execution = await alert_repo.record_execution(
                    alerts_checked=alerts_checked,
                    alerts_triggered=alerts_triggered,
                    notifications_sent=notifications_sent,
                    metadata={
                        "since": since.isoformat(),
                        "completed_at": datetime.now(timezone.utc).isoformat(),
                        "emails_sent": emails_sent,
                        "slack_messages_sent": slack_messages_sent,
                    },
                )

                return execution

    async def _send_alert_notifications(
        self, 
        triggered_alerts: list[tuple[Alert, Any, UUID]],
        alert_repo: AlertRepository,
        auth_repo: AuthRepository,
        narrative_repo: NarrativeRepository,
    ) -> tuple[int, int]:
        """Send notifications for triggered alerts via configured channels (email and/or Slack).
        
        Returns:
            Tuple of (emails_sent, slack_messages_sent)
        """
        
        # Group alerts by (user_id, org_id)
        user_alerts = defaultdict(list)
        
        for alert, triggered, narrative_id in triggered_alerts:
            key = (alert.user_id, alert.organisation_id)
            user_alerts[key].append((alert, triggered, narrative_id))

        emails_sent = 0
        slack_messages_sent = 0
        
        for (user_id, org_id), alerts in user_alerts.items():
            # Separate alerts by channel type
            email_alerts = [a for a in alerts if a[0].has_email_channel]
            slack_alerts = [a for a in alerts if a[0].has_slack_channel]
            
            # Send email notifications
            if email_alerts:
                sent = await self._send_email_notification(
                    user_id, org_id, email_alerts, alert_repo, auth_repo, narrative_repo
                )
                emails_sent += sent
            
            # Send Slack notifications
            if slack_alerts:
                sent = await self._send_slack_notification(
                    org_id, slack_alerts, alert_repo, narrative_repo
                )
                slack_messages_sent += sent

        return emails_sent, slack_messages_sent
    
    async def _send_email_notification(
        self,
        user_id: UUID,
        org_id: UUID,
        alerts: list[tuple[Alert, Any, UUID]],
        alert_repo: AlertRepository,
        auth_repo: AuthRepository,
        narrative_repo: NarrativeRepository,
    ) -> int:
        """Send email notification for a group of alerts."""
        user = await auth_repo.get_user_by_id(user_id)
        org = await auth_repo.get_organisation(org_id)
        
        if not user or not org:
            return 0

        alert_details = []
        for alert, triggered, narrative_id in alerts:
            narrative = await narrative_repo.get_narrative(narrative_id)
            if narrative:
                alert_details.append({
                    "alert_name": alert.name,
                    "alert_type": alert.alert_type.value,
                    "narrative_title": narrative.title,
                    "narrative_id": str(narrative_id),
                    "trigger_value": triggered.trigger_value,
                    "threshold": alert.threshold,
                    "keyword": alert.keyword,
                    "triggered_at": triggered.triggered_at,
                })

        if not alert_details:
            return 0

        subject, body = alerts_message(
            organisation_name=org.display_name,
            alerts=alert_details,
            locale=org.language,
        )
        
        try:
            emailer = await get_emailer()
            emailer.send(
                to=user.email,
                subject=subject,
                html=body,
            )
            
            # Mark email delivery as successful
            for _, triggered, _ in alerts:
                await alert_repo.record_channel_delivery(
                    triggered.id, ChannelType.EMAIL.value, "sent"
                )
                # Also mark legacy field for backward compatibility
                await alert_repo.mark_notification_sent(triggered.id)
            
            return 1
        except Exception as e:
            print(f"Warning: Failed to send email to {user.email}: {e}")
            
            # Mark email delivery as failed
            for _, triggered, _ in alerts:
                await alert_repo.record_channel_delivery(
                    triggered.id, ChannelType.EMAIL.value, "failed"
                )
            
            return 0
    
    async def _send_slack_notification(
        self,
        org_id: UUID,
        alerts: list[tuple[Alert, Any, UUID]],
        alert_repo: AlertRepository,
        narrative_repo: NarrativeRepository,
    ) -> int:
        """Send Slack notifications for a group of alerts.
        
        Returns:
            Number of Slack messages successfully sent
        """
        from core.integrations.slack.service import SlackService
        
        # Get Slack installations for this organization
        slack_service = SlackService(self._connection_factory)
        installations = await slack_service.get_installations_by_organisation(org_id)
        
        if not installations:
            # No Slack configured for this organization
            for alert, triggered, _ in alerts:
                await alert_repo.record_channel_delivery(
                    triggered.id, ChannelType.SLACK.value, "skipped"
                )
            return 0
        
        # Group alerts by slack_channel_id
        alerts_by_channel = defaultdict(list)
        for alert, triggered, narrative_id in alerts:
            for channel_id in alert.slack_channel_ids:
                alerts_by_channel[channel_id].append((alert, triggered, narrative_id))
        
        messages_sent = 0
        
        # Send to each configured Slack channel
        for channel_id, channel_alerts in alerts_by_channel.items():
            # Format message for this group of alerts
            message = await self._format_slack_alert_message(
                channel_alerts, narrative_repo
            )
            
            try:
                # Send message using channel_id directly
                await slack_service.send_message_to_slack(
                    organisation_id=org_id,
                    channel=channel_id,
                    text=message
                )
                
                messages_sent += 1
                
                # Mark as sent
                for _, triggered, _ in channel_alerts:
                    await alert_repo.record_channel_delivery(
                        triggered.id, ChannelType.SLACK.value, "sent"
                    )
                    
            except Exception as e:
                print(f"Warning: Failed to send Slack notification to channel {channel_id}: {e}")
                # Mark as failed
                for _, triggered, _ in channel_alerts:
                    await alert_repo.record_channel_delivery(
                        triggered.id, ChannelType.SLACK.value, "failed"
                    )
        
        return messages_sent
    
    async def _format_slack_alert_message(
        self,
        alerts: list[tuple[Alert, Any, UUID]],
        narrative_repo: NarrativeRepository,
    ) -> str:
        """Format alerts into a Slack-friendly message (Markdown)."""
        
        lines = ["🔔 *Alert Notifications*\n"]
        
        for alert, triggered, narrative_id in alerts:
            narrative = await narrative_repo.get_narrative(narrative_id)
            if not narrative:
                continue
            
            alert_name = alert.name
            alert_type = alert.alert_type.value
            narrative_title = narrative.title
            
            # Build description based on alert type (similar to email)
            if alert_type == "narrative_views":
                description = f"Narrative \"{narrative_title}\" reached {triggered.trigger_value:,} views (threshold: {alert.threshold:,})"
            elif alert_type == "narrative_claims_count":
                description = f"Narrative \"{narrative_title}\" reached {triggered.trigger_value} claims (threshold: {alert.threshold})"
            elif alert_type == "narrative_videos_count":
                description = f"Narrative \"{narrative_title}\" reached {triggered.trigger_value} videos (threshold: {alert.threshold})"
            elif alert_type == "narrative_with_topic":
                description = f"New narrative \"{narrative_title}\" created with tracked topic"
            elif alert_type == "keyword":
                description = f"Narrative \"{narrative_title}\" contains keyword \"{alert.keyword}\""
            else:
                description = f"Alert triggered for narrative \"{narrative_title}\""
            
            # Format alert item
            lines.append(f"*{alert_name}*")
            lines.append(f"_{alert_type.replace('_', ' ').title()}_")
            lines.append(f"{description}")
            
            # Add link to narrative
            narrative_url = f"{APP_BASE_URL}/narratives/{narrative_id}"
            lines.append(f"<{narrative_url}|View Narrative →>")
            lines.append("")  # Empty line between alerts
        
        return "\n".join(lines)
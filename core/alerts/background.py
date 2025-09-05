"""Background tasks for the alert system."""

import logging
from typing import Any

from core.email import Emailer

log = logging.getLogger(__name__)


async def send_alert_email(
    emailer: Emailer,
    to: str,
    subject: str,
    body: str,
    alert_ids: list[str],
) -> None:
    """Send alert email as a background task.
    
    Args:
        emailer: The email service
        to: Recipient email address
        subject: Email subject
        body: HTML email body
        alert_ids: List of alert IDs for logging
    """
    
    emailer.send(to, subject, body)
    
    log.info(f"Sent alert email to {to} for alerts: {alert_ids}")
import i18n

from core import config

button_style = (
    "background: #00533D; color: #ffffff; border-radius: 6px; display: block;"
    "margin: 24px auto 0 auto; padding: 12px 24px; font-weight: 600;"
    "text-decoration: none; font-size: 1em;"
)

container_style = (
    "background: #f1f1f1; color: #333; border-radius: 16px; margin: 24px auto; "
    "padding: 24px; max-width: 500px; font-family: Archivo, Arial, ui-sans-serif, "
    "system-ui, sans-serif; font-size: 1.2em; text-align: center;"
)


def invite_message(organisation_name: str, token: str, locale: str) -> tuple[str, str]:
    subject = i18n.t(
        "email.invite.subject",
        locale=locale,
        organisation_name=organisation_name,
    )

    body = f"""
    <div style="{container_style}">
        <h1 style="color: #333;">{subject}</h1>
        <p style="margin: 2em">{i18n.t("email.invite.message", locale=locale, organisation_name=organisation_name)}</p>
        <p style="margin: 2em">{i18n.t("email.invite.description", locale=locale)}</p>
        <p>
            <a href="{config.APP_BASE_URL}/invitation?token={token}" style="{button_style}">
                {i18n.t("email.invite.accept_invite", locale=locale)}
            </a>
        </p>
    <div>
    """

    return subject, body


def password_reset_message(token: str, locale: str) -> tuple[str, str]:
    subject = i18n.t("email.password_reset.subject", locale=locale)

    body = f"""
    <div style="{container_style}">
        <h1 style="color: #333;">{subject}</h1>
        <p style="margin: 2em">{i18n.t("email.password_reset.message", locale=locale)}</p>
        <p style="margin: 2em">{i18n.t("email.password_reset.not_you", locale=locale)}</p>
        <p style="margin: 2em">{i18n.t("email.password_reset.reset_message", locale=locale)}</p>
        <p>
            <a href="{config.APP_BASE_URL}/password-reset?token={token}" style="{button_style}">
                {i18n.t("email.password_reset.reset_link", locale=locale)}
            </a>
        </p>
    <div>
    """

    return subject, body


def alerts_message(
    organisation_name: str, alerts: list[dict], locale: str
) -> tuple[str, str]:
    subject = f"Alert Summary for {organisation_name}"
    
    alert_items_html = ""
    for alert in alerts:
        alert_type = alert["alert_type"]
        narrative_title = alert["narrative_title"]
        
        if alert_type == "narrative_views":
            description = f"Narrative '{narrative_title}' reached {alert['trigger_value']:,} views (threshold: {alert['threshold']:,})"
        elif alert_type == "narrative_claims_count":
            description = f"Narrative '{narrative_title}' reached {alert['trigger_value']} claims (threshold: {alert['threshold']})"
        elif alert_type == "narrative_videos_count":
            description = f"Narrative '{narrative_title}' reached {alert['trigger_value']} videos (threshold: {alert['threshold']})"
        elif alert_type == "narrative_with_topic":
            description = f"New narrative '{narrative_title}' created with tracked topic"
        elif alert_type == "keyword":
            description = f"Narrative '{narrative_title}' contains keyword '{alert['keyword']}'"
        else:
            description = f"Alert triggered for narrative '{narrative_title}'"
        
        alert_items_html += f"""
        <div style="background: white; border-left: 4px solid #00533D; margin: 1em 0; padding: 1em; text-align: left;">
            <p style="margin: 0; font-weight: bold; color: #00533D;">{alert_type.replace('_', ' ').title()}</p>
            <p style="margin: 0.5em 0 0 0; color: #666;">{description}</p>
            <p style="margin: 0.5em 0 0 0;">
                <a href="{config.APP_BASE_URL}/narratives/{alert['narrative_id']}" style="color: #00533D; text-decoration: underline;">
                    View Narrative →
                </a>
            </p>
        </div>
        """
    
    body = f"""
    <div style="{container_style}">
        <h1 style="color: #333;">Alert Summary</h1>
        <p style="margin: 1em 0; color: #666;">The following alerts have been triggered for {organisation_name}:</p>
        <div style="margin: 2em 0;">
            {alert_items_html}
        </div>
        <p style="margin: 2em 0 0 0; color: #999; font-size: 0.9em;">
            To manage your alerts, visit your dashboard settings.
        </p>
    </div>
    """
    
    return subject, body

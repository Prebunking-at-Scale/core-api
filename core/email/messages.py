from html import escape
from typing import TYPE_CHECKING

import i18n

from core import config

if TYPE_CHECKING:
    from core.alerts.models import DigestEntry

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


def magic_link_message(token: str, locale: str) -> tuple[str, str]:
    subject = i18n.t("email.magic_link.subject", locale=locale)
    expiry_minutes = int(config.MAGIC_LINK_TTL.total_seconds() / 60)

    body = f"""
    <div style="{container_style}">
        <h1 style="color: #333;">{subject}</h1>
        <p style="margin: 2em">{i18n.t("email.magic_link.message", locale=locale)}</p>
        <p style="margin: 2em">{i18n.t("email.magic_link.not_you", locale=locale)}</p>
        <p>
            <a href="{config.APP_BASE_URL}/magic-login?token={token}" style="{button_style}">
                {i18n.t("email.magic_link.login_link", locale=locale)}
            </a>
        </p>
        <p style="margin: 2em; font-size: 0.9em; color: #666;">
            {i18n.t("email.magic_link.expires", locale=locale, minutes=expiry_minutes)}
        </p>
    <div>
    """

    return subject, body


def alert_digest_message(entries: "list[DigestEntry]", summaries: dict) -> tuple[str, str]:
    """The daily alerts e-mail (frontend docs/alerts.md, "Daily digest"), in English:
    per triggered alert, new narratives, new claims in each followed narrative, then
    new claims; each title in bold with the conditions it met, 5 per section.
    `summaries[alert_id]` holds each condition's summary, by position."""
    from core.alerts.models import DigestSection

    count = len(entries)
    subject = f"{count} alert triggered" if count == 1 else f"{count} alerts triggered"
    section_title = "margin: 1.2em 0 0.3em 0; font-size: 1em; color: #1F2937;"
    item_style = "margin: 0.25em 0; color: #1F2937; line-height: 1.4;"

    def title_html(item) -> str:
        title = f"<strong>{escape(item.title)}</strong>"
        if not item.link:
            return title
        return (
            f'<a href="{escape(config.APP_BASE_URL + item.link)}" '
            f'style="color: #1F2937; text-decoration: underline;">{title}</a>'
        )

    def items_html(entry, section: DigestSection, indent: bool = False) -> str:
        out = ""
        for item in section.items:
            met = " or ".join(summaries[entry.alert_id][n - 1] for n in item.conditions)
            out += (
                f'<p style="{item_style}{" padding-left: 1em;" if indent else ""}">'
                f"&bull; {title_html(item)} "
                f'<span style="color: #6B7280;">({escape(met)})</span></p>'
            )
        if section.total > len(section.items):
            out += (
                f'<p style="margin: 0.4em 0;{" padding-left: 1em;" if indent else ""}">'
                f'<a href="{config.APP_BASE_URL}/alerts/{entry.alert_id}" '
                f'style="color: #00533D; text-decoration: underline;">See all ({section.total})</a></p>'
            )
        return out

    blocks = ""
    for entry in entries:
        block = (
            '<h2 style="margin: 0; padding-bottom: 0.3em; border-bottom: 2px solid #00533D; '
            f'color: #1F2937; font-size: 1.25em;">Alert triggered: {escape(entry.alert_name)}</h2>'
        )
        if entry.narratives.total:
            block += f'<h3 style="{section_title}">New narratives</h3>' + items_html(entry, entry.narratives)
        if entry.in_narratives:
            block += f'<h3 style="{section_title}">New claims in selected narratives</h3>'
            for group in entry.in_narratives:
                block += (
                    f'<p style="margin: 0.5em 0 0.2em 0; color: #374151;">In &ldquo;{escape(group.narrative_title)}&rdquo;</p>'
                    + items_html(entry, group, indent=True)
                )
        if entry.claims.total:
            block += f'<h3 style="{section_title}">New claims</h3>' + items_html(entry, entry.claims)
        blocks += f'<div style="background: white; border-radius: 8px; margin: 1em 0; padding: 1em 1.2em;">{block}</div>'

    body = f"""
    <div style="{container_style} max-width: 640px; text-align: left; font-size: 1em;">
        {blocks}
        <p style="margin: 1.5em 0 0 0; color: #999; font-size: 0.85em; text-align: center;">
            <a href="{config.APP_BASE_URL}/alerts" style="color: #999;">Manage your alerts</a>
        </p>
    </div>
    """
    return subject, body

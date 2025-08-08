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

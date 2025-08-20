from typing import Protocol, runtime_checkable

from core import config
from core.email import smtp
from core.email.print import PrinterEmailer


@runtime_checkable
class Emailer(Protocol):
    def send(self, to: str, subject: str, html: str) -> None: ...


async def get_emailer() -> Emailer:
    if config.SMTP_HOST:
        return smtp.SMTPEmailer(
            config.SMTP_HOST,
            int(config.SMTP_PORT),
            config.SMTP_USERNAME,
            config.SMTP_PASSWORD,
            config.EMAIL_FROM,
        )
    return PrinterEmailer(config.EMAIL_FROM)

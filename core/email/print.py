from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class PrinterEmailer:
    """used for testing and local development"""

    def __init__(self, email_from: str):
        self._email_from = email_from

    def send(self, to: str, subject: str, html: str) -> None:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self._email_from
        msg["To"] = to
        msg.attach(MIMEText(html, "html"))

        print(msg.as_string())

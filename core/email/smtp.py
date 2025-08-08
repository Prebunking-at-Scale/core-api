import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr


class SMTPEmailer:
    def __init__(
        self, host: str, port: int, username: str, password: str, email_from: str
    ):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._email_from = email_from

    def send(self, to: str, subject: str, html: str) -> None:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = formataddr(("Prebunking at Scale", "auto@fullfact.org"))
        msg["To"] = to
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL(self._host, self._port) as s:
            s.login(self._username, self._password)
            errs = s.sendmail(self._email_from, [to], msg.as_string())
            if errs:
                raise Exception(errs)

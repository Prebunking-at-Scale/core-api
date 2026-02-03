import httpx


class MailgunEmailer:
    def __init__(self, domain: str, api_key: str, email_from: str):
        self._domain = domain
        self._api_key = api_key
        self._email_from = email_from
        self._base_url = f"https://api.eu.mailgun.net/v3/{domain}/messages"

    def send(self, to: str, subject: str, html: str) -> None:
        response = httpx.post(
            self._base_url,
            auth=("api", self._api_key),
            data={
                "from": f"Prebunking at Scale <{self._email_from}>",
                "to": to,
                "subject": subject,
                "html": html,
            },
        )
        response.raise_for_status()

from unittest.mock import MagicMock, patch

import httpx
from pytest import raises

from core.email.mailgun import MailgunEmailer


def test_mailgun_emailer_send_success() -> None:
    emailer = MailgunEmailer(
        domain="test.mailgun.org",
        api_key="test-api-key",
        email_from="test@example.com",
    )

    with patch("httpx.post") as mock_post:
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        emailer.send(
            to="recipient@example.com",
            subject="Test Subject",
            html="<p>Test content</p>",
        )

        mock_post.assert_called_once_with(
            "https://api.eu.mailgun.net/v3/test.mailgun.org/messages",
            auth=("api", "test-api-key"),
            data={
                "from": "Prebunking at Scale <test@example.com>",
                "to": "recipient@example.com",
                "subject": "Test Subject",
                "html": "<p>Test content</p>",
            },
        )
        mock_response.raise_for_status.assert_called_once()


def test_mailgun_emailer_send_failure() -> None:
    emailer = MailgunEmailer(
        domain="test.mailgun.org",
        api_key="test-api-key",
        email_from="test@example.com",
    )

    with patch("httpx.post") as mock_post:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unauthorized",
            request=MagicMock(),
            response=MagicMock(status_code=401),
        )
        mock_post.return_value = mock_response

        with raises(httpx.HTTPStatusError):
            emailer.send(
                to="recipient@example.com",
                subject="Test Subject",
                html="<p>Test content</p>",
            )

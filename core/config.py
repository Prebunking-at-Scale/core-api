import json
import os
from datetime import timedelta

import i18n
from dotenv import load_dotenv

load_dotenv()

DEV_MODE = os.environ.get("DEVELOPMENT_MODE", "prod") == "dev"

"""general settings"""
# The URL to prefix on external linkings pointing at the service. If running locally
# this is likely http://localhost:8000. When deployed, this could be e.g.
# https://pas.fullfact.org
APP_BASE_URL = os.environ.get("APP_BASE_URL", "")

# The name of the bucket used to store video content
VIDEO_STORAGE_BUCKET_NAME = os.environ.get("VIDEO_STORAGE_BUCKET_NAME", "")

"""database settings"""
DB_HOST = os.environ.get("DATABASE_HOST", "")
DB_PORT = os.environ.get("DATABASE_PORT", "")
DB_USER = os.environ.get("DATABASE_USER", "")
DB_PASSWORD = os.environ.get("DATABASE_PASSWORD", "")
DB_NAME = os.environ.get("DATABASE_NAME", "")

"""auth settings"""
VALID_API_KEYS = json.loads(os.environ.get("API_KEYS", "[]"))
JWT_SECRET = os.environ["JWT_SECRET"]

# how long a login token should last before expiring
AUTH_TOKEN_TTL = timedelta(days=30)
# how long an invite should last before expiring (if not accepted)
INVITE_TTL = timedelta(days=7)
# how long a password reset token should last before expiring
PASSWORD_RESET_TTL = timedelta(minutes=30)
# how long a magic link token should last before expiring
MAGIC_LINK_TTL = timedelta(minutes=15)

"""email settings"""
EMAIL_FROM = os.environ.get("EMAIL_FROM", "no-reply@mail.prebunking.efcsn.com")
MAILGUN_DOMAIN = os.environ.get("MAILGUN_DOMAIN", "")
MAILGUN_API_KEY = os.environ.get("MAILGUN_API_KEY", "")

SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = os.environ.get("SMTP_PORT", "")
SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")

"""narrative API settings"""
NARRATIVES_BASE_URL = os.environ.get("NARRATIVES_BASE_ENDPOINT")
NARRATIVES_API_KEY = os.environ.get("NARRATIVES_API_KEY")

"""graph-events dispatcher settings"""
# How often the dispatcher polls the outbox when it has work left to do.
# Kept short so that the lag between a Postgres mutation and Neo4j catching
# up stays sub-second under normal load.
GRAPH_EVENTS_POLL_INTERVAL_SEC = float(
    os.environ.get("GRAPH_EVENTS_POLL_INTERVAL_SEC", "1.5")
)
# How many pending events the dispatcher claims per cycle. Bigger batches
# reduce DB round-trips; smaller ones keep failure blast radius narrow.
GRAPH_EVENTS_BATCH_SIZE = int(os.environ.get("GRAPH_EVENTS_BATCH_SIZE", "50"))
# Exponential-backoff base for failed dispatches.
GRAPH_EVENTS_BACKOFF_BASE_SEC = float(
    os.environ.get("GRAPH_EVENTS_BACKOFF_BASE_SEC", "5.0")
)
GRAPH_EVENTS_BACKOFF_MAX_SEC = float(
    os.environ.get("GRAPH_EVENTS_BACKOFF_MAX_SEC", "300.0")
)
# After this many failed attempts the event stops being retried; it stays
# in the table with processed_at=NULL so the drift detector / runbook can
# pick it up.
GRAPH_EVENTS_MAX_ATTEMPTS = int(os.environ.get("GRAPH_EVENTS_MAX_ATTEMPTS", "12"))

"""internationalisation"""
i18n.set("file_format", "json")
i18n.set("filename_format", "{locale}.{format}")
i18n.set("skip_locale_root_data", True)
i18n.set("fallback", "en")
i18n.set("enable_memoization", True)

locales_path = "./core/i18n/locales"
i18n.set("load_path", [locales_path])

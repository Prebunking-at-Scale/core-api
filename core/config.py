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

"""narrative virality / analysis indicator tuning

Weights and thresholds for the virality and acceleration indicators. Read from the
environment so a deployment can retune them without a code change; the defaults are
the production values. Composite and acceleration weights must each sum to 1.
"""
# Composite virality score weights (must sum to 1)
COMPOSITE_ENGAGEMENT_WEIGHT = float(os.environ.get("COMPOSITE_ENGAGEMENT_WEIGHT", "0.50"))
COMPOSITE_REACH_WEIGHT = float(os.environ.get("COMPOSITE_REACH_WEIGHT", "0.30"))
COMPOSITE_VELOCITY_WEIGHT = float(os.environ.get("COMPOSITE_VELOCITY_WEIGHT", "0.20"))

# Acceleration rate score weights (must sum to 1)
ACCELERATION_ENGAGEMENT_WEIGHT = float(os.environ.get("ACCELERATION_ENGAGEMENT_WEIGHT", "0.40"))
ACCELERATION_VIDEO_VOLUME_WEIGHT = float(os.environ.get("ACCELERATION_VIDEO_VOLUME_WEIGHT", "0.35"))
ACCELERATION_VIEWS_WEIGHT = float(os.environ.get("ACCELERATION_VIEWS_WEIGHT", "0.25"))

# Hard cap on individual change_* components inside acceleration_rate.
# Without it, a single video going from 1 → 10k views (change=9999) drowns
# the weighted sum and makes the per-dimension weights meaningless.
ACCELERATION_CHANGE_CAP = float(os.environ.get("ACCELERATION_CHANGE_CAP", "5.0"))

# Alert-level percentile thresholds. Both indicators are classified by their
# PERCENT_RANK within the run's cohort, never by their raw values, so each threshold
# means a knowable fraction of that cohort. See
# NarrativeService.update_narrative_alert_levels for the classification.
COMPOSITE_PERCENTILE_VIRAL = float(os.environ.get("COMPOSITE_PERCENTILE_VIRAL", "0.95"))
COMPOSITE_PERCENTILE_EARLY_SURGE_MAX = float(os.environ.get("COMPOSITE_PERCENTILE_EARLY_SURGE_MAX", "0.50"))
COMPOSITE_PERCENTILE_WATCH_MIN = float(os.environ.get("COMPOSITE_PERCENTILE_WATCH_MIN", "0.70"))
ACCELERATION_PERCENTILE_SURGE = float(os.environ.get("ACCELERATION_PERCENTILE_SURGE", "0.95"))
ACCELERATION_PERCENTILE_WATCH_MIN = float(os.environ.get("ACCELERATION_PERCENTILE_WATCH_MIN", "0.70"))

"""internationalisation"""
i18n.set("file_format", "json")
i18n.set("filename_format", "{locale}.{format}")
i18n.set("skip_locale_root_data", True)
i18n.set("fallback", "en")
i18n.set("enable_memoization", True)

locales_path = "./core/i18n/locales"
i18n.set("load_path", [locales_path])

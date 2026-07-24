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
# Raw virality score parameters. These shape the two state signals themselves, before
# any ranking. They live here rather than in the service because both the service and
# the bulk cohort query in the repository need them, and a formula that exists twice
# eventually disagrees with itself.
VIRALITY_SCORE_LIKES_WEIGHT = int(os.environ.get("VIRALITY_SCORE_LIKES_WEIGHT", "1"))
VIRALITY_SCORE_COMMENTS_WEIGHT = int(os.environ.get("VIRALITY_SCORE_COMMENTS_WEIGHT", "5"))
VIRALITY_SCORE_REACH_CAP_LIMIT = int(os.environ.get("VIRALITY_SCORE_REACH_CAP_LIMIT", "10"))

# Composite virality score weights (must sum to 1).
#
# Composite is the SPREAD-STATE axis: how far a narrative has spread, right now. Every
# term on it must be a level, never a change — `velocity` used to sit here and was
# dropped, because a growth term on the state axis double-counts the growth that the
# acceleration axis exists to measure. See D6 in docs/narrative-alert-redesign.md.
# 0.625/0.375 is the old 0.50/0.30 rescaled proportionally once velocity's 0.20 left.
COMPOSITE_ENGAGEMENT_WEIGHT = float(os.environ.get("COMPOSITE_ENGAGEMENT_WEIGHT", "0.625"))
COMPOSITE_REACH_WEIGHT = float(os.environ.get("COMPOSITE_REACH_WEIGHT", "0.375"))

# Acceleration rate score weights (must sum to 1).
#
# Acceleration is the CHANGE-IN-SPREAD axis, and it measures speed of distribution;
# engagement change is a MODIFIER on that and must not be able to overturn it. The one
# hard constraint is ACCELERATION_ENGAGEMENT_WEIGHT < ACCELERATION_VIEWS_WEIGHT — the
# old 0.40/0.35/0.25 broke it, so a narrative whose views grew while its engagement
# ratio dipped scored a *negative* rate and was floored to zero. Measured against
# 2026-07-16 (n=2237), the old weights erased 679 genuine growers; these erase 43.
ACCELERATION_ENGAGEMENT_WEIGHT = float(os.environ.get("ACCELERATION_ENGAGEMENT_WEIGHT", "0.10"))
ACCELERATION_VIDEO_VOLUME_WEIGHT = float(os.environ.get("ACCELERATION_VIDEO_VOLUME_WEIGHT", "0.35"))
ACCELERATION_VIEWS_WEIGHT = float(os.environ.get("ACCELERATION_VIEWS_WEIGHT", "0.55"))

# A baseline older than this many days cannot anchor a rate (D4). Per-day
# normalisation would otherwise launder an old surge into today's number: a video last
# seen three months ago contributes a quarter's growth, divided down but anchored to a
# baseline that means nothing now. Measured 2026-07-15: 96.8% of the cohort is within 7
# days, 99.2% within 14, so this excludes ~1% of videos. Set to 0 to disable the bound.
ACCELERATION_MAX_BASELINE_AGE_DAYS = int(os.environ.get("ACCELERATION_MAX_BASELINE_AGE_DAYS", "14"))

# Alert-level percentile thresholds. Both axes are classified by their PERCENT_RANK
# within their own cohort, never by raw values, so each threshold means a knowable
# fraction of that cohort — and a rank is self-calibrating against a scraper whose
# coverage drifts, which an absolute bar is not (D2).
#
# The four labels are RECTANGLES on the percentile plane, not quadrants, so there are
# six thresholds and not two, and there is a no-badge region (small AND flat) in the
# bottom-left. See D1 for the geometry and NarrativeService._classify for the order.
ALERT_COMPOSITE_LO = float(os.environ.get("ALERT_COMPOSITE_LO", "0.40"))   # early_surge ceiling / trending floor
ALERT_COMPOSITE_MID = float(os.environ.get("ALERT_COMPOSITE_MID", "0.50"))  # consolidated floor
ALERT_COMPOSITE_HI = float(os.environ.get("ALERT_COMPOSITE_HI", "0.80"))   # viral floor
ALERT_ACCEL_LO = float(os.environ.get("ALERT_ACCEL_LO", "0.40"))           # consolidated ceiling / trending floor
ALERT_ACCEL_MID = float(os.environ.get("ALERT_ACCEL_MID", "0.50"))         # early_surge floor
ALERT_ACCEL_HI = float(os.environ.get("ALERT_ACCEL_HI", "0.80"))           # viral floor

"""internationalisation"""
i18n.set("file_format", "json")
i18n.set("filename_format", "{locale}.{format}")
i18n.set("skip_locale_root_data", True)
i18n.set("fallback", "en")
i18n.set("enable_memoization", True)

locales_path = "./core/i18n/locales"
i18n.set("load_path", [locales_path])

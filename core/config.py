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

# There is deliberately no reach cap. `reach_score` used to be
# `min(views / avg_views, 10) / 10`, tunable through VIRALITY_SCORE_REACH_CAP_LIMIT; the
# whole expression is gone. Everything downstream reads the PERCENT_RANK of the score,
# and a rank is invariant under any monotonic transform, so dividing by a per-run
# constant never moved a narrative relative to another. The clip did — it tied every
# narrative above the ceiling into one indistinguishable block, and it did so at the top
# of the axis, which is exactly the band `viral` and `consolidated` read. Measured on a
# dev database (n=2242), that was 23 narratives — ~1% of the corpus, but all 23 inside the
# top reach quintile, i.e. the largest narratives we have, left to be ordered by a
# size-neutral ratio. D2's ranking already delivers the bounded, outlier-immune [0, 1] the
# cap was there to provide.

# Composite virality score weights (must sum to 1).
#
# Composite is the SPREAD-STATE axis: how far a narrative has spread, right now. Every
# term on it must be a level, never a change — `velocity` used to sit here and was
# dropped, because a growth term on the state axis double-counts the growth that the
# acceleration axis exists to measure. See D6 in docs/narrative-spread-level-redesign.md.
#
# Reach LEADS the blend. The axis asks how far a narrative has spread, and reach is the
# only term that answers it: engagement_score is a per-view ratio, so it is size-neutral
# by construction and a 2k-view narrative with dense comments scores what a 2M-view one
# does at the same density. Engagement is quality of spread — the right tiebreak between
# narratives of comparable size, a modifier rather than the thing itself. That mirrors
# ACCELERATION_ENGAGEMENT_WEIGHT below: engagement modifies on both axes and leads on
# neither. The magnitudes are a free choice; the ordering is not. Production ran the
# reverse (engagement 0.50, reach 0.30, velocity 0.20).
COMPOSITE_ENGAGEMENT_WEIGHT = float(os.environ.get("COMPOSITE_ENGAGEMENT_WEIGHT", "0.375"))
COMPOSITE_REACH_WEIGHT = float(os.environ.get("COMPOSITE_REACH_WEIGHT", "0.625"))

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

# Spread-level percentile thresholds. Both axes are classified by their PERCENT_RANK
# within their own cohort, never by raw values, so each threshold means a knowable
# fraction of that cohort — and a rank is self-calibrating against a scraper whose
# coverage drifts, which an absolute bar is not (D2).
#
# The four labels are RECTANGLES on the percentile plane, not quadrants, so there are
# six thresholds and not two, and there is a no-badge region (small AND flat) in the
# bottom-left. See D1 for the geometry and NarrativeService._classify for the order.
SPREAD_COMPOSITE_LO = float(os.environ.get("SPREAD_COMPOSITE_LO", "0.40"))   # early_surge ceiling / trending floor
SPREAD_COMPOSITE_MID = float(os.environ.get("SPREAD_COMPOSITE_MID", "0.50"))  # consolidated floor
SPREAD_COMPOSITE_HI = float(os.environ.get("SPREAD_COMPOSITE_HI", "0.80"))   # viral floor
SPREAD_ACCEL_LO = float(os.environ.get("SPREAD_ACCEL_LO", "0.40"))           # consolidated ceiling / trending floor
SPREAD_ACCEL_MID = float(os.environ.get("SPREAD_ACCEL_MID", "0.50"))         # early_surge floor
SPREAD_ACCEL_HI = float(os.environ.get("SPREAD_ACCEL_HI", "0.80"))           # viral floor

"""internationalisation"""
i18n.set("file_format", "json")
i18n.set("filename_format", "{locale}.{format}")
i18n.set("skip_locale_root_data", True)
i18n.set("fallback", "en")
i18n.set("enable_memoization", True)

locales_path = "./core/i18n/locales"
i18n.set("load_path", [locales_path])

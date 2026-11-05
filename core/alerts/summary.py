"""A condition in words, for the e-mail: "Topic: Migration · Language: Spanish". The
same wording as the frontend's alert list (useConditionSummary)."""

from typing import Any
from uuid import UUID

import pycountry
from psycopg import AsyncCursor
from psycopg.rows import DictRow

from core.alerts.models import AlertCondition

PLATFORMS = {"tiktok": "TikTok", "youtube": "YouTube", "instagram": "Instagram"}
SPREAD_PATTERNS = {
    "viral": "Viral",
    "early_surge": "Early surge",
    "trending": "Trending",
    "consolidated": "Consolidated",
}


def language_name(code: str) -> str:
    found = pycountry.languages.get(alpha_2=code) or pycountry.languages.get(alpha_3=code)
    return found.name if found else code


async def names_for(session: AsyncCursor[DictRow], conditions: list[AlertCondition]) -> dict[str, str]:
    """Names of the topics and entities the conditions use, by id."""
    ids = {v for c in conditions for key in ("topic_id", "entity_id") for v in c.filters.get(key, [])}
    if not ids:
        return {}
    uuids = [UUID(v) for v in ids]
    await session.execute(
        """
        SELECT id::text AS id, topic AS name FROM topics WHERE id = ANY(%(ids)s)
        UNION ALL
        SELECT id::text, name FROM entities WHERE id = ANY(%(ids)s)
        """,
        {"ids": uuids},
    )
    return {row["id"]: row["name"] for row in await session.fetchall()}


def condition_summary(condition: AlertCondition, names: dict[str, str]) -> str:
    f: dict[str, Any] = condition.filters
    parts: list[str] = []

    def add(label: str, values: list[str]) -> None:
        if values:
            parts.append(f"{label}: {', '.join(values)}")

    add("Topic", [names.get(v, v) for v in f.get("topic_id", [])])
    add("Keywords (all)" if f.get("keyword_mode") == "all" else "Keywords", f.get("keyword", []))
    add("Language", [language_name(v) for v in f.get("language", [])])
    add("Platform", [PLATFORMS.get(v, v) for v in f.get("platform", [])])
    add("Channel", f.get("channel", []))
    add("Entities", [names.get(v, v) for v in f.get("entity_id", [])])
    add("Spread pattern", [SPREAD_PATTERNS.get(v, v) for v in f.get("spread_pattern", [])])
    if not parts:
        return "any new claim" if condition.type == "new_claim_in_narrative" else "no filters"
    return " · ".join(parts)

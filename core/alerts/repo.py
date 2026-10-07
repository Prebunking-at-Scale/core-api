from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb

from core.alerts.models import AlertCondition, AlertConditionInput, AlertRule
from core.alerts.summary import names_for
from core.search.models import SearchFilters
from core.search.query import QUERIES

# How far back an element may have appeared and still be reported: an element can
# match a few days after it appears (a claim gets its topic or its narrative later).
CANDIDATE_WINDOW_DAYS = 7
# Matches counted per condition and run, at most
MATCH_CAP = 10_000


def search_filters(filters: dict[str, Any]) -> SearchFilters:
    """A condition's filters as the search's, so alerts match exactly like Research."""
    return SearchFilters(
        topic_ids=filters.get("topic_id", []),
        entity_ids=filters.get("entity_id", []),
        keywords=filters.get("keyword", []),
        keyword_mode=filters.get("keyword_mode", "any"),
        languages=filters.get("language", []),
        platforms=filters.get("platform", []),
        channels=filters.get("channel", []),
        min_score=filters.get("min_score"),
        max_score=filters.get("max_score"),
        spread_patterns=filters.get("spread_pattern", []),
    )


class AlertRepository:
    """Alerts belong to one person in one organisation: every read and write for the
    API is scoped to both."""

    def __init__(self, session: psycopg.AsyncCursor[DictRow]) -> None:
        self._session = session

    # Reading ---------------------------------------------------------------------

    async def _with_conditions(self, rows: list[DictRow]) -> list[AlertRule]:
        if not rows:
            return []
        ids = [row["id"] for row in rows]
        await self._session.execute(
            """
            SELECT id, alert_id, type, narrative_id, filters
            FROM alert_rule_conditions
            WHERE alert_id = ANY(%(ids)s)
            ORDER BY alert_id, position
            """,
            {"ids": ids},
        )
        conditions: dict[UUID, list[AlertCondition]] = {}
        for row in await self._session.fetchall():
            conditions.setdefault(row["alert_id"], []).append(
                AlertCondition(id=row["id"], type=row["type"], narrative_id=row["narrative_id"], filters=row["filters"])
            )
        return [AlertRule(**row, conditions=conditions.get(row["id"], [])) for row in rows]

    _SELECT = """
        SELECT a.id, a.name, a.enabled, a.created_at,
               (SELECT max(r.reported_at) FROM alert_rule_reports r WHERE r.alert_id = a.id) AS last_match_at
        FROM alert_rules a
    """

    async def list_own(self, organisation_id: UUID, user_id: UUID) -> list[AlertRule]:
        await self._session.execute(
            self._SELECT
            + " WHERE a.organisation_id = %(organisation_id)s AND a.user_id = %(user_id)s"
            + " ORDER BY a.created_at DESC, a.id",
            {"organisation_id": organisation_id, "user_id": user_id},
        )
        return await self._with_conditions(await self._session.fetchall())

    async def get_own(self, organisation_id: UUID, user_id: UUID, alert_id: UUID) -> AlertRule | None:
        await self._session.execute(
            self._SELECT
            + " WHERE a.id = %(id)s AND a.organisation_id = %(organisation_id)s AND a.user_id = %(user_id)s",
            {"id": alert_id, "organisation_id": organisation_id, "user_id": user_id},
        )
        found = await self._with_conditions(await self._session.fetchall())
        return found[0] if found else None

    # Writing ---------------------------------------------------------------------

    async def _write_conditions(self, alert_id: UUID, conditions: list[AlertConditionInput]) -> None:
        await self._session.execute("DELETE FROM alert_rule_conditions WHERE alert_id = %(id)s", {"id": alert_id})
        await self._session.executemany(
            """
            INSERT INTO alert_rule_conditions (alert_id, position, type, narrative_id, filters)
            VALUES (%(alert_id)s, %(position)s, %(type)s, %(narrative_id)s, %(filters)s)
            """,
            [
                {
                    "alert_id": alert_id,
                    "position": position,
                    "type": c.type,
                    "narrative_id": c.narrative_id,
                    "filters": Jsonb(c.filters),
                }
                for position, c in enumerate(conditions, start=1)
            ],
        )

    async def create(
        self,
        organisation_id: UUID,
        user_id: UUID,
        name: str,
        enabled: bool,
        conditions: list[AlertConditionInput],
    ) -> UUID:
        await self._session.execute(
            """
            INSERT INTO alert_rules (organisation_id, user_id, name, enabled)
            VALUES (%(organisation_id)s, %(user_id)s, %(name)s, %(enabled)s)
            RETURNING id
            """,
            {"organisation_id": organisation_id, "user_id": user_id, "name": name, "enabled": enabled},
        )
        row = await self._session.fetchone()
        assert row
        await self._write_conditions(row["id"], conditions)
        return row["id"]

    async def replace(
        self,
        alert_id: UUID,
        name: str,
        enabled: bool,
        conditions: list[AlertConditionInput],
        restart_counting: bool,
    ) -> None:
        await self._session.execute(
            """
            UPDATE alert_rules
            SET name = %(name)s, enabled = %(enabled)s, updated_at = CURRENT_TIMESTAMP,
                counting_since = CASE WHEN %(restart)s THEN CURRENT_TIMESTAMP ELSE counting_since END
            WHERE id = %(id)s
            """,
            {"id": alert_id, "name": name, "enabled": enabled, "restart": restart_counting},
        )
        await self._write_conditions(alert_id, conditions)

    async def delete(self, alert_id: UUID) -> None:
        await self._session.execute("DELETE FROM alert_rules WHERE id = %(id)s", {"id": alert_id})

    async def follow_merged_narrative(self, source_id: UUID, target_id: UUID) -> None:
        """Conditions that followed a narrative merged into another follow the target."""
        await self._session.execute(
            "UPDATE alert_rule_conditions SET narrative_id = %(target)s WHERE narrative_id = %(source)s",
            {"source": source_id, "target": target_id},
        )

    async def names(self, conditions: list[AlertCondition]) -> dict[str, str]:
        return await names_for(self._session, conditions)

    async def narrative_titles(self, ids: list[UUID]) -> dict[UUID, str]:
        await self._session.execute("SELECT id, title FROM narratives WHERE id = ANY(%(ids)s)", {"ids": ids})
        return {row["id"]: row["title"] for row in await self._session.fetchall()}

    # Matching --------------------------------------------------------------------

    async def matches(
        self, alert_id: UUID, condition: AlertCondition, counting_since: datetime
    ) -> list[tuple[UUID, str]]:
        """(id, title) of what one condition matches now and its alert hasn't reported:
        elements that appeared after the alert's starting point (and in the last
        CANDIDATE_WINDOW_DAYS), with the search's own matching. Newest first."""
        filters = search_filters(condition.filters)
        params: dict[str, Any] = {
            "_alert": alert_id,
            "_since": counting_since,
            "_window": f"{CANDIDATE_WINDOW_DAYS} days",
            "_cap": MATCH_CAP,
        }
        if condition.type == "new_narrative":
            query = QUERIES["narratives"](filters)
            sql = f"""
                SELECT n.id, n.title, n.created_at AS appeared FROM narratives n
                WHERE {query.where_sql}
                  AND n.created_at >= %(_since)s
                  AND n.created_at >= CURRENT_TIMESTAMP - %(_window)s::interval
                  AND NOT EXISTS (SELECT 1 FROM alert_rule_reports r WHERE r.alert_id = %(_alert)s
                                  AND r.element_kind = 'narrative' AND r.element_id = n.id)
                ORDER BY n.created_at DESC LIMIT %(_cap)s
            """
        else:
            query = QUERIES["claims"](filters)
            if condition.type == "new_claim":
                appeared = "c.created_at"
                joined = ""
            else:
                # Joining the followed narrative is what counts, even for an older claim
                appeared = "fn.created_at"
                joined = "JOIN claim_narratives fn ON fn.claim_id = c.id AND fn.narrative_id = %(_narrative)s"
                params["_narrative"] = condition.narrative_id
            sql = f"""
                SELECT c.id, c.claim AS title, {appeared} AS appeared FROM {query.from_sql} {joined}
                WHERE {query.where_sql}
                  AND {appeared} >= %(_since)s
                  AND {appeared} >= CURRENT_TIMESTAMP - %(_window)s::interval
                  AND NOT EXISTS (SELECT 1 FROM alert_rule_reports r WHERE r.alert_id = %(_alert)s
                                  AND r.element_kind = 'claim' AND r.element_id = c.id)
                ORDER BY {appeared} DESC LIMIT %(_cap)s
            """
        await self._session.execute(sql, {**query.params, **params})
        return [(row["id"], row["title"]) for row in await self._session.fetchall()]

    async def record_reports(self, alert_id: UUID, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        await self._session.executemany(
            """
            INSERT INTO alert_rule_reports (alert_id, element_kind, element_id, conditions, section, in_narrative_id)
            VALUES (%(alert_id)s, %(kind)s, %(id)s, %(conditions)s, %(section)s, %(in_narrative_id)s)
            ON CONFLICT DO NOTHING
            """,
            [{"alert_id": alert_id, **row} for row in rows],
        )

    # The daily run ---------------------------------------------------------------

    async def recipients_with_alerts(self) -> list[DictRow]:
        """Each person with an enabled alert, while they and their organisation are
        active: (user_id, organisation_id, email)."""
        await self._session.execute(
            """
            SELECT DISTINCT a.user_id, a.organisation_id, u.email
            FROM alert_rules a
            JOIN users u ON u.id = a.user_id
            JOIN organisations o ON o.id = a.organisation_id AND o.deactivated IS NULL
            JOIN organisation_users ou ON ou.user_id = a.user_id AND ou.organisation_id = a.organisation_id
                 AND ou.deactivated IS NULL
            WHERE a.enabled
            """
        )
        return await self._session.fetchall()

    async def counting_since(self, alert_id: UUID) -> datetime:
        await self._session.execute("SELECT counting_since FROM alert_rules WHERE id = %(id)s", {"id": alert_id})
        row = await self._session.fetchone()
        assert row
        return row["counting_since"]

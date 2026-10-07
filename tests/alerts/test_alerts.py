"""The new alerts (frontend docs/alerts.md and docs/alerts-implementation.md): owner-only
API, validation, matching (no backlog, once only, joining a followed narrative), the
daily e-mail, merges, and the migration of today's alerts. Uses the search's data set."""

from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from litestar import Litestar
from litestar.testing import AsyncTestClient
from pytest import MonkeyPatch, fixture

import core.alerts.service as alert_service_module
from core.alerts.service import AlertService
from core.auth.models import Organisation
from core.auth.service import AuthService
from tests.auth.conftest import create_organisation, create_user
from tests.search.conftest import CLIMATE, HEALTH, SearchData, insert_search_data

Client = AsyncTestClient[Litestar]


@fixture
def tables_to_truncate() -> list[str]:
    return [
        "alert_rule_reports",
        "alert_rule_conditions",
        "alert_rules",
        "alerts",
        "videos",
        "video_claims",
        "claim_topics",
        "narratives",
        "claim_narratives",
        "narrative_topics",
        "narrative_entities",
        "entities",
    ]


@fixture
async def auth_service(conn_factory: Any) -> AuthService:
    return AuthService(conn_factory)


@fixture
async def organisation(auth_service: AuthService) -> Organisation:
    return await create_organisation(auth_service, short_name=f"org-{uuid4().hex[:6]}")


@fixture
async def data(conn_factory: Any, tables_to_truncate: list[str]) -> SearchData:
    # Tests without a client don't get its clean-up afterwards: start clean here
    async with conn_factory() as conn:
        await conn.execute("TRUNCATE " + ", ".join(tables_to_truncate) + " CASCADE")
        return await insert_search_data(conn)


class Person:
    def __init__(self, user_id: UUID, token: str) -> None:
        self.id = user_id
        self.headers = {"Authorization": f"Bearer {token}"}


async def person(auth_service: AuthService, organisation: Organisation) -> Person:
    user = await create_user(auth_service, organisation, False, is_super_admin=False)
    await auth_service.update_password(user, "password123")
    login = await auth_service.login(user.email, "password123")
    return Person(user.id, login.organisations[organisation.id].token)


def narrative_alert(name: str = "Health watch", **filters: Any) -> dict[str, Any]:
    return {"name": name, "conditions": [{"type": "new_narrative", "filters": filters or {"topic_id": [str(HEALTH)]}}]}


class FakeEmailer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, str]] = []

    def send(self, to: str, subject: str, html: str) -> None:
        self.sent.append((to, subject, html))


@fixture
def emailer(monkeypatch: MonkeyPatch) -> FakeEmailer:
    fake = FakeEmailer()

    async def get_fake() -> FakeEmailer:
        return fake

    monkeypatch.setattr(alert_service_module, "get_emailer", get_fake)
    return fake


async def sql(conn_factory: Any, query: str, params: Any = None) -> list[Any]:
    async with conn_factory() as conn:
        cur = await conn.execute(query, params)
        return await cur.fetchall() if cur.description else []


# The API ---------------------------------------------------------------------------


async def test_an_alert_is_its_creators_only(
    auth_client: Client, auth_service: AuthService, organisation: Organisation
) -> None:
    alice = await person(auth_service, organisation)
    bob = await person(auth_service, organisation)

    response = await auth_client.post("/api/alerts", json=narrative_alert(), headers=alice.headers)
    assert response.status_code == 201, response.text
    alert = response.json()["data"]
    assert [c["type"] for c in alert["conditions"]] == ["new_narrative"]

    assert len((await auth_client.get("/api/alerts", headers=alice.headers)).json()["data"]) == 1
    assert (await auth_client.get("/api/alerts", headers=bob.headers)).json()["data"] == []
    for method in ("get", "put", "delete"):
        kwargs: dict[str, Any] = {"headers": bob.headers}
        if method == "put":
            kwargs["json"] = narrative_alert("Stolen")
        response = await getattr(auth_client, method)(f"/api/alerts/{alert['id']}", **kwargs)
        assert response.status_code == 404, method

    response = await auth_client.delete(f"/api/alerts/{alert['id']}", headers=alice.headers)
    assert response.status_code == 204
    assert (await auth_client.get("/api/alerts", headers=alice.headers)).json()["data"] == []


async def test_what_an_alert_needs(
    auth_client: Client, auth_service: AuthService, organisation: Organisation
) -> None:
    alice = await person(auth_service, organisation)
    narrative = str(uuid4())
    cases = {
        "name_required": {"name": " ", "conditions": [{"type": "new_claim", "filters": {"language": ["es"]}}]},
        "conditions_required": {"name": "x", "conditions": []},
        "invalid_type": {"name": "x", "conditions": [{"type": "views", "filters": {}}]},
        "narrative_required": {"name": "x", "conditions": [{"type": "new_claim_in_narrative", "filters": {}}]},
        "narrative_not_allowed": {
            "name": "x",
            "conditions": [{"type": "new_claim", "narrative_id": narrative, "filters": {"language": ["es"]}}],
        },
        "empty_condition": {"name": "x", "conditions": [{"type": "new_narrative", "filters": {"keyword": []}}]},
        "filter_not_allowed": {"name": "x", "conditions": [{"type": "new_narrative", "filters": {"min_score": 3}}]},
        # Claims are never filtered by their score
        "filter_not_allowed ": {"name": "x", "conditions": [{"type": "new_claim", "filters": {"min_score": 3}}]},
        "invalid_filter": {"name": "x", "conditions": [{"type": "new_narrative", "filters": {"topic_id": ["nope"]}}]},
    }
    for case, body in cases.items():
        code = case.strip()
        response = await auth_client.post("/api/alerts", json=body, headers=alice.headers)
        assert response.status_code == 422, case
        assert code in response.json()["extra"]["errors"], (case, response.json())

    # A condition following a narrative may have no filters: every new claim of it
    body = {"name": "Follow", "conditions": [{"type": "new_claim_in_narrative", "narrative_id": narrative, "filters": {}}]}
    assert (await auth_client.post("/api/alerts", json=body, headers=alice.headers)).status_code == 201


async def test_alerts_are_listed_newest_first(
    auth_client: Client, auth_service: AuthService, organisation: Organisation
) -> None:
    alice = await person(auth_service, organisation)
    for name in ("First", "Second", "Third"):
        await auth_client.post("/api/alerts", json=narrative_alert(name), headers=alice.headers)
    listed = (await auth_client.get("/api/alerts", headers=alice.headers)).json()["data"]
    assert [a["name"] for a in listed] == ["Third", "Second", "First"]


# Matching and the e-mail -------------------------------------------------------------


async def test_no_backlog_once_only_and_the_email(
    conn_factory: Any, auth_service: AuthService, organisation: Organisation, data: SearchData, emailer: FakeEmailer
) -> None:
    alice = await person(auth_service, organisation)
    service = AlertService(conn_factory)
    from core.alerts.models import AlertRuleInput

    alert = await service.create(organisation.id, alice.id, AlertRuleInput(**narrative_alert()))
    # Everything already there appeared before the alert: nothing is reported
    await sql(conn_factory, "UPDATE narratives SET created_at = CURRENT_TIMESTAMP - interval '1 hour'")
    assert await service.digest_preview(organisation.id, alice.id) == []
    assert await service.send_digests() == (0, 0)

    # A narrative that appears afterwards is
    await sql(conn_factory, "UPDATE narratives SET created_at = CURRENT_TIMESTAMP + interval '1 second' WHERE id = %s", (data["n_vax"],))
    preview = await service.digest_preview(organisation.id, alice.id)
    assert [i.title for i in preview[0].narratives.items] == ["Vaccines contain microchips to track people"]

    assert await service.send_digests() == (1, 1)
    [(to, subject, html)] = emailer.sent
    assert subject == "1 alert triggered"
    assert "Alert triggered: Health watch" in html
    assert "<strong>Vaccines contain microchips to track people</strong>" in html
    assert "(Topic: Health)" in html

    # Once only
    assert await service.send_digests() == (0, 0)
    listed = await service.list_own(organisation.id, alice.id)
    assert listed[0].id == alert.id and listed[0].last_match_at is not None


async def test_a_claim_joining_a_followed_narrative_counts_even_if_older(
    conn_factory: Any, auth_service: AuthService, organisation: Organisation, data: SearchData, emailer: FakeEmailer
) -> None:
    alice = await person(auth_service, organisation)
    service = AlertService(conn_factory)
    from core.alerts.models import AlertRuleInput

    old = "CURRENT_TIMESTAMP - interval '2 days'"
    await sql(conn_factory, f"UPDATE video_claims SET created_at = {old} WHERE id <> %s", (data["c3"],))
    await sql(conn_factory, f"UPDATE claim_narratives SET created_at = {old}")
    await service.create(
        organisation.id,
        alice.id,
        AlertRuleInput(
            name="Chemtrails",
            conditions=[
                {"type": "new_claim_in_narrative", "narrative_id": data["n_chem"], "filters": {}},
                {"type": "new_claim", "filters": {"language": ["fr"]}},
            ],
        ),
    )
    await sql(conn_factory, "UPDATE alert_rules SET counting_since = CURRENT_TIMESTAMP - interval '1 hour'")
    # An old German claim joins the narrative now; the French c3 is new and joins now too
    await sql(conn_factory, "INSERT INTO claim_narratives (claim_id, narrative_id) VALUES (%s, %s)", (data["c7"], data["n_chem"]))
    await sql(conn_factory, "UPDATE claim_narratives SET created_at = CURRENT_TIMESTAMP WHERE claim_id = %s", (data["c3"],))

    [entry] = await service.digest_preview(organisation.id, alice.id)
    [group] = entry.in_narratives
    assert group.narrative_title == "Planes spray chemicals to control the weather"
    by_id = {i.id: i.conditions for i in group.items}
    assert by_id == {data["c7"]: [1], data["c3"]: [1, 2]}
    # c3 is only listed under its narrative, not again under new claims
    assert entry.claims.total == 0


async def test_a_merged_narrative_is_followed_in_its_target(
    api_key_client: Client, conn_factory: Any, auth_service: AuthService, organisation: Organisation, data: SearchData
) -> None:
    alice = await person(auth_service, organisation)
    service = AlertService(conn_factory)
    from core.alerts.models import AlertRuleInput

    alert = await service.create(
        organisation.id,
        alice.id,
        AlertRuleInput(name="Hoax", conditions=[{"type": "new_claim_in_narrative", "narrative_id": data["n_climate"], "filters": {}}]),
    )
    response = await api_key_client.post(f"/api/narratives/{data['n_climate']}/merge", json={"into": str(data["n_chem"])})
    assert response.status_code == 204, response.text

    [condition] = (await service.get_own(organisation.id, alice.id, alert.id)).conditions
    assert condition.narrative_id == data["n_chem"]
    assert await sql(conn_factory, "SELECT 1 FROM narratives WHERE id = %s", (data["n_climate"],)) == []
    topics = await sql(conn_factory, "SELECT topic_id FROM narrative_topics WHERE narrative_id = %s", (data["n_chem"],))
    assert [r["topic_id"] for r in topics] == [CLIMATE]

    same = await api_key_client.post(f"/api/narratives/{data['n_chem']}/merge", json={"into": str(data["n_chem"])})
    assert same.status_code == 400


async def test_updating_a_narrative_keeps_when_its_claims_joined(
    api_key_client: Client, conn_factory: Any, data: SearchData
) -> None:
    await sql(conn_factory, "UPDATE claim_narratives SET created_at = '2026-01-01' WHERE narrative_id = %s", (data["n_chem"],))
    claim_ids = [str(data["c1"]), str(data["c2"]), str(data["c8"])]  # c3 leaves, c8 joins
    response = await api_key_client.patch(f"/api/narratives/{data['n_chem']}", json={"claim_ids": claim_ids})
    assert response.status_code == 200, response.text

    rows = await sql(
        conn_factory,
        "SELECT claim_id, created_at FROM claim_narratives WHERE narrative_id = %s",
        (data["n_chem"],),
    )
    joined = {r["claim_id"]: r["created_at"].year for r in rows}
    assert joined == {data["c1"]: 2026, data["c2"]: 2026, data["c8"]: joined[data["c8"]]}
    assert str(rows[0]["created_at"].date()) != "" and data["c3"] not in joined
    old = {r["claim_id"] for r in rows if str(r["created_at"]).startswith("2026-01-01")}
    assert old == {data["c1"], data["c2"]}


# The migration of today's alerts -------------------------------------------------------


async def test_topic_and_keyword_alerts_are_carried_over_and_thresholds_dropped(
    conn_factory: Any, auth_service: AuthService, organisation: Organisation, auth_client: Client
) -> None:
    alice = await person(auth_service, organisation)
    rows = [
        ("Health", "narrative_with_topic", "topic_id", HEALTH, True),
        ("Vacunas", "keyword", "keyword", "vacuna", False),
        ("Big ones", "narrative_views", "threshold", 1000, True),
    ]
    for name, alert_type, column, value, enabled in rows:
        await sql(
            conn_factory,
            f"""
            INSERT INTO alerts (user_id, organisation_id, name, alert_type, scope, {column}, enabled)
            VALUES (%s, %s, %s, %s, 'general', %s, %s)
            """,
            (alice.id, organisation.id, name, alert_type, value, enabled),
        )
    migration = Path("core/migrations/27.migrate-alerts-to-alert-rules.up.sql").read_text()
    async with conn_factory() as conn:
        await conn.execute(migration)

    carried = await AlertService(conn_factory).list_own(organisation.id, alice.id)
    by_name = {a.name: a for a in carried}
    assert set(by_name) == {"Health", "Vacunas"}
    assert by_name["Health"].conditions[0].type == "new_narrative"
    assert by_name["Health"].conditions[0].filters == {"topic_id": [str(HEALTH)]}
    assert by_name["Vacunas"].conditions[0].filters == {"keyword": ["vacuna"]}
    assert by_name["Vacunas"].enabled is False


async def test_narratives_to_follow_by_title_first_then_by_their_claims(
    auth_client: Client, auth_service: AuthService, organisation: Organisation, data: SearchData
) -> None:
    alice = await person(auth_service, organisation)

    async def found(text: str) -> list[tuple[str, str]]:
        response = await auth_client.get("/api/alerts/narratives", params={"text": text}, headers=alice.headers)
        assert response.status_code == 200, response.text
        return [(n["title"], n["matched_in"]) for n in response.json()["data"]]

    # "weather" is in n_chem's title, and in c1, a claim of n_chem and of n_climate
    assert await found("WEATHER") == [
        ("Planes spray chemicals to control the weather", "title"),
        ("Climate change is a hoax", "claims"),
    ]
    # Only in the claims
    assert await found("weather weapon") == [
        ("Planes spray chemicals to control the weather", "claims"),
        ("Climate change is a hoax", "claims"),
    ]
    assert await found("x") == []  # too short


async def test_narratives_to_follow_leave_room_for_both_kinds(
    auth_client: Client, auth_service: AuthService, organisation: Organisation, data: SearchData, conn_factory: Any
) -> None:
    alice = await person(auth_service, organisation)
    for i in range(6):
        await sql(conn_factory, "INSERT INTO narratives (title, description) VALUES (%s, '')", (f"Weather story {i}",))
    response = await auth_client.get("/api/alerts/narratives", params={"text": "weather", "limit": 4}, headers=alice.headers)
    kinds = [n["matched_in"] for n in response.json()["data"]]
    # 7 titles, 1 narrative only through a claim (n_climate): the claims kind is shown,
    # and the room it doesn't use goes to titles
    assert kinds == ["title", "title", "title", "claims"]

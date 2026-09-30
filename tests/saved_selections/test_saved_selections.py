"""Saved selections: the organisation's defaults from its feeds, and each person's
own selections, private to them."""

from typing import Any
from uuid import uuid4

from litestar import Litestar
from litestar.testing import AsyncTestClient
from pytest import fixture

from core.auth.models import Organisation, User
from core.auth.service import AuthService
from tests.auth.conftest import create_organisation, create_user

Client = AsyncTestClient[Litestar]

CLIMATE = "db3d996b-e691-4ce5-8c46-e35a82a9b28c"
HEALTH = "bb52f622-b9ee-4d5b-9b70-5fd05046528b"


@fixture
def tables_to_truncate() -> list[str]:
    return ["saved_selections", "channel_feeds", "keyword_feeds"]


@fixture
async def auth_service(conn_factory: Any) -> AuthService:
    return AuthService(conn_factory)


@fixture
async def organisation(auth_service: AuthService) -> Organisation:
    return await create_organisation(auth_service, short_name=f"maldita-{uuid4().hex[:6]}")


async def token_for(auth_service: AuthService, organisation: Organisation) -> str:
    user: User = await create_user(auth_service, organisation, False, is_super_admin=False)
    await auth_service.update_password(user, "password123")
    login = await auth_service.login(user.email, "password123")
    return login.organisations[organisation.id].token


def as_user(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def listed(client: Client, token: str, kind: str) -> list[dict[str, Any]]:
    response = await client.get("/api/saved-selections", params={"kind": kind}, headers=as_user(token))
    assert response.status_code == 200, response.text
    return response.json()["data"]


async def save(client: Client, token: str, **body: Any) -> Any:
    return await client.post("/api/saved-selections", json=body, headers=as_user(token))


async def test_defaults_come_from_the_organisations_feeds(
    auth_client: Client, auth_service: AuthService, organisation: Organisation, conn_factory: Any
) -> None:
    async with conn_factory() as conn:
        for channel, platform in (("Le Réveil", "youtube"), ("@datos_ocultos", "tiktok")):
            await conn.execute(
                "INSERT INTO channel_feeds (organisation_id, channel, platform) VALUES (%s, %s, %s)",
                (organisation.id, channel, platform),
            )
        for topic, keywords in ((HEALTH, ["vacuna", "OMS"]), (CLIMATE, ["sequía", "HAARP"])):
            await conn.execute(
                "INSERT INTO keyword_feeds (organisation_id, topic_id, keywords) VALUES (%s, %s, %s)",
                (organisation.id, topic, keywords),
            )
    token = await token_for(auth_service, organisation)

    assert await listed(auth_client, token, "channel") == [
        {
            "id": "default-channels",
            "kind": "channel",
            "name": f"{organisation.short_name}-channels",
            "values": ["@datos_ocultos", "Le Réveil"],
            "is_default": True,
            "created_at": None,
        }
    ]
    keywords = await listed(auth_client, token, "keyword")
    assert [(k["name"], k["values"], k["is_default"]) for k in keywords] == [
        (f"{organisation.short_name}-Climate", ["sequía", "HAARP"], True),
        (f"{organisation.short_name}-Health", ["vacuna", "OMS"], True),
    ]
    assert keywords[0]["id"] == f"default-topic-{CLIMATE}"
    assert await listed(auth_client, token, "entity_id") == []

    # Defaults can't be deleted: they change with the feeds
    response = await auth_client.delete("/api/saved-selections/default-channels", headers=as_user(token))
    assert response.status_code == 403


async def test_a_persons_selections_are_private(
    auth_client: Client, auth_service: AuthService, organisation: Organisation
) -> None:
    alice = await token_for(auth_service, organisation)
    bob = await token_for(auth_service, organisation)
    other_org = await create_organisation(auth_service)
    carol = await token_for(auth_service, other_org)

    response = await save(auth_client, alice, kind="keyword", name="Vacunas", values=["vacuna", "vacunación"])
    assert response.status_code == 201, response.text
    saved = response.json()["data"]
    assert (saved["name"], saved["values"], saved["is_default"]) == ("Vacunas", ["vacuna", "vacunación"], False)

    assert [s["name"] for s in await listed(auth_client, alice, "keyword")] == ["Vacunas"]
    assert await listed(auth_client, bob, "keyword") == []
    assert await listed(auth_client, carol, "keyword") == []

    # Someone else's selection doesn't exist for you
    for token in (bob, carol):
        response = await auth_client.delete(f"/api/saved-selections/{saved['id']}", headers=as_user(token))
        assert response.status_code == 404
    # The same name is free for someone else
    assert (await save(auth_client, bob, kind="keyword", name="vacunas", values=["x"])).status_code == 201

    response = await auth_client.delete(f"/api/saved-selections/{saved['id']}", headers=as_user(alice))
    assert response.status_code == 204
    assert await listed(auth_client, alice, "keyword") == []


async def test_what_a_selection_needs(
    auth_client: Client, auth_service: AuthService, organisation: Organisation, conn_factory: Any
) -> None:
    async with conn_factory() as conn:
        await conn.execute(
            "INSERT INTO keyword_feeds (organisation_id, topic_id, keywords) VALUES (%s, %s, %s)",
            (organisation.id, CLIMATE, ["sequía"]),
        )
    token = await token_for(auth_service, organisation)
    assert (await save(auth_client, token, kind="keyword", name="Lista", values=["a"])).status_code == 201

    cases = {
        "invalid_kind": {"kind": "topic", "name": "x", "values": ["a"]},
        "name_required": {"kind": "keyword", "name": "   ", "values": ["a"]},
        "name_too_long": {"kind": "keyword", "name": "x" * 61, "values": ["a"]},
        "values_required": {"kind": "keyword", "name": "x", "values": [" ", ""]},
        "name_taken": {"kind": "keyword", "name": "LISTA", "values": ["b"]},
    }
    for code, body in cases.items():
        response = await save(auth_client, token, **body)
        assert (response.status_code, response.json()["detail"]) == (422, code), code

    # A default's name is taken as well
    response = await save(auth_client, token, kind="keyword", name=f"{organisation.short_name.upper()}-climate", values=["b"])
    assert (response.status_code, response.json()["detail"]) == (422, "name_taken")

    # Values are trimmed and deduplicated
    response = await save(auth_client, token, kind="channel", name="Canales", values=[" @a ", "@a", "@b"])
    assert response.json()["data"]["values"] == ["@a", "@b"]


async def test_needs_a_signed_in_person(auth_client: Client) -> None:
    response = await auth_client.get("/api/saved-selections", params={"kind": "keyword"})
    assert response.status_code == 401

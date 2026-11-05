"""The search's filter options: channels and claim languages."""

from typing import Any

from litestar import Litestar
from litestar.testing import AsyncTestClient

import core.app as app
from core.auth.models import Organisation
from core.search.service import clear_languages_cache
from tests.search.conftest import SearchData

Client = AsyncTestClient[Litestar]


async def add_channel_feed(organisation: Organisation, channel: str, platform: str) -> None:
    async with app.app.state.connection_factory() as conn:
        await conn.execute(
            "INSERT INTO channel_feeds (organisation_id, channel, platform) VALUES (%s, %s, %s)",
            (organisation.id, channel, platform),
        )


async def channels(client: Client, organisation: Organisation, **params: Any) -> list[dict[str, Any]]:
    response = await client.get(
        "/api/search/channels", params={"organisation_id": str(organisation.id), **params}
    )
    assert response.status_code == 200, response.text
    return response.json()["data"]


async def test_without_text_only_the_organisations_own_channels(
    api_key_client: Client, data: SearchData, organisation: Organisation
) -> None:
    await add_channel_feed(organisation, "Le Réveil", "youtube")
    await add_channel_feed(organisation, "@datos_ocultos", "tiktok")
    assert await channels(api_key_client, organisation) == [
        {"channel": "@datos_ocultos", "platform": "tiktok", "is_own": True},
        {"channel": "Le Réveil", "platform": "youtube", "is_own": True},
    ]
    assert [c["channel"] for c in await channels(api_key_client, organisation, platform="youtube")] == ["Le Réveil"]


async def test_the_own_channels_are_never_cut_at_the_limit(
    api_key_client: Client, data: SearchData, organisation: Organisation
) -> None:
    # "Ours" selects them all, so all of them come back, whatever the limit
    for i in range(25):
        await add_channel_feed(organisation, f"@feed{i:02d}", "youtube")
    assert len(await channels(api_key_client, organisation)) == 25
    assert len(await channels(api_key_client, organisation, limit=5)) == 25


async def test_with_text_every_collected_channel_containing_it_own_first(
    api_key_client: Client, data: SearchData, organisation: Organisation
) -> None:
    await add_channel_feed(organisation, "@truthseeker99", "tiktok")
    found = await channels(api_key_client, organisation, text="T")
    names = [c["channel"] for c in found]
    assert names[0] == "@truthseeker99" and found[0]["is_own"] is True
    assert set(names) == {"@truthseeker99", "EuroSkeptic TV", "@datos_ocultos", "@klartext_de", "salud.natural"}
    assert [c["channel"] for c in await channels(api_key_client, organisation, text="t", platform="youtube")] == [
        "EuroSkeptic TV"
    ]


async def test_channel_text_is_literal(api_key_client: Client, data: SearchData, organisation: Organisation) -> None:
    assert await channels(api_key_client, organisation, text="%") == []
    assert [c["channel"] for c in await channels(api_key_client, organisation, text="datos_")] == ["@datos_ocultos"]


async def test_claim_languages_most_common_first(api_key_client: Client, data: SearchData) -> None:
    clear_languages_cache()
    response = await api_key_client.get("/api/search/languages")
    assert response.status_code == 200
    assert response.json()["data"] == [
        {"language": "es", "count": 5},
        {"language": "en", "count": 3},
        {"language": "de", "count": 2},
        {"language": "fr", "count": 1},
    ]

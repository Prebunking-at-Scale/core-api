"""The search's matching rules (docs/search.md), on the data set in conftest.py.

These are the acceptance cases of the frontend's dev search (its
tests/devSearch/match.test.ts), plus counts, paging and sort order.
"""

from typing import Any

from litestar import Litestar
from litestar.testing import AsyncTestClient

import core.search.repo as search_repo
from tests.search.conftest import (
    CLAIMS,
    CLIMATE,
    CONFLICTS,
    HEALTH,
    NARRATIVES,
    VIDEOS,
    SearchData,
)

Client = AsyncTestClient[Litestar]


async def search(client: Client, tab: str, **params: Any) -> dict[str, Any]:
    params.setdefault("limit", 100)
    response = await client.get(f"/api/search/{tab}", params=params)
    assert response.status_code == 200, response.text
    return response.json()


async def matched(client: Client, data: SearchData, tab: str, **params: Any) -> dict[str, str]:
    """{short name: match_source} of every result."""
    body = await search(client, tab, **params)
    return {data.name_of(item["id"]): item["match_source"] for item in body["data"]}


async def test_no_filters_returns_everything_directly(api_key_client: Client, data: SearchData) -> None:
    narratives = await matched(api_key_client, data, "narratives")
    assert set(narratives) == set(NARRATIVES)
    assert set(narratives.values()) == {"direct"}
    assert set(await matched(api_key_client, data, "claims")) == set(CLAIMS)
    assert set(await matched(api_key_client, data, "videos")) == set(VIDEOS)


# Topics ----------------------------------------------------------------------


async def test_topic_matches_a_tagged_narrative_directly_and_an_untagged_one_through_a_claim(
    api_key_client: Client, data: SearchData
) -> None:
    narratives = await matched(api_key_client, data, "narratives", topic_id=str(CLIMATE))
    assert narratives == {"n_climate": "direct", "n_chem": "claims"}


async def test_a_claims_topics_are_its_own_never_its_narratives(api_key_client: Client, data: SearchData) -> None:
    # c10 has no topic of its own; its narrative n_vax is about Health
    claims = await matched(api_key_client, data, "claims", topic_id=str(HEALTH))
    assert set(claims) == {"c4", "c5", "c6"}


async def test_topic_carries_to_videos_without_the_label(api_key_client: Client, data: SearchData) -> None:
    videos = await matched(api_key_client, data, "videos", topic_id=str(CLIMATE))
    assert videos == {"v1": "direct"}


async def test_topics_are_ored(api_key_client: Client, data: SearchData) -> None:
    claims = await matched(api_key_client, data, "claims", topic_id=[str(CLIMATE), str(CONFLICTS)])
    assert set(claims) == {"c1", "c9", "c7"}


async def test_claim_topics_come_from_the_classifier_not_the_claim_finder(
    api_key_client: Client, data: SearchData
) -> None:
    # c9's metadata.topics says Health; claim_topics says Climate
    body = await search(api_key_client, "claims", keyword="carbon taxes")
    [claim] = body["data"]
    assert [t["topic"] for t in claim["topics"]] == ["Climate"]
    assert "c9" not in await matched(api_key_client, data, "claims", topic_id=str(HEALTH))


# Keywords --------------------------------------------------------------------


async def test_keyword_matches_a_narrative_title_directly_and_claim_text_through_claims(
    api_key_client: Client, data: SearchData
) -> None:
    assert (await matched(api_key_client, data, "narratives", keyword="vaccine")) == {"n_vax": "direct"}
    assert (await matched(api_key_client, data, "narratives", keyword="vacuna")) == {"n_vax": "claims"}


async def test_with_a_claim_filter_a_narratives_keywords_must_be_in_that_claim(
    api_key_client: Client, data: SearchData
) -> None:
    # n_vax's title says "Vaccines"; its English TikTok claim (c6) doesn't
    assert await matched(api_key_client, data, "narratives", keyword="vaccines") == {"n_vax": "direct"}
    assert await matched(api_key_client, data, "narratives", keyword="vaccines", platform="tiktok", language="en") == {}
    # A claim that has both: c5, a TikTok claim about the microchip
    assert await matched(api_key_client, data, "narratives", keyword="microchip", platform="tiktok") == {
        "n_vax": "claims"
    }
    # The other tabs agree
    assert await matched(api_key_client, data, "claims", keyword="vaccines", platform="tiktok", language="en") == {}


async def test_keyword_matches_a_video_title_directly_and_its_claims_with_the_label(
    api_key_client: Client, data: SearchData
) -> None:
    videos = await matched(api_key_client, data, "videos", keyword="vacuna")
    # v4's title says "vacunas"; v5's title doesn't, but its claim c5 says "vacuna"
    assert videos == {"v4": "direct", "v5": "claims"}


async def test_keywords_ignore_case_accents_and_hyphens(api_key_client: Client, data: SearchData) -> None:
    plain = await matched(api_key_client, data, "claims", keyword="VACUNACION")
    accented = await matched(api_key_client, data, "claims", keyword="vacunación")
    assert set(plain) == set(accented) == {"c4"}
    assert set(await matched(api_key_client, data, "claims", keyword="weather-weapon")) == {"c1"}


async def test_keywords_match_any_by_default_and_all_in_the_same_text(api_key_client: Client, data: SearchData) -> None:
    anyof = await matched(api_key_client, data, "claims", keyword=["pisos", "sequías"])
    assert set(anyof) == {"c8", "c2"}
    both = await matched(api_key_client, data, "claims", keyword=["inmigrantes", "pisos"], keyword_mode="all")
    assert set(both) == {"c8"}
    none = await matched(api_key_client, data, "claims", keyword=["pisos", "sequías"], keyword_mode="all")
    assert none == {}


async def test_all_keywords_must_be_in_one_claim_of_a_narrative(api_key_client: Client, data: SearchData) -> None:
    # n_vax has "Pfizer" in c5 and "5G" in c6 and c10, never both in one claim
    narratives = await matched(api_key_client, data, "narratives", keyword=["Pfizer", "5G"], keyword_mode="all")
    assert "n_vax" not in narratives


async def test_keywords_are_phrases_and_literal(api_key_client: Client, data: SearchData) -> None:
    assert set(await matched(api_key_client, data, "claims", keyword="carbon taxes")) == {"c9"}
    assert await matched(api_key_client, data, "claims", keyword="taxes carbon") == {}
    # % and _ are not wildcards
    assert await matched(api_key_client, data, "claims", keyword="%") == {}
    assert await matched(api_key_client, data, "claims", keyword="c_rbon") == {}


# Entities --------------------------------------------------------------------


async def test_entities_match_narratives_directly(api_key_client: Client, data: SearchData) -> None:
    narratives = await matched(api_key_client, data, "narratives", entity_id=str(data["e_nato"]))
    assert narratives == {"n_nato": "direct"}


async def test_claims_found_through_their_narrative_say_so(api_key_client: Client, data: SearchData) -> None:
    body = await search(api_key_client, "claims", entity_id=[str(data["e_pfizer"]), str(data["e_nato"])])
    by_name = {data.name_of(item["id"]): item for item in body["data"]}
    assert set(by_name) == {"c4", "c5", "c6", "c10", "c7"}
    assert by_name["c6"]["match_source"] == "narrative"
    assert [e["name"] for e in by_name["c6"]["via_entities"]] == ["Pfizer"]
    # c5 names Pfizer itself
    assert by_name["c5"]["match_source"] == "direct"
    assert by_name["c5"]["via_entities"] == []


async def test_entities_carry_to_videos(api_key_client: Client, data: SearchData) -> None:
    videos = await matched(api_key_client, data, "videos", entity_id=str(data["e_nato"]))
    assert set(videos) == {"v6"}


# Claim-level filters on narratives -----------------------------------------------


async def test_claim_level_filters_must_hold_on_the_same_claim(api_key_client: Client, data: SearchData) -> None:
    # n_chem has a French claim (c3, Le Réveil) and a claim from @datos_ocultos (c2),
    # but no claim that is both
    assert "n_chem" not in await matched(api_key_client, data, "narratives", language="fr", channel="@datos_ocultos")
    assert (await matched(api_key_client, data, "narratives", language="fr", channel="Le Réveil")) == {"n_chem": "direct"}


async def test_a_direct_topic_match_combines_with_a_claim_match(api_key_client: Client, data: SearchData) -> None:
    # n_climate is about Climate itself, and its claim c1 is in English
    assert await matched(api_key_client, data, "narratives", topic_id=str(CLIMATE), language="en") == {
        "n_climate": "direct",
        "n_chem": "claims",
    }
    # Keywords don't: n_chem's title says "Planes", its French claim c3 doesn't
    assert await matched(api_key_client, data, "narratives", keyword="planes", language="fr") == {}


async def test_claim_only_filters_never_add_the_label(api_key_client: Client, data: SearchData) -> None:
    narratives = await matched(api_key_client, data, "narratives", platform="tiktok")
    assert set(narratives) == {"n_chem", "n_vax", "n_nato"}
    assert set(narratives.values()) == {"direct"}


# Platform, channel, date -----------------------------------------------------------


async def test_platform_applies_to_videos_and_carries_to_claims(api_key_client: Client, data: SearchData) -> None:
    assert set(await matched(api_key_client, data, "videos", platform="instagram")) == {"v4"}
    assert set(await matched(api_key_client, data, "claims", platform="instagram")) == {"c4", "c10"}


async def test_channel_is_exact_ignoring_case(api_key_client: Client, data: SearchData) -> None:
    assert set(await matched(api_key_client, data, "videos", channel="@DATOS_OCULTOS")) == {"v2"}
    assert await matched(api_key_client, data, "videos", channel="@datos") == {}


async def test_upload_date_bounds_are_inclusive(api_key_client: Client, data: SearchData) -> None:
    videos = await matched(
        api_key_client, data, "videos", start_date="2026-09-01T00:00:00", end_date="2026-09-03T23:59:59"
    )
    assert set(videos) == {"v4", "v5", "v6"}


# Tab-specific filters, counts, paging, order ----------------------------------------


async def test_spread_pattern_only_counts_on_narratives_and_score_never_filters(
    api_key_client: Client, data: SearchData
) -> None:
    # min_score isn't a filter: claims are never filtered by their score
    params = {"min_score": 4.5, "spread_pattern": "viral"}
    assert set(await matched(api_key_client, data, "claims", **params)) == set(CLAIMS)
    assert set(await matched(api_key_client, data, "narratives", **params)) == {"n_vax"}
    assert set(await matched(api_key_client, data, "videos", **params)) == set(VIDEOS)


async def test_counts_follow_the_filters(api_key_client: Client, data: SearchData) -> None:
    response = await api_key_client.get("/api/search/counts", params={"topic_id": str(CLIMATE)})
    assert response.status_code == 200
    assert response.json()["data"] == {
        "narratives": {"total": 2, "capped": False},
        "claims": {"total": 2, "capped": False},
        "videos": {"total": 1, "capped": False},
    }


async def test_counts_stop_at_the_cap(api_key_client: Client, data: SearchData, monkeypatch: Any) -> None:
    monkeypatch.setattr(search_repo, "COUNT_CAP", 3)
    body = await search(api_key_client, "claims")
    assert body["total"] == 3 and body["total_capped"] is True
    assert len(body["data"]) == len(CLAIMS)  # the page itself isn't cut


async def test_paging(api_key_client: Client, data: SearchData) -> None:
    first = await search(api_key_client, "claims", limit=4, offset=0)
    second = await search(api_key_client, "claims", limit=4, offset=4)
    assert (first["page"], first["size"], second["page"], second["size"]) == (1, 4, 2, 4)
    assert not {c["id"] for c in first["data"]} & {c["id"] for c in second["data"]}


async def test_claims_and_videos_newest_upload_first_narratives_newest_first(
    api_key_client: Client, data: SearchData
) -> None:
    videos = [data.name_of(v["id"]) for v in (await search(api_key_client, "videos"))["data"]]
    assert videos == ["v1", "v2", "v3", "v4", "v5", "v6", "v7"]
    claims = [data.name_of(c["id"]) for c in (await search(api_key_client, "claims"))["data"]]
    assert [CLAIMS[c][0] for c in claims] == sorted((CLAIMS[c][0] for c in claims), key=lambda v: VIDEOS[v][3], reverse=True)
    narratives = [data.name_of(n["id"]) for n in (await search(api_key_client, "narratives"))["data"]]
    assert narratives == ["n_chem", "n_vax", "n_nato", "n_climate"]


async def test_claim_results_carry_their_video_narratives_and_topics(api_key_client: Client, data: SearchData) -> None:
    body = await search(api_key_client, "claims", keyword="weather weapon")
    [claim] = body["data"]
    assert claim["video"]["channel"] == "EuroSkeptic TV"
    assert {data.name_of(n["id"]) for n in claim["narratives"]} == {"n_chem", "n_climate"}
    assert [t["id"] for t in claim["topics"]] == [str(CLIMATE)]
    assert claim["metadata"]["language"] == "en"


async def test_narrative_results_carry_their_sizes(api_key_client: Client, data: SearchData) -> None:
    body = await search(api_key_client, "narratives", keyword="vaccine")
    [narrative] = body["data"]
    assert (narrative["claim_count"], narrative["video_count"], narrative["language_count"]) == (4, 2, 2)
    assert narrative["spread_pattern"] == "viral"
    assert [t["topic"] for t in narrative["topics"]] == ["Health"]


async def test_limit_is_bounded(api_key_client: Client, data: SearchData) -> None:
    response = await api_key_client.get("/api/search/claims", params={"limit": 101})
    assert response.status_code == 400

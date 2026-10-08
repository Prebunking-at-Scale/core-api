"""A small, hand-built data set where every search rule has a case (docs/search.md).

It mirrors the frontend's dev search fixture: a narrative tagged with a topic and one
that only has it through a claim, keywords in titles and in claim text, claims whose
narrative has an entity, claims without a narrative, a video without claims.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from litestar import Litestar
from litestar.testing import AsyncTestClient
from psycopg.types.json import Jsonb
from pytest import fixture

import core.app as app
from core.auth.models import Organisation
from core.auth.service import AuthService
from tests.auth.conftest import create_organisation

CLIMATE = UUID("db3d996b-e691-4ce5-8c46-e35a82a9b28c")
HEALTH = UUID("bb52f622-b9ee-4d5b-9b70-5fd05046528b")
MIGRATION = UUID("3cd4a9cd-5906-4b0b-9167-57ff22c2345a")
CONFLICTS = UUID("0d7aaf8d-5b7e-4c0c-b03a-28457e27ac7d")


@dataclass
class SearchData:
    """Ids by the short names used in the tests (v1, c4, n_vax, e_nato…)."""

    ids: dict[str, UUID]

    def __getitem__(self, name: str) -> UUID:
        return self.ids[name]

    def name_of(self, value: str) -> str:
        for name, id_ in self.ids.items():
            if str(id_) == value:
                return name
        raise KeyError(value)


VIDEOS = {
    # name: (platform, channel, title, uploaded_at)
    "v1": ("youtube", "EuroSkeptic TV", "Heatwave or weather weapon?", "2026-09-18"),
    "v2": ("tiktok", "@datos_ocultos", "Chemtrails sobre Madrid", "2026-09-10"),
    "v3": ("youtube", "Le Réveil", "Ces traînées ne sont pas naturelles", "2026-09-05"),
    "v4": ("instagram", "salud.natural", "Lo que llevan las vacunas", "2026-09-03"),
    "v5": ("tiktok", "@truthseeker99", "Pfizer chip patent", "2026-09-02"),
    "v6": ("tiktok", "@klartext_de", "Wehrpflicht für die Ukraine?", "2026-09-01"),
    "v7": ("youtube", "freedom.voices", "Morning routine", "2026-08-30"),
}

CLAIMS = {
    # name: (video, language, score, topics (claim_topics), text)
    "c1": ("v1", "en", 4.1, [CLIMATE], "The heatwave was caused by a weather weapon sprayed from planes."),
    "c2": ("v2", "es", 3.0, [], "Los aviones fumigan productos químicos para provocar sequías."),
    "c3": ("v3", "fr", 3.2, [], "Les avions répandent des produits chimiques."),
    "c4": ("v4", "es", 4.5, [HEALTH], "La vacunación masiva inserta microchips."),
    "c5": ("v5", "es", 4.0, [HEALTH], "Pfizer patentó un microchip para la vacuna."),
    "c6": ("v5", "en", 3.5, [HEALTH], "Bill Gates funds 5G tracking."),
    "c7": ("v6", "de", 4.2, [CONFLICTS], "Die NATO schickt Wehrpflichtige in die Ukraine."),
    "c8": ("v2", "es", 2.0, [MIGRATION], "Los inmigrantes reciben pisos gratis."),
    "c9": ("v1", "en", 2.5, [CLIMATE], "Carbon taxes are a scam."),
    "c10": ("v4", "es", 3.0, [], "El grafeno de las vacunas se activa con 5G."),
    "c11": ("v6", "de", 3.9, [], "Die Grenzen werden geschlossen."),
}

# What the claim finder put in metadata.topics, where it differs: the search ignores it
FINDER_TOPICS = {"c9": [HEALTH, "kerncentrale"]}

NARRATIVES = {
    # name: (title, created_at, topics, entities, claims, spread_pattern)
    "n_chem": ("Planes spray chemicals to control the weather", "2026-09-20", [], [], ["c1", "c2", "c3"], None),
    "n_vax": ("Vaccines contain microchips to track people", "2026-09-15", [HEALTH], ["e_pfizer"], ["c4", "c5", "c6", "c10"], "viral"),
    "n_nato": ("NATO is preparing to send conscripts", "2026-09-10", [CONFLICTS], ["e_nato"], ["c7"], "early_surge"),
    "n_climate": ("Climate change is a hoax", "2026-09-05", [CLIMATE], [], ["c1"], None),
}

ENTITIES = {"e_nato": "NATO", "e_pfizer": "Pfizer"}


@fixture
def tables_to_truncate() -> list[str]:
    return [
        "videos",
        "video_claims",
        "claim_topics",
        "narratives",
        "claim_narratives",
        "narrative_topics",
        "narrative_entities",
        "entities",
        "channel_feeds",
        "saved_selections",
    ]


async def insert_search_data(conn: Any) -> SearchData:
    ids: dict[str, UUID] = {}
    for name, entity in ENTITIES.items():
        ids[name] = uuid4()
        await conn.execute(
            "INSERT INTO entities (id, wikidata_id, name) VALUES (%s, %s, %s)",
            (ids[name], f"Q-{name}", entity),
        )
    for name, (platform, channel, title, uploaded) in VIDEOS.items():
        ids[name] = uuid4()
        await conn.execute(
            """
            INSERT INTO videos (id, title, description, platform, source_url,
                                destination_path, channel, uploaded_at, metadata)
            VALUES (%s, %s, '', %s, %s, '', %s, %s, '{}')
            """,
            (ids[name], title, platform, f"https://example.org/{name}", channel, datetime.fromisoformat(uploaded)),
        )
    for name, (video, language, score, topics, text) in CLAIMS.items():
        ids[name] = uuid4()
        await conn.execute(
            """
            INSERT INTO video_claims (id, video_id, claim, start_time_s, metadata)
            VALUES (%s, %s, %s, 12, %s)
            """,
            (
                ids[name],
                ids[video],
                text,
                Jsonb({"language": language, "score": score, "topics": [str(t) for t in FINDER_TOPICS.get(name, [])]}),
            ),
        )
        for topic in topics:
            await conn.execute(
                "INSERT INTO claim_topics (claim_id, topic_id) VALUES (%s, %s)", (ids[name], topic)
            )
    for name, (title, created, topics, entities, claims, spread) in NARRATIVES.items():
        ids[name] = uuid4()
        await conn.execute(
            """
            INSERT INTO narratives (id, title, description, created_at, spread_pattern)
            VALUES (%s, %s, '', %s, %s)
            """,
            (ids[name], title, datetime.fromisoformat(created), spread),
        )
        for topic in topics:
            await conn.execute(
                "INSERT INTO narrative_topics (narrative_id, topic_id) VALUES (%s, %s)",
                (ids[name], topic),
            )
        for entity in entities:
            await conn.execute(
                "INSERT INTO narrative_entities (narrative_id, entity_id) VALUES (%s, %s)",
                (ids[name], ids[entity]),
            )
        for claim in claims:
            await conn.execute(
                "INSERT INTO claim_narratives (claim_id, narrative_id) VALUES (%s, %s)",
                (ids[claim], ids[name]),
            )
    return SearchData(ids)


@fixture
async def data(api_key_client: AsyncTestClient[Litestar]) -> SearchData:
    async with app.app.state.connection_factory() as conn:
        return await insert_search_data(conn)


@fixture
async def organisation(conn_factory: Any) -> Organisation:
    return await create_organisation(AuthService(conn_factory))

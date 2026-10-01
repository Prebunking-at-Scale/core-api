"""GET /api/narratives has two date filters that mean different things:
start_date/end_date on when the narrative's videos were posted, and
created_start/created_end on when the narrative was created. Uses the search data set
(conftest.py): narratives created 5, 10, 15 and 20 September; n_chem and n_climate have
a video posted on 18 September, the others none after the 10th."""

from typing import Any

from litestar import Litestar
from litestar.testing import AsyncTestClient

from tests.search.conftest import SearchData

Client = AsyncTestClient[Litestar]


async def listed(client: Client, data: SearchData, **params: Any) -> set[str]:
    response = await client.get("/api/narratives", params={"limit": 100, "offset": 0, **params})
    assert response.status_code == 200, response.text
    body = response.json()
    names = {data.name_of(n["id"]) for n in body["data"]}
    assert body["total"] == len(names)
    return names


async def test_created_bounds_are_on_the_narratives_creation(api_key_client: Client, data: SearchData) -> None:
    assert await listed(api_key_client, data, created_start="2026-09-12T00:00:00") == {"n_chem", "n_vax"}
    assert await listed(api_key_client, data, created_end="2026-09-08T00:00:00") == {"n_climate"}


async def test_created_and_upload_bounds_combine(api_key_client: Client, data: SearchData) -> None:
    # Upload date alone: narratives with a video posted from the 15th on
    assert await listed(api_key_client, data, start_date="2026-09-15T00:00:00") == {"n_chem", "n_climate"}
    # And created from the 12th on
    assert await listed(
        api_key_client, data, start_date="2026-09-15T00:00:00", created_start="2026-09-12T00:00:00"
    ) == {"n_chem"}

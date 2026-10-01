from datetime import datetime
from typing import Literal
from uuid import UUID

from litestar import Controller, get
from litestar.di import Provide
from litestar.params import Parameter

from core.auth.models import Organisation
from core.models import NarrativeSpreadPattern
from core.response import JSON
from core.search.models import (
    ChannelOption,
    LanguageOption,
    SearchClaim,
    SearchCounts,
    SearchFilters,
    SearchNarrative,
    SearchPage,
    SearchVideo,
)
from core.search.service import SearchService
from core.uow import ConnectionFactory


async def search_service(connection_factory: ConnectionFactory) -> SearchService:
    return SearchService(connection_factory=connection_factory)


def search_filters(
    topic_id: list[UUID] | None = Parameter(
        default=None, description="Topics (OR). A claim's own topics (claim_topics), or a narrative's."
    ),
    entity_id: list[UUID] | None = Parameter(
        default=None, description="Entities (OR), on narratives; claims and videos through them."
    ),
    keyword: list[str] | None = Parameter(
        default=None,
        description=(
            "Keywords, each matched as a phrase anywhere in claim text, video titles "
            "or narrative titles, ignoring case, accents and hyphens."
        ),
    ),
    keyword_mode: Literal["any", "all"] = Parameter(
        default="any", description="all: every keyword in the same text."
    ),
    language: list[str] | None = Parameter(default=None, description="Claim languages (OR)."),
    platform: list[str] | None = Parameter(default=None, description="Video platforms (OR)."),
    channel: list[str] | None = Parameter(
        default=None, description="Video channels (OR), exact, ignoring case."
    ),
    start_date: datetime | None = Parameter(
        default=None, description="Video upload date, inclusive."
    ),
    end_date: datetime | None = Parameter(
        default=None, description="Video upload date, inclusive."
    ),
    min_score: float | None = Parameter(default=None, description="Claims only."),
    max_score: float | None = Parameter(default=None, description="Claims only."),
    spread_pattern: list[NarrativeSpreadPattern] | None = Parameter(
        default=None, description="Narratives only (OR)."
    ),
) -> SearchFilters:
    return SearchFilters(
        topic_ids=topic_id or [],
        entity_ids=entity_id or [],
        keywords=keyword or [],
        keyword_mode=keyword_mode,
        languages=language or [],
        platforms=platform or [],
        channels=channel or [],
        start_date=start_date,
        end_date=end_date,
        min_score=min_score,
        max_score=max_score,
        spread_patterns=spread_pattern or [],
    )


def _page_number(limit: int, offset: int) -> int:
    return offset // limit + 1


class SearchController(Controller):
    """One search over narratives, claims and videos with the same filters
    (docs/search.md). Results and totals are global; only the channel options know
    the organisation, to put its own channels first."""

    path = "/search"
    tags = ["search"]

    dependencies = {
        "search_service": Provide(search_service),
        "filters": Provide(search_filters, sync_to_thread=False),
    }

    @get(path="/narratives", summary="Search narratives, newest first")
    async def search_narratives(
        self,
        search_service: SearchService,
        filters: SearchFilters,
        limit: int = Parameter(default=12, ge=1, le=100),
        offset: int = Parameter(default=0, ge=0),
    ) -> SearchPage[list[SearchNarrative]]:
        items, total, capped = await search_service.narratives(filters, limit, offset)
        return SearchPage(
            data=items,
            total=total,
            total_capped=capped,
            page=_page_number(limit, offset),
            size=len(items),
        )

    @get(path="/claims", summary="Search claims, newest video upload first")
    async def search_claims(
        self,
        search_service: SearchService,
        filters: SearchFilters,
        limit: int = Parameter(default=12, ge=1, le=100),
        offset: int = Parameter(default=0, ge=0),
    ) -> SearchPage[list[SearchClaim]]:
        items, total, capped = await search_service.claims(filters, limit, offset)
        return SearchPage(
            data=items,
            total=total,
            total_capped=capped,
            page=_page_number(limit, offset),
            size=len(items),
        )

    @get(path="/videos", summary="Search videos, newest upload first")
    async def search_videos(
        self,
        search_service: SearchService,
        filters: SearchFilters,
        limit: int = Parameter(default=12, ge=1, le=100),
        offset: int = Parameter(default=0, ge=0),
    ) -> SearchPage[list[SearchVideo]]:
        items, total, capped = await search_service.videos(filters, limit, offset)
        return SearchPage(
            data=items,
            total=total,
            total_capped=capped,
            page=_page_number(limit, offset),
            size=len(items),
        )

    @get(path="/counts", summary="How many results each tab has, up to 10,000")
    async def search_counts(
        self, search_service: SearchService, filters: SearchFilters
    ) -> JSON[SearchCounts]:
        return JSON(await search_service.counts(filters))

    @get(
        path="/channels",
        summary="Channel options: the organisation's own, or channels matching a text",
    )
    async def search_channels(
        self,
        search_service: SearchService,
        optional_organisation: Organisation | None,
        text: str | None = Parameter(
            default=None,
            description=(
                "Part of the channel name, ignoring case. Without it, only the "
                "organisation's own channels (its channel feeds) are returned."
            ),
        ),
        platform: list[str] | None = None,
        limit: int = Parameter(default=20, ge=1, le=100),
    ) -> JSON[list[ChannelOption]]:
        return JSON(
            await search_service.channels(
                text,
                platform or [],
                optional_organisation.id if optional_organisation else None,
                limit,
            )
        )

    @get(path="/languages", summary="Language options: claim languages with counts")
    async def search_languages(
        self, search_service: SearchService
    ) -> JSON[list[LanguageOption]]:
        return JSON(await search_service.languages())

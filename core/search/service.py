import time
from typing import AsyncContextManager
from uuid import UUID

from core.search.models import (
    ChannelOption,
    LanguageOption,
    SearchClaim,
    SearchCounts,
    SearchFilters,
    SearchNarrative,
    SearchTab,
    SearchVideo,
    TabCount,
)
from core.search.repo import SearchRepository
from core.uow import ConnectionFactory, uow

# Counting languages reads every claim, and the answer barely moves: keep it an hour.
LANGUAGES_TTL_S = 60 * 60
_languages_cache: tuple[float, list[LanguageOption]] | None = None


class SearchService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def repo(self) -> AsyncContextManager[SearchRepository]:
        return uow(SearchRepository, self._connection_factory)

    async def narratives(
        self, filters: SearchFilters, limit: int, offset: int
    ) -> tuple[list[SearchNarrative], int, bool]:
        async with self.repo() as repo:
            total, capped = await repo.count("narratives", filters)
            page = await repo.page_ids("narratives", filters, limit, offset)
            return await repo.narratives(page), total, capped

    async def claims(
        self, filters: SearchFilters, limit: int, offset: int
    ) -> tuple[list[SearchClaim], int, bool]:
        async with self.repo() as repo:
            total, capped = await repo.count("claims", filters)
            page = await repo.page_ids("claims", filters, limit, offset)
            return await repo.claims(page, filters.entity_ids), total, capped

    async def videos(
        self, filters: SearchFilters, limit: int, offset: int
    ) -> tuple[list[SearchVideo], int, bool]:
        async with self.repo() as repo:
            total, capped = await repo.count("videos", filters)
            page = await repo.page_ids("videos", filters, limit, offset)
            return await repo.videos(page), total, capped

    async def counts(self, filters: SearchFilters) -> SearchCounts:
        async with self.repo() as repo:
            tabs: dict[str, TabCount] = {}
            order: tuple[SearchTab, ...] = ("narratives", "claims", "videos")
            for tab in order:
                total, capped = await repo.count(tab, filters)
                tabs[tab] = TabCount(total=total, capped=capped)
            return SearchCounts(**tabs)

    async def channels(
        self,
        text: str | None,
        platforms: list[str],
        organisation_id: UUID | None,
        limit: int,
    ) -> list[ChannelOption]:
        """With no text, all the organisation's own channels (what "Ours" selects, so
        never cut at the limit); with text, up to `limit` collected channels containing
        it, the organisation's own first."""
        async with self.repo() as repo:
            if not text or not text.strip():
                if not organisation_id:
                    return []
                return await repo.own_channels(organisation_id, platforms)
            return await repo.search_channels(
                text.strip(), platforms, organisation_id, limit
            )

    async def languages(self) -> list[LanguageOption]:
        global _languages_cache
        now = time.monotonic()
        if _languages_cache and now - _languages_cache[0] < LANGUAGES_TTL_S:
            return _languages_cache[1]
        async with self.repo() as repo:
            languages = await repo.claim_languages()
        _languages_cache = (now, languages)
        return languages


def clear_languages_cache() -> None:
    """For tests."""
    global _languages_cache
    _languages_cache = None

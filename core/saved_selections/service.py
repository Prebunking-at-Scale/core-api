from typing import AsyncContextManager, get_args
from uuid import UUID

from core.auth.models import Organisation
from core.errors import NotFoundError
from core.media_feeds.service import MediaFeedsService
from core.saved_selections.models import (
    NAME_MAX_LENGTH,
    SavedSelection,
    SavedSelectionError,
    SavedSelectionInput,
    SavedSelectionKind,
)
from core.saved_selections.repo import SavedSelectionRepository
from core.uow import ConnectionFactory, uow

DEFAULT_ID_PREFIX = "default-"


class DefaultSelectionError(Exception):
    """A default selection can't be deleted: it is edited as the organisation's feeds."""


class SavedSelectionService:
    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory
        self._feeds = MediaFeedsService(connection_factory)

    def repo(self) -> AsyncContextManager[SavedSelectionRepository]:
        return uow(SavedSelectionRepository, self._connection_factory)

    async def defaults(
        self, organisation: Organisation, kind: SavedSelectionKind
    ) -> list[SavedSelection]:
        """The organisation's defaults, read live from its feeds so they always match
        what it collects: "Channels" and one per keyword feed, named after its topic.
        Each organisation only sees its own, so the names don't carry it. There are no
        default entity selections."""
        if kind == "channel":
            channel_feeds = await self._feeds.get_channel_feeds(organisation.id)
            channels = sorted({feed.channel for feed in channel_feeds}, key=str.lower)
            if not channels:
                return []
            return [
                SavedSelection(
                    id=f"{DEFAULT_ID_PREFIX}channels",
                    kind="channel",
                    name="Channels",
                    values=channels,
                    is_default=True,
                )
            ]
        if kind == "keyword":
            keyword_feeds = await self._feeds.get_keyword_feeds(organisation.id)
            return sorted(
                (
                    SavedSelection(
                        id=f"{DEFAULT_ID_PREFIX}topic-{feed.topic_id}",
                        kind="keyword",
                        name=feed.topic_name,
                        values=feed.keywords,
                        is_default=True,
                    )
                    for feed in keyword_feeds
                    if feed.keywords
                ),
                key=lambda selection: selection.name.lower(),
            )
        return []

    async def list_for(
        self, organisation: Organisation, user_id: UUID, kind: SavedSelectionKind
    ) -> list[SavedSelection]:
        """Defaults first, then the person's own, each by name."""
        async with self.repo() as repo:
            own = await repo.list_own(organisation.id, user_id, kind)
        return [*await self.defaults(organisation, kind), *own]

    async def create(
        self, organisation: Organisation, user_id: UUID, data: SavedSelectionInput
    ) -> SavedSelection:
        if data.kind not in get_args(SavedSelectionKind):
            raise SavedSelectionError(detail="invalid_kind")
        kind: SavedSelectionKind = data.kind  # type: ignore[assignment]
        name = data.name.strip()
        if not name:
            raise SavedSelectionError(detail="name_required")
        if len(name) > NAME_MAX_LENGTH:
            raise SavedSelectionError(detail="name_too_long")
        values: list[str] = []
        for value in data.values:
            value = value.strip()
            if value and value not in values:
                values.append(value)
        if not values:
            raise SavedSelectionError(detail="values_required")
        # A default's name is taken too, so a list never shadows one
        if any(d.name.lower() == name.lower() for d in await self.defaults(organisation, kind)):
            raise SavedSelectionError(detail="name_taken")
        async with self.repo() as repo:
            return await repo.create(organisation.id, user_id, kind, name, values)

    async def delete(
        self, organisation: Organisation, user_id: UUID, selection_id: str
    ) -> None:
        if selection_id.startswith(DEFAULT_ID_PREFIX):
            raise DefaultSelectionError()
        try:
            parsed = UUID(selection_id)
        except ValueError:
            raise NotFoundError()
        async with self.repo() as repo:
            if not await repo.delete(organisation.id, user_id, parsed):
                raise NotFoundError()

from litestar import Controller, delete, get, post
from litestar.di import Provide
from litestar.exceptions import PermissionDeniedException

from core.auth.models import Organisation, User
from core.response import JSON
from core.saved_selections.models import (
    SavedSelection,
    SavedSelectionInput,
    SavedSelectionKind,
)
from core.saved_selections.service import DefaultSelectionError, SavedSelectionService
from core.uow import ConnectionFactory


async def saved_selection_service(
    connection_factory: ConnectionFactory,
) -> SavedSelectionService:
    return SavedSelectionService(connection_factory=connection_factory)


class SavedSelectionController(Controller):
    """Named selections of keywords, entities or channels for the search filters.
    The organisation's defaults come from its feeds and everyone in it sees them;
    the selections a person saves are visible to that person only."""

    path = "/saved-selections"
    tags = ["saved selections"]

    dependencies = {"saved_selection_service": Provide(saved_selection_service)}

    @get(path="/", summary="The organisation's defaults, then your own selections")
    async def list_saved_selections(
        self,
        saved_selection_service: SavedSelectionService,
        organisation: Organisation,
        user: User,
        kind: SavedSelectionKind,
    ) -> JSON[list[SavedSelection]]:
        return JSON(await saved_selection_service.list_for(organisation, user.id, kind))

    @post(
        path="/",
        summary="Save a selection, visible to you only",
        description=(
            "422 with detail invalid_kind, name_required, name_too_long, "
            "values_required or name_taken (names are unique per person and kind, "
            "ignoring case, defaults included)."
        ),
    )
    async def create_saved_selection(
        self,
        saved_selection_service: SavedSelectionService,
        organisation: Organisation,
        user: User,
        data: SavedSelectionInput,
    ) -> JSON[SavedSelection]:
        return JSON(await saved_selection_service.create(organisation, user.id, data))

    @delete(
        path="/{selection_id:str}",
        summary="Delete one of your selections",
        description="403 for a default (edit the organisation's feeds instead); 404 for anything that isn't yours.",
    )
    async def delete_saved_selection(
        self,
        saved_selection_service: SavedSelectionService,
        organisation: Organisation,
        user: User,
        selection_id: str,
    ) -> None:
        try:
            await saved_selection_service.delete(organisation, user.id, selection_id)
        except DefaultSelectionError:
            raise PermissionDeniedException(detail="is_default")

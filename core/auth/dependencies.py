from typing import Any
from uuid import UUID

from litestar import Request
from litestar.di import Provide

from core.auth.models import AuthToken, Identity, Organisation, User
from core.errors import OrganisationIDRequiredError


def user(request: Request[Identity, AuthToken, Any]) -> User:
    return request.user.user


def organisation(
    request: Request[Identity, AuthToken, Any],
    organisation_id: UUID | None = None,
) -> Organisation:
    """Although organisation_id is not used here, it has the desirable side effect of
    making it possible to set organisation_id in the generated documentation.
    organisation_id is then picked up by the middleware and injected into the
    generated JWT for requests using an API key."""
    if not request.user.organisation:
        raise OrganisationIDRequiredError()
    return request.user.organisation


auth = {
    "user": Provide(user, sync_to_thread=False),
    "organisation": Provide(organisation, sync_to_thread=False),
}

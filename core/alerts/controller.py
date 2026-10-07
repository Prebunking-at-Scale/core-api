from uuid import UUID

from litestar import Controller, delete, get, post, put
from litestar.di import Provide

from core.alerts.models import AlertRule, AlertRuleInput, DigestEntry
from core.alerts.service import AlertService
from core.auth.models import Organisation, User
from core.response import JSON
from core.uow import ConnectionFactory

_VALIDATION = (
    "422 with detail and extra.errors from: name_required, name_too_long, "
    "conditions_required, too_many_conditions, invalid_type, narrative_required, "
    "narrative_not_allowed, empty_condition, filter_not_allowed, invalid_filter."
)


async def alert_service(connection_factory: ConnectionFactory) -> AlertService:
    return AlertService(connection_factory=connection_factory)


class AlertController(Controller):
    """Alerts (frontend docs/alerts.md): each belongs to the person who created it, who
    is the only one to see it and receive it. Anything that isn't yours is a 404."""

    path = "/alerts"
    tags = ["alerts"]
    dependencies = {"alert_service": Provide(alert_service)}

    @get(path="/", summary="Your alerts, newest first (the order of the panel and of the e-mail)")
    async def list_alerts(
        self, alert_service: AlertService, organisation: Organisation, user: User
    ) -> JSON[list[AlertRule]]:
        return JSON(await alert_service.list_own(organisation.id, user.id))

    @post(path="/", summary="Create an alert; it starts counting now", description=_VALIDATION)
    async def create_alert(
        self, alert_service: AlertService, organisation: Organisation, user: User, data: AlertRuleInput
    ) -> JSON[AlertRule]:
        return JSON(await alert_service.create(organisation.id, user.id, data))

    @get(path="/digest-preview", summary="What your next alerts e-mail would contain, so far")
    async def digest_preview(
        self, alert_service: AlertService, organisation: Organisation, user: User
    ) -> JSON[list[DigestEntry]]:
        return JSON(await alert_service.digest_preview(organisation.id, user.id))

    @get(path="/{alert_id:uuid}", summary="One of your alerts")
    async def get_alert(
        self, alert_service: AlertService, organisation: Organisation, user: User, alert_id: UUID
    ) -> JSON[AlertRule]:
        return JSON(await alert_service.get_own(organisation.id, user.id, alert_id))

    @put(
        path="/{alert_id:uuid}",
        summary="Replace one of your alerts",
        description="Changing its conditions, or enabling it again, starts counting from now. " + _VALIDATION,
    )
    async def replace_alert(
        self,
        alert_service: AlertService,
        organisation: Organisation,
        user: User,
        alert_id: UUID,
        data: AlertRuleInput,
    ) -> JSON[AlertRule]:
        return JSON(await alert_service.replace(organisation.id, user.id, alert_id, data))

    @delete(path="/{alert_id:uuid}", summary="Delete one of your alerts")
    async def delete_alert(
        self, alert_service: AlertService, organisation: Organisation, user: User, alert_id: UUID
    ) -> None:
        await alert_service.delete(organisation.id, user.id, alert_id)

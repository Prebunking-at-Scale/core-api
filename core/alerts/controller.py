from typing import Any
from uuid import UUID

from litestar import Controller, Request, delete, get, patch, post
from litestar.di import Provide
from litestar.status_codes import HTTP_204_NO_CONTENT

from core.alerts.models import Alert, AlertScope, AlertType, CreateAlertRequest, UpdateAlertRequest
from core.alerts.service import AlertService
from core.auth.models import AuthToken, Identity
from core.response import PaginatedJSON


def alert_service(connection_factory: Any) -> AlertService:
    return AlertService(connection_factory=connection_factory)


class AlertController(Controller):
    path = "/alerts"
    tags = ["alerts"]
    dependencies = {
        "alert_service": Provide(alert_service, sync_to_thread=False),
    }

    @post(
        path="/",
        summary="Create a new alert",
        description="Create a new alert for the authenticated user and organisation",
    )
    async def create_alert(
        self,
        request: Request[Identity, AuthToken, Any],
        alert_service: AlertService,
        data: CreateAlertRequest,
    ) -> Alert:
        return await alert_service.create_alert(
            identity=request.user,
            name=data.name,
            alert_type=data.alert_type,
            scope=data.scope,
            narrative_id=data.narrative_id,
            threshold=data.threshold,
            topic_id=data.topic_id,
            keyword=data.keyword,
            metadata=data.metadata,
        )

    @get(
        path="/",
        summary="Get user alerts",
        description="Get all alerts for the authenticated user and organisation",
    )
    async def get_alerts(
        self,
        request: Request[Identity, AuthToken, Any],
        alert_service: AlertService,
        enabled_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> PaginatedJSON[list[Alert]]:
        alerts, total = await alert_service.get_user_alerts(
            identity=request.user,
            enabled_only=enabled_only,
            limit=limit,
            offset=offset,
        )
        return PaginatedJSON(
            data=alerts,
            total=total,
            page=offset // limit + 1,
            size=limit,
        )

    @get(
        path="/{alert_id:uuid}",
        summary="Get a specific alert",
        description="Get details of a specific alert",
    )
    async def get_alert(
        self,
        alert_id: UUID,
        request: Request[Identity, AuthToken, Any],
        alert_service: AlertService,
    ) -> Alert:
        return await alert_service.get_alert(alert_id, request.user)

    @patch(
        path="/{alert_id:uuid}",
        summary="Update an alert",
        description="Update an existing alert",
    )
    async def update_alert(
        self,
        alert_id: UUID,
        request: Request[Identity, AuthToken, Any],
        alert_service: AlertService,
        data: UpdateAlertRequest,
    ) -> Alert:
        return await alert_service.update_alert(
            alert_id=alert_id,
            identity=request.user,
            name=data.name,
            enabled=data.enabled,
            threshold=data.threshold,
            keyword=data.keyword,
        )

    @delete(
        path="/{alert_id:uuid}",
        summary="Delete an alert",
        description="Delete an existing alert",
        status_code=HTTP_204_NO_CONTENT,
    )
    async def delete_alert(
        self,
        alert_id: UUID,
        request: Request[Identity, AuthToken, Any],
        alert_service: AlertService,
    ) -> None:
        await alert_service.delete_alert(alert_id, request.user)

    # TODO: Add endpoint to manually trigger alert processing
    # This would use BackgroundTask to send emails asynchronously:
    # @post(
    #     path="/process",
    #     summary="Manually process alerts (admin only)",
    # )
    # async def process_alerts_manual(
    #     self,
    #     alert_service: AlertService,
    # ) -> Response[AlertExecution]:
    #     execution = await alert_service.process_alerts()
    #     return Response(
    #         execution,
    #         background=BackgroundTask(send_alert_emails, ...)
    #     )
    
    @get(
        path="/types",
        summary="Get available alert types",
        description="Get list of available alert types and their descriptions",
    )
    async def get_alert_types(self) -> dict:
        return {
            "types": [
                {
                    "value": AlertType.NARRATIVE_VIEWS.value,
                    "description": "Alert when narrative views exceed threshold",
                    "requires": ["threshold"],
                    "scopes": ["general", "specific"],
                },
                {
                    "value": AlertType.NARRATIVE_CLAIMS_COUNT.value,
                    "description": "Alert when narrative claims count exceeds threshold",
                    "requires": ["threshold"],
                    "scopes": ["general", "specific"],
                },
                {
                    "value": AlertType.NARRATIVE_VIDEOS_COUNT.value,
                    "description": "Alert when narrative videos count exceeds threshold",
                    "requires": ["threshold"],
                    "scopes": ["general", "specific"],
                },
                {
                    "value": AlertType.NARRATIVE_WITH_TOPIC.value,
                    "description": "Alert when new narrative with specific topic is created",
                    "requires": ["topic_id"],
                    "scopes": ["general"],
                },
                {
                    "value": AlertType.KEYWORD.value,
                    "description": "Alert when narrative contains specific keyword",
                    "requires": ["keyword"],
                    "scopes": ["general"],
                },
            ],
            "scopes": [
                {
                    "value": AlertScope.GENERAL.value,
                    "description": "Apply to all narratives",
                },
                {
                    "value": AlertScope.SPECIFIC.value,
                    "description": "Apply to specific narrative only",
                    "requires": ["narrative_id"],
                },
            ],
        }
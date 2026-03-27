from litestar import Router

from core.integrations.slack.controller import SlackController

integrations_router = Router(
    path="/integrations",
    route_handlers=[
        SlackController,
    ],
)

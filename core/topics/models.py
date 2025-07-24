from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO

from core.models import Topic


class TopicWithStats(Topic):
    narrative_count: int = 0
    claim_count: int = 0


class TopicDTO(PydanticDTO[Topic]):
    config = DTOConfig(
        exclude={
            "id",
            "created_at",
            "updated_at",
        },
    )

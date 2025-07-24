from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO

from core.models import Transcript


class TranscriptDTO(PydanticDTO[Transcript]):
    config = DTOConfig(
        exclude={
            "video_id",
        },
    )

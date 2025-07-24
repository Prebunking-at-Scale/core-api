from uuid import UUID

from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from pydantic import BaseModel

from core.models import Claim, Narrative, Topic, Video


class EnrichedClaim(Claim):
    topics: list[Topic] = []  # Associated topics
    video: Video | None = None  # Video information
    narratives: list[Narrative] = []  # Associated narratives


class VideoClaims(BaseModel):
    video_id: UUID | None
    claims: list[Claim]


class VideoClaimsDTO(PydanticDTO[VideoClaims]):
    config = DTOConfig(
        exclude={
            "video_id",
            "claims.*.created_at",
            "claims.*.updated_at",
        },
    )


class ClaimUpdate(BaseModel):
    entities: list[UUID] = []  # Future entity IDs
    topics: list[UUID] = []  # Topic IDs to associate

from uuid import UUID

from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from pydantic import BaseModel

from core.entities.models import EntityInput
from core.models import Claim, Entity, Narrative, Topic, Video


class EnrichedClaim(Claim):
    topics: list[Topic] = []  
    entities: list[Entity] = [] 
    video: Video | None = None  
    narratives: list[Narrative] = [] 


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
    entities: list[EntityInput] | None = None 
    topics: list[UUID] | None = None 

from datetime import datetime
from typing import Annotated
from uuid import UUID, uuid4

from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from pydantic import BaseModel, ConfigDict, EmailStr, Field


class Organisation(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    id: UUID = Field(default_factory=uuid4)
    display_name: Annotated[str, Field(min_length=2)]
    country_codes: list[Annotated[str, Field(pattern=r"^[A-Z]{3}$")]]
    short_name: Annotated[str, Field(pattern=r"^[a-z0-9\-]+$", min_length=2)]
    deactivated: datetime | None = None


class User(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    id: UUID = Field(default_factory=uuid4)
    display_name: Annotated[str, Field(min_length=2)]
    email: EmailStr


class OrganisationInvite(BaseModel):
    user_email: EmailStr
    as_admin: bool = False


class OrganisationUpdate(PydanticDTO[Organisation]):
    config = DTOConfig(
        partial=True,
        include={"display_name", "country_code", "deactivated"},
    )

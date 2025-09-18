from dataclasses import dataclass
from datetime import datetime
from typing import Annotated
from uuid import UUID, uuid4

from litestar.dto import DTOConfig
from litestar.plugins.pydantic import PydanticDTO
from litestar.security.jwt import Token
from pydantic import BaseModel, ConfigDict, EmailStr, Field, SecretStr


class Organisation(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    id: UUID = Field(default_factory=uuid4)
    display_name: Annotated[str, Field(min_length=2, examples=["Full Fact"])]
    country_codes: list[Annotated[str, Field(pattern=r"^[A-Z]{3}$")]] = Field(
        examples=[["GBR"]]
    )
    language: Annotated[str, Field(pattern=r"^[a-z]{2}$", examples=["en", "es"])]
    short_name: Annotated[
        str, Field(pattern=r"^[a-z0-9\-]+$", min_length=2, examples=["fullfact"])
    ]
    deactivated: Annotated[datetime | None, Field(exclude=True, examples=[None])] = None


class User(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    id: UUID = Field(default_factory=uuid4)
    email: Annotated[str, Field(examples=["auto@fullfact.org"])]
    display_name: Annotated[str, Field(min_length=2, examples=["Will Moy"])]
    password_last_updated: datetime | None = None
    is_super_admin: bool = False


class OrganisationUser(User):
    """User with their organisation membership status"""

    invited: datetime | None = None
    accepted: datetime | None = None
    is_admin: bool = False


class Identity(BaseModel):
    """An identity and roles associated with a specific request. This should
    be used to perform authorization checks for requests that require one"""

    user: User
    organisation: Organisation | None = None
    is_organisation_admin: bool = False


@dataclass
class AuthToken(Token):
    """AuthToken extends a standard jwt token to include PAS specific details"""

    is_api_user: bool = False
    is_password_reset: bool = False
    organisation_id: str | None = None
    is_super_admin_override: bool = False


class OrganisationInvite(BaseModel):
    """OrganisationInvite includes the details required to invite a user to an
    organisation"""

    user_email: EmailStr
    as_admin: bool = False


class Login(BaseModel):
    email: str
    password: Annotated[SecretStr, Field(min_length=12)]


class PasswordChange(BaseModel):
    new_password: Annotated[SecretStr, Field(min_length=12)]


class AdminStatus(BaseModel):
    is_admin: bool


class SuperAdminStatus(BaseModel):
    is_super_admin: bool


class OrganisationToken(BaseModel):
    organisation: Organisation
    token: str
    is_organisation_admin: bool = False


class LoginOptions(BaseModel):
    user: User
    organisations: dict[UUID, OrganisationToken]
    first_time_setup: bool = False


class OrganisationCreateDTO(PydanticDTO[Organisation]):
    config = DTOConfig(
        partial=True,
        exclude={
            "id",
            "deactivated",
        },
    )


class OrganisationUpdateDTO(PydanticDTO[Organisation]):
    config = DTOConfig(
        partial=True,
        include={
            "display_name",
            "country_code",
            "language",
        },
    )


class UserUpdateDTO(PydanticDTO[User]):
    config = DTOConfig(
        partial=True,
        include={
            "display_name",
        },
    )

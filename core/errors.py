from litestar.exceptions import HTTPException


class NotAuthorizedError(HTTPException):
    status_code = 401


class InvalidInviteError(HTTPException):
    status_code = 403


class NotFoundError(HTTPException):
    status_code = 404


class ConflictError(HTTPException):
    status_code = 409


class OrganisationIDRequiredError(HTTPException):
    status_code = 400
    detail = "organisation_id must be provided for this endpoint"

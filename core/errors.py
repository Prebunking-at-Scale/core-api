from litestar.exceptions import HTTPException


class ConflictError(HTTPException):
    status_code = 409

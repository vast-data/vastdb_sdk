import logging
from enum import Enum

class HttpErrors(Enum):
    SUCCESS = 200
    BAD_REQUEST = 400
    FOBIDDEN = 403
    NOT_FOUND = 404
    METHOD_NOT_ALLOW = 405
    REQUEST_TIMEOUT = 408
    CONFLICT = 409
    INTERNAL_SERVER_ERROR = 500
    NOT_IMPLEMENTED = 501
    SERVICE_UNAVAILABLE = 503

log = logging.getLogger(__name__)

class VastException(Exception):
    pass


class NotFoundError(VastException):
    pass


class AccessDeniedError(VastException):
    pass


class InvalidArgumentError(VastException):
    pass


class BadRequest(VastException):
    pass


class MethodNotAllowed(VastException):
    pass


class RequestTimeout(VastException):
    pass


class Conflict(VastException):
    pass


class InternalServerError(VastException):
    pass


class NotImplemented(VastException):
    pass


class ServiceUnavailable(VastException):
    pass

class ImportFilesError(Exception):
    def __init__(self, message, error_dict):
        super().__init__(message)
        self.error_dict = error_dict

import logging
import xml.etree.ElementTree
from dataclasses import dataclass
from enum import Enum

import requests


class HttpStatus(Enum):
    SUCCESS = 200
    BAD_REQUEST = 400
    FOBIDDEN = 403
    NOT_FOUND = 404
    METHOD_NOT_ALLOWED = 405
    REQUEST_TIMEOUT = 408
    CONFLICT = 409
    INTERNAL_SERVER_ERROR = 500
    NOT_IMPLEMENTED = 501
    SERVICE_UNAVAILABLE = 503
    INSUFFICIENT_CAPACITY = 507


log = logging.getLogger(__name__)


@dataclass
class HttpError(Exception):
    code: str
    message: str
    method: str
    url: str
    status: int  # HTTP status
    headers: requests.structures.CaseInsensitiveDict  # HTTP response headers

    def __post_init__(self):
        self.args = [vars(self)]


class NotFound(HttpError):
    pass


class Forbidden(HttpError):
    pass


class BadRequest(HttpError):
    pass


class MethodNotAllowed(HttpError):
    pass


class RequestTimeout(HttpError):
    pass


class Conflict(HttpError):
    pass


class InternalServerError(HttpError):
    pass


class NotImplemented(HttpError):
    pass


class ServiceUnavailable(HttpError):
    pass


class Slowdown(ServiceUnavailable):
    pass


class UnexpectedError(HttpError):
    pass


class InsufficientCapacity(HttpError):
    pass


@dataclass
class ImportFilesError(Exception):
    message: str
    error_dict: dict


class InvalidArgument(Exception):
    pass


class TooLargeRequest(InvalidArgument):
    pass


class TooWideRow(TooLargeRequest):
    pass


class Missing(Exception):
    pass


class MissingTransaction(Missing):
    pass


class MissingRowIdColumn(Missing):
    pass


class NotSupported(Exception):
    pass


@dataclass
class MissingBucket(Missing):
    bucket: str


@dataclass
class MissingSnapshot(Missing):
    bucket: str
    snapshot: str


@dataclass
class MissingSchema(Missing):
    bucket: str
    schema: str


@dataclass
class MissingTable(Missing):
    bucket: str
    schema: str
    table: str


@dataclass
class MissingProjection(Missing):
    bucket: str
    schema: str
    table: str
    projection: str


class Exists(Exception):
    pass


@dataclass
class SchemaExists(Exists):
    bucket: str
    schema: str


@dataclass
class TableExists(Exists):
    bucket: str
    schema: str
    table: str


@dataclass
class NotSupportedCommand(NotSupported):
    bucket: str
    schema: str
    table: str


@dataclass
class NotSupportedVersion(NotSupported):
    err_msg: str
    version: str


@dataclass
class ConnectionError(Exception):
    cause: Exception
    may_retry: bool


def handle_unavailable(**kwargs):
    if kwargs['code'] == 'SlowDown':
        raise Slowdown(**kwargs)
    raise ServiceUnavailable(**kwargs)


ERROR_TYPES_MAP = {
    HttpStatus.BAD_REQUEST: BadRequest,
    HttpStatus.FOBIDDEN: Forbidden,
    HttpStatus.NOT_FOUND: NotFound,
    HttpStatus.METHOD_NOT_ALLOWED: MethodNotAllowed,
    HttpStatus.REQUEST_TIMEOUT: RequestTimeout,
    HttpStatus.CONFLICT: Conflict,
    HttpStatus.INTERNAL_SERVER_ERROR: InternalServerError,
    HttpStatus.NOT_IMPLEMENTED: NotImplemented,
    HttpStatus.SERVICE_UNAVAILABLE: handle_unavailable,
    HttpStatus.INSUFFICIENT_CAPACITY: InsufficientCapacity,
}


def from_response(res: requests.Response):
    if res.status_code == HttpStatus.SUCCESS.value:
        return None

    log.debug("response: url='%s', code=%s, headers=%s, body='%s'", res.request.url, res.status_code, res.headers, res.text)
    # try to parse S3 XML response for the error details:
    code_str = None
    message_str = None
    if res.text:
        try:
            root = xml.etree.ElementTree.fromstring(res.text)
            code = root.find('Code')
            code_str = code.text if code is not None else None
            message = root.find('Message')
            message_str = message.text if message is not None else None
        except xml.etree.ElementTree.ParseError:
            log.debug("invalid XML: %r", res.text)

    kwargs = dict(
        code=code_str,
        message=message_str,
        method=res.request.method,
        url=res.request.url,
        status=res.status_code,
        headers=res.headers,
    )
    log.warning("RPC failed: %s", kwargs)
    status = HttpStatus(res.status_code)
    error_type = ERROR_TYPES_MAP.get(status, UnexpectedError)
    return error_type(**kwargs)  # type: ignore

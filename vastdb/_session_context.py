import logging
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Event, Lock

from urllib3.util import parse_url

from vastdb.config import SessionConfig
from vastdb.errors import ApiResponseError

from ._internal import VastdbApi

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Endpoint:
    """Endpoint with optional what cnode it relates to."""

    url: str
    cnode: str | None = None


@dataclass(frozen=True)
class _UrlTemplate:
    scheme: str
    port: int

    @staticmethod
    def from_base_endpoint(endpoint: str) -> "_UrlTemplate":
        url = parse_url(endpoint)

        port = url.port
        if port is None:
            port = 443 if url.scheme == "https" else 80

        return _UrlTemplate(scheme=url.scheme or "http",
                          port=port)

    def create(self, hostname: str) -> str:
        return f"{self.scheme}://{hostname}:{self.port}"


def _parse_env_from_rcf_header(rcf_header_value: str) -> str | None:
    val, _ = rcf_header_value.split(":", maxsplit=1)

    if not re.fullmatch(r"E\d+", val):
        raise ApiResponseError(
            f"Unexpected RCF value format: '{val}'. Expected format matching 'E\\d+' (e.g., 'E123')."
        )

    return val


def _get_vip_cnode(api: VastdbApi, vip_url: str) -> str | None:
    api_for_vip = api.with_endpoint(vip_url)
    tx_resp = api_for_vip.begin_transaction()

    try:
        txid = int(tx_resp.headers["tabular-txid"])

        rcf_value = tx_resp.headers.get("x-vast-rcf-id")
        if rcf_value is None:
            return None

        return _parse_env_from_rcf_header(rcf_value)
    finally:
        api_for_vip.rollback_transaction(txid)


def _discover_cnodes(api: VastdbApi, vips: list[str], url_builder: _UrlTemplate) -> list[Endpoint]:
    endpoints = []

    early_cancel_event = Event()

    def _resolve_vip(vip: str) -> Endpoint:
        vip_url = url_builder.create(vip)

        if early_cancel_event.is_set():
            return Endpoint(url=vip_url)

        cnode = _get_vip_cnode(api, vip_url)

        if cnode is None:
            early_cancel_event.set()

        return Endpoint(url=vip_url, cnode=cnode)

    with ThreadPoolExecutor() as executor:
        endpoints = list(executor.map(_resolve_vip, vips))

    # if even one endpoint failed cnode resolution we do not use any cnode resolution
    if early_cancel_event.is_set():
        log.warning(
            "Data Endpoint discovery ended partially. Only using endpoints without CNode mapping. This might prevent certain optimizations.")
        return [Endpoint(url=endpoint.url) for endpoint in endpoints]

    return endpoints


@dataclass(frozen=True)
class _DataEndpoints:
    endpoints: list[Endpoint]

    def __post_init__(self):
        """Post init."""
        if len(self.endpoints) == 0:
            raise ValueError("data_endpoints cannot be empty")

    def require_endpoints(self) -> list[Endpoint]:
        return self.endpoints


class _DiscoveredEndpoints:
    def __init__(self) -> None:
        self._endpoints: list[Endpoint] | None = None
        self._discovery_lock = Lock()

    def discover_cnodes(self, api: VastdbApi, vips: list[str], url_template: _UrlTemplate):
        if self._endpoints is not None:
            return

        with self._discovery_lock:
            if self._endpoints is None:
                self._endpoints = _discover_cnodes(api, vips, url_template)

    @property
    def endpoints(self) -> list[Endpoint] | None:
        return self._endpoints

    def require_endpoints(self) -> list[Endpoint]:
        if self._endpoints is None:
            raise RuntimeError("Discovered endpoints not loaded as expected")

        return self._endpoints


class SessionContext:
    def __init__(self,
                 api: VastdbApi,
                 endpoint: str,
                 config: SessionConfig):
        self._api = api
        self.endpoint = endpoint
        self._url_template = _UrlTemplate.from_base_endpoint(endpoint)

        self._data_endpoints: _DataEndpoints | _DiscoveredEndpoints = (
            _DataEndpoints([Endpoint(url) for url in config.data_endpoints])
            if config.data_endpoints is not None
            else _DiscoveredEndpoints()
        )

    def discover_cnodes(self, vips: list[str]):
        if len(vips) == 0:
            log.warning("No vips returned from server. Discovery did not work as expected. Using single endpoint")
            self._data_endpoints = _DataEndpoints([Endpoint(self.endpoint)])

        if (
                isinstance(self._data_endpoints, _DiscoveredEndpoints) and
                self._data_endpoints.endpoints is None
            ):
            self._data_endpoints.discover_cnodes(self._api,
                                                vips,
                                                url_template=self._url_template)

    def require_data_endpoints(self) -> list[Endpoint]:
        return self._data_endpoints.require_endpoints()

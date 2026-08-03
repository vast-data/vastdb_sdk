import contextlib
import logging
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from itertools import cycle

import pytest

import vastdb.config
import vastdb.errors
from vastdb._internal import UnsupportedServer
from vastdb.session import Session

log = logging.getLogger(__name__)


def test_hello_world(session: Session):
    with session.transaction() as tx:
        assert tx.txid is not None


def test_bad_credentials(session: Session):
    with pytest.raises(vastdb.errors.Forbidden):
        vastdb.connect(access='BAD', secret='BAD', endpoint=str(session.api.next_endpoint()))


def test_bad_endpoint(session: Session):
    backoff_config = vastdb.config.BackoffConfig(max_tries=3)
    with pytest.raises(vastdb.errors.ConnectionError):
        vastdb.connect(access='BAD', secret='BAD', endpoint='http://invalid-host-name-for-tests:12345', backoff_config=backoff_config)


def test_version_extraction():
    # A list of version and expected version parsed by API
    TEST_CASES = [
            ("nginx", UnsupportedServer),                               # non-vast server
            ("vast", NotImplementedError),                              # vast server without version in header
            ("vast 5", NotImplementedError),                            # major
            ("vast 5.2", NotImplementedError),                          # major.minor
            ("vast 5.2.0", NotImplementedError),                        # major.minor.patch
            ("vast 5.2.0.10", (5, 2, 0, 10)),                           # major.minor.patch.protocol
            ("vast 5.2.0.10 some other things", NotImplementedError),   # suffix
            ("vast 5.2.0.10.20", NotImplementedError),                  # extra version
    ]

    # Mock OPTIONS handle that cycles through the test cases response
    class MockOptionsHandler(BaseHTTPRequestHandler):
        versions_iterator = cycle(TEST_CASES)

        def __init__(self, *args) -> None:
            super().__init__(*args)

        def do_GET(self):
            if urllib.parse.urlparse(self.path).query != "transaction":
                self.send_error(404)
                return

            self._server_header = next(self.versions_iterator)[0]
            self.send_response(200)
            self.end_headers()

        def version_string(self):
            return self._server_header

        def log_message(self, format, *args):
            log.debug(format, *args)

    # start the server on localhost on some available port port
    server_address = ('localhost', 0)
    httpd = HTTPServer(server_address, MockOptionsHandler)

    def start_http_server_in_thread():
        log.info(f"Mock HTTP server is running on port {httpd.server_port}")
        httpd.serve_forever()
        log.info("Mock HTTP server killed")

    # start the server in a thread so we have the main thread to operate the API
    server_thread = threading.Thread(target=start_http_server_in_thread)
    server_thread.start()

    try:
        for _, expected in TEST_CASES:
            manager = contextlib.nullcontext()
            if isinstance(expected, type) and issubclass(expected, NotImplementedError):
                manager = pytest.raises(expected)
            with manager:
                s = vastdb.connect(endpoint=f"http://localhost:{httpd.server_port}", access="abc", secret="abc")
                assert s.api.vast_version == expected
    finally:
        # make sure we shut the server down no matter what
        httpd.shutdown()

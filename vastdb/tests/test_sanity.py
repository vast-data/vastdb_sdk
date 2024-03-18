import logging

import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from vastdb import api
from itertools import cycle

log = logging.getLogger(__name__)

def test_hello_world(rpc):
    with rpc.transaction() as tx:
        assert tx.txid is not None

def test_version_extraction():
    # A list of version and expected version parsed by API
    TEST_CASES = [
            (None, None), # vast server without version in header
            ("5", None),                                    # major only is not supported
            ("5.2", "5.2"),                                 # major.minor
            ("5.2.0", "5.2.0"),                             # major.minor.patch
            ("5.2.0.0", "5.2.0.0"),                         # major.minor.patch.protocol
            ("5.2.0.0 some other things", "5.2.0.0"),       # Test forward comptibility 1
            ("5.2.0.0.20 some other things", "5.2.0.0"),    # Test forward comptibility 2
    ]

    # Mock OPTIONS handle that cycles through the test cases response
    class MockOptionsHandler(BaseHTTPRequestHandler):
        versions_iterator = cycle(TEST_CASES)

        def __init__(self, *args) -> None:
            super().__init__(*args)

        def do_OPTIONS(self):
            self.send_response(204)
            self.end_headers()

        def version_string(self):
            version = next(self.versions_iterator)[0]
            return f"vast {version}" if version else "vast"

        def log_message(self, format, *args):
            log.debug(format,*args)

    # start the server on localhost on some available port port
    server_address =('localhost', 0)
    httpd = HTTPServer(server_address, MockOptionsHandler)

    def start_http_server_in_thread():
        log.info(f"Mock HTTP server is running on port {httpd.server_port}")
        httpd.serve_forever()
        log.info("Mock HTTP server killed")

    # start the server in a thread so we have the main thread to operate the API
    server_thread = threading.Thread(target=start_http_server_in_thread)
    server_thread.start()

    try:
        for test_case in TEST_CASES:
            tester = api.VastdbApi(endpoint=f"http://localhost:{httpd.server_port}", access_key="abc", secret_key="abc")
            assert tester.vast_version == test_case[1]
    finally:
        # make sure we shut the server down no matter what
        httpd.shutdown()

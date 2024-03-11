from vastdb import v2

import logging


log = logging.getLogger(__name__)


def test_hello_world():
    with v2.context() as ctx:
        log.info("Hello World!")

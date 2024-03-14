import logging

log = logging.getLogger(__name__)


def test_hello_world(rpc):
    with rpc.transaction() as tx:
        assert tx.txid is not None

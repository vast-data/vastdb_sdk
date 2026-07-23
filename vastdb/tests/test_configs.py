from typing import Callable

import pyarrow as pa
import pytest

from vastdb import TableRef
from vastdb.config import SessionConfig
from vastdb.session import Session
from vastdb.table import Table, TableInTransaction
from vastdb.table_metadata import TableMetadata


@pytest.mark.parametrize('session_config, expected_endpoints', [
    (SessionConfig(data_endpoints=["http://localhost:9090", "http://localhost:9090", "http://localhost:9090"]),
     ["http://localhost:9090", "http://localhost:9090", "http://localhost:9090"]),
    (SessionConfig(data_endpoints=None), ["http://localhost:9090"]),
    (None, ["http://localhost:9090"])
])
def test_session_data_endpoints_propagate_table_in_tx(session_provider: Callable[..., Session],
                                                      session_config: SessionConfig,
                                                      expected_endpoints: list[str]):
    session = session_provider(session_config)
    table_md = TableMetadata(TableRef("b", "s", "t"))
    with session.transaction() as tx:
        t = tx.table_from_metadata(table_md)
        assert isinstance(t, TableInTransaction)
        assert t._session_context.data_endpoints == expected_endpoints


@pytest.mark.parametrize('session_config, expected_endpoints', [
    (SessionConfig(data_endpoints=["http://localhost:9090", "http://localhost:9090", "http://localhost:9090"]),
     ["http://localhost:9090", "http://localhost:9090", "http://localhost:9090"]),
    (SessionConfig(data_endpoints=None), ["http://localhost:9090"]),
    (None, ["http://localhost:9090"])
])
def test_session_data_endpoints_propagate_legacy_table(session_provider: Callable[..., Session],
                                                       clean_bucket_name,
                                                       session_config: SessionConfig,
                                                       expected_endpoints: list[str]):
    session = session_provider(session_config)
    with session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).create_schema("s").create_table(
            "t", columns=pa.schema([('l', pa.list_(pa.int8()))]))
        assert isinstance(t, Table)
        assert t._session_context.data_endpoints == expected_endpoints

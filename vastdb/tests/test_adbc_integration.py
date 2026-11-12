from typing import Optional
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from vastdb._adbc import END_USER_PROPERTY, QE_SENTINEL_FOR_DEFAULT_VALUE, AdbcConnection
from vastdb.session import Session
from vastdb.table_metadata import TableRef
from vastdb.transaction import TransactionNotActiveError


def test_sanity(session, clean_bucket_name: str):
    arrow_schema = pa.schema([("n", pa.int32())])

    ref = TableRef(clean_bucket_name, "s", "t")
    data_table = pa.table(schema=arrow_schema, data=[[1, 2, 3, 4, 5]])

    with session.transaction() as tx:
        table = (
            tx.bucket(clean_bucket_name)
            .create_schema("s")
            .create_table("t", arrow_schema)
        )
        table.insert(data_table)

    with session.transaction() as tx:
        tx.adbc_conn.cursor.execute(f"SELECT * FROM {ref.query_engine_full_path}")
        res = tx.adbc_conn.cursor.fetchall()

        assert res == [(1,), (2,), (3,), (4,), (5,)]


def test_adbc_shares_tx(session, clean_bucket_name: str):
    arrow_schema = pa.schema([("n", pa.int32())])

    data_table = pa.table(schema=arrow_schema, data=[[1, 2, 3, 4, 5]])

    with session.transaction() as tx:
        table = (
            tx.bucket(clean_bucket_name)
            .create_schema("s")
            .create_table("t", arrow_schema)
        )
        table.insert(data_table)

        # expecting adbc execute to "see" table if it shares the transaction with the pysdk
        tx.adbc_conn.cursor.execute(f"SELECT * FROM {table.ref.query_engine_full_path}")
        assert tx.adbc_conn.cursor.fetchall() == [(1,), (2,), (3,), (4,), (5,)]


def test_adbc_conn_unreachable_tx_close(session):
    with session.transaction() as tx:
        assert tx.adbc_conn is not None

    # adbc conn should not be reachable after tx close
    with pytest.raises(TransactionNotActiveError):
        tx.adbc_conn


def test_adbc_conn_not_opened_until_accessed(session):
    with session.transaction() as tx:
        assert tx._adbc_conn is None
        assert tx.adbc_conn is not None
        assert tx._adbc_conn is not None


def test_vector_search_adbc_conn_is_separate_from_user_adbc_conn():
    with (
        patch("vastdb.transaction.AdbcConnection") as MockAdbcConnection,
        patch("vastdb._internal.VastdbApi") as MockVastdbApi,
    ):
        mock_api_instance = MockVastdbApi.return_value
        mock_api_instance.begin_transaction.return_value.headers = {
            "tabular-txid": "12345"
        }
        mock_api_instance.vast_version = (5, 4, 0, 0)

        user_conn = MagicMock(name="user_adbc_conn")
        search_conn = MagicMock(name="vector_search_adbc_conn")
        MockAdbcConnection.side_effect = [user_conn, search_conn]

        session = Session(
            access="test_access",
            secret="test_secret",
            endpoint="http://localhost:9090",
            adbc_driver_path="/mock/driver/path",
        )

        with session.transaction() as tx:
            assert tx._adbc_conn is None
            assert tx._vector_search_adbc_conn is None

            assert tx.adbc_conn is user_conn
            assert tx._vector_search_adbc_conn is None

            assert tx._vector_search_conn is search_conn
            assert tx.adbc_conn is user_conn
            assert tx._adbc_conn is not tx._vector_search_adbc_conn

        user_conn.close.assert_called_once()
        search_conn.close.assert_called_once()


def test_two_simulatnious_txs_with_adbc(session, clean_bucket_name: str):
    arrow_schema = pa.schema([("n", pa.int32())])

    data_table = pa.table(schema=arrow_schema, data=[[1, 2, 3, 4, 5]])

    with session.transaction() as tx:
        table = (
            tx.bucket(clean_bucket_name)
            .create_schema("s")
            .create_table("t1", arrow_schema)
        )
        table.insert(data_table)

        # expecting adbc execute to "see" table if it shares the transaction with the pysdk
        tx.adbc_conn.cursor.execute(f"SELECT * FROM {table.ref.query_engine_full_path}")
        assert tx.adbc_conn.cursor.fetchall() == [(1,), (2,), (3,), (4,), (5,)]

    with session.transaction() as tx:
        table = (
            tx.bucket(clean_bucket_name).schema("s").create_table("t2", arrow_schema)
        )
        table.insert(data_table)

        # expecting adbc execute to "see" table if it shares the transaction with the pysdk
        tx.adbc_conn.cursor.execute(f"SELECT * FROM {table.ref.query_engine_full_path}")
        assert tx.adbc_conn.cursor.fetchall() == [(1,), (2,), (3,), (4,), (5,)]


@pytest.mark.parametrize("end_user", [("mock-end-user",), (None,)])
def test_end_user_passed_to_adbc_connect(end_user: Optional[str]):
    with (
        patch("vastdb._adbc.connect") as mock_connect,
        patch("vastdb._internal.VastdbApi") as MockVastdbApi,
    ):
        mock_api_instance = MockVastdbApi.return_value
        mock_api_instance.begin_transaction.return_value.headers = {
            "tabular-txid": "12345"
        }
        # A version that supports everything needed.
        mock_api_instance.vast_version = (5, 4, 0, 0)

        session = Session(
            access="test_access",
            secret="test_secret",
            endpoint="http://localhost:9090",
            end_user=end_user,
            adbc_driver_path="/mock/driver/path"
        )

        with session.transaction() as tx:
            # ADBC connection is established lazily on first access
            mock_connect.assert_not_called()
            _ = tx.adbc_conn

        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args.kwargs
        conn_kwargs = call_kwargs.get("conn_kwargs", {})
        assert (end_user is None) ^ (END_USER_PROPERTY in conn_kwargs)
        assert (end_user is None) ^ (conn_kwargs.get(END_USER_PROPERTY) == end_user)


def test_search_options_reset_omitted_keys_to_qe_default_sentinel():
    mock_dbapi_conn = MagicMock()
    mock_adbc_connection = MagicMock()
    mock_dbapi_conn.adbc_connection = mock_adbc_connection

    with patch("vastdb._adbc._get_adbc_connection", return_value=mock_dbapi_conn):
        conn = AdbcConnection(
            endpoint="http://localhost:9090",
            access_key="ak",
            secret_key="sk",
            txid=1,
            adbc_driver_path="/mock/driver/path",
        )

    conn._apply_search_options({"opt_a": "1", "opt_b": "2"})
    mock_adbc_connection.set_options.assert_called_with(opt_a="1", opt_b="2")

    mock_adbc_connection.set_options.reset_mock()
    conn._apply_search_options({"opt_a": "3"})
    mock_adbc_connection.set_options.assert_called_once()
    applied = mock_adbc_connection.set_options.call_args.kwargs
    assert applied == {"opt_a": "3", "opt_b": QE_SENTINEL_FOR_DEFAULT_VALUE}

    mock_adbc_connection.set_options.reset_mock()
    conn._apply_search_options(None)
    applied = mock_adbc_connection.set_options.call_args.kwargs
    assert applied == {
        "opt_a": QE_SENTINEL_FOR_DEFAULT_VALUE,
        "opt_b": QE_SENTINEL_FOR_DEFAULT_VALUE,
    }


def test_search_options_noop_when_nothing_to_apply():
    mock_dbapi_conn = MagicMock()
    mock_adbc_connection = MagicMock()
    mock_dbapi_conn.adbc_connection = mock_adbc_connection

    with patch("vastdb._adbc._get_adbc_connection", return_value=mock_dbapi_conn):
        conn = AdbcConnection(
            endpoint="http://localhost:9090",
            access_key="ak",
            secret_key="sk",
            txid=1,
            adbc_driver_path="/mock/driver/path",
        )

    conn._apply_search_options(None)
    mock_adbc_connection.set_options.assert_not_called()

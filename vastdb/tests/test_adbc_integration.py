import pyarrow as pa
import pytest

from vastdb.table_metadata import TableRef
from vastdb.transaction import NoAdbcConnectionError


def test_sanity(session_factory, clean_bucket_name: str):
    session = session_factory(with_adbc=True)

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


def test_adbc_shares_tx(session_factory, clean_bucket_name: str):
    session = session_factory(with_adbc=True)

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


def test_adbc_conn_unreachable_tx_close(session_factory):
    session = session_factory(with_adbc=True)

    with session.transaction() as tx:
        assert tx.adbc_conn is not None

    # adbc conn should not be reachable after tx close
    with pytest.raises(NoAdbcConnectionError):
        tx.adbc_conn


def test_two_simulatnious_txs_with_adbc(session_factory, clean_bucket_name: str):
    session = session_factory(with_adbc=True)

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

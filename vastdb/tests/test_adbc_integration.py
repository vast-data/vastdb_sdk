import pyarrow as pa

from vastdb.table_metadata import TableRef


def test_sanity(session_factory, clean_bucket_name: str):
    session = session_factory(with_adbc=True)

    arrow_schema = pa.schema([('n', pa.int32())])

    ref = TableRef(clean_bucket_name, 's', 't')
    data_table = pa.table(schema=arrow_schema, data=[[1, 2, 3, 4, 5]])

    with session.transaction() as tx:
        table = tx.bucket(clean_bucket_name).create_schema(
            's').create_table('t', arrow_schema)
        table.insert(data_table)

    # TODO merge in same tx
    with session.transaction() as tx:
        tx.adbc_conn.cursor.execute(f"SELECT * FROM {ref.query_engine_full_path}")
        res = tx.adbc_conn.cursor.fetchall()

        assert res == [(1,), (2,), (3,), (4,), (5,)]

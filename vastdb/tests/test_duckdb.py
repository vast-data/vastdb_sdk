import logging

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from .util import prepare_data

log = logging.getLogger(__name__)


def test_duckdb(session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int32()),
        ('b', pa.float64()),
    ])
    data = pa.table(schema=columns, data=[
        [111, 222, 333],
        [0.5, 1.5, 2.5],
    ])
    with prepare_data(session, clean_bucket_name, 's', 't', data) as t:
        conn = duckdb.connect()
        batches = t.select(columns=['a'], predicate=(t['b'] < 2))  # noqa: F841
        actual = conn.execute('SELECT max(a) as "a_max" FROM batches').arrow()
        expected = (data
            .filter(pc.field('b') < 2)
            .group_by([])
            .aggregate([('a', 'max')]))
        assert actual == expected


# def test_closed_tx(session, clean_bucket_name):
#     assert duckdb.__version__ == "1.0.0", "doesn't reproduce with newer duckdb versions, when updating duckdb in tests/when relevant need to update this test accordingly."

#     columns = pa.schema([
#         ('a', pa.int64()),
#     ])
#     data = pa.table(schema=columns, data=[
#         list(range(10000)),
#     ])

#     with session.transaction() as tx:
#         t = tx.bucket(clean_bucket_name).create_schema("s1").create_table("t1", columns)
#         t.insert(data)

#         config = QueryConfig(
#             num_sub_splits=1,
#             num_splits=1,
#             num_row_groups_per_sub_split=1,
#             limit_rows_per_sub_split=100)
#         batches = t.select(config=config)  # noqa: F841
#         first = next(batches)  # make sure that HTTP response processing has started
#         assert first['a'].to_pylist() == list(range(100))

#         conn = duckdb.connect()
#         res = conn.execute('SELECT a FROM batches')
#         log.debug("closing tx=%s after first batch=%s", t.tx, first)

#     # transaction is closed, collecting the result should fail internally in DuckDB
#     with pytest.raises(duckdb.InvalidInputException):
#         res.arrow()

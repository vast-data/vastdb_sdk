import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from tempfile import NamedTemporaryFile
from contextlib import contextmanager, closing
from vastdb.v2 import INTERNAL_ROW_ID

import logging

log = logging.getLogger(__name__)

@contextmanager
def prepare_data(rpc, clean_bucket_name, schema_name, table_name, arrow_table):
    with rpc.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(table_name, arrow_table.schema)
        rb = t.insert(arrow_table)
        log.debug("row_ids=%s" % rb.to_pydict()[INTERNAL_ROW_ID])
        assert rb.num_rows == arrow_table.num_rows
        yield t
        t.drop()
        s.drop()

log = logging.getLogger(__name__)

def test_tables(rpc, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int16()),
        ('b', pa.float32()),
        ('s', pa.utf8()),
    ])
    expected = pa.table(schema=columns, data=[
        [111, 222, 333],
        [0.5, 1.5, 2.5],
        ['a', 'bb', 'ccc'],
    ])
    with prepare_data(rpc, clean_bucket_name, 's', 't', expected) as t:
        actual = pa.Table.from_batches(t.select(columns=['a', 'b', 's']))
        assert actual == expected

        actual = pa.Table.from_batches(t.select())
        assert actual == expected

        actual = pa.Table.from_batches(t.select(columns=['a', 'b']))
        assert actual == expected.select(['a', 'b'])

        actual = pa.Table.from_batches(t.select(columns=['b', 's', 'a']))
        assert actual == expected.select(['b', 's', 'a'])

        actual = pa.Table.from_batches(t.select(columns=['s']))
        assert actual == expected.select(['s'])

        actual = pa.Table.from_batches(t.select(columns=[]))
        assert actual == expected.select([])

        actual = pa.Table.from_batches(t.select(columns=['s'], internal_row_id=True))
        log.debug("actual=%s", actual)
        assert actual.to_pydict() == {
            's': ['a', 'bb', 'ccc'],
            INTERNAL_ROW_ID: [0, 1, 2]
        }

        columns_to_update = pa.schema([
            (INTERNAL_ROW_ID, pa.uint64()),
            ('a', pa.int16())
        ])

        rb = pa.record_batch(schema=columns_to_update, data=[
            [0, 2],  # update rows 0,2
            [1110, 3330]
        ])

        t.update(rb)
        actual = pa.Table.from_batches(t.select(columns=['a', 'b']))
        assert actual.to_pydict() == {
            'a': [1110, 222, 3330],
            'b': [0.5, 1.5, 2.5]
        }

        columns_to_delete = pa.schema([(INTERNAL_ROW_ID, pa.uint64())])
        rb = pa.record_batch(schema=columns_to_delete, data=[[0, 1]])  # delete rows 0,1

        t.delete(rb)
        actual = pa.Table.from_batches(t.select(columns=['a', 'b', 's']))
        assert actual.to_pydict() == {
            'a': [3330],
            'b': [2.5],
            's': ['ccc']
        }


def test_filters(rpc, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int32()),
        ('b', pa.float64()),
        ('s', pa.utf8()),
    ])
    expected = pa.table(schema=columns, data=[
        [111, 222, 333],
        [0.5, 1.5, 2.5],
        ['a', 'bb', 'ccc'],
    ])
    with prepare_data(rpc, clean_bucket_name, 's', 't', expected) as t:
        def select(predicate):
            return pa.Table.from_batches(t.select(predicate=predicate))

        assert select(None) == expected

        assert select(t['a'] > 222) == expected.filter(pc.field('a') > 222)
        assert select(t['a'] < 222) == expected.filter(pc.field('a') < 222)
        assert select(t['a'] == 222) == expected.filter(pc.field('a') == 222)
        assert select(t['a'] != 222) == expected.filter(pc.field('a') != 222)
        assert select(t['a'] <= 222) == expected.filter(pc.field('a') <= 222)
        assert select(t['a'] >= 222) == expected.filter(pc.field('a') >= 222)

        assert select(t['b'] > 1.5) == expected.filter(pc.field('b') > 1.5)
        assert select(t['b'] < 1.5) == expected.filter(pc.field('b') < 1.5)
        assert select(t['b'] == 1.5) == expected.filter(pc.field('b') == 1.5)
        assert select(t['b'] != 1.5) == expected.filter(pc.field('b') != 1.5)
        assert select(t['b'] <= 1.5) == expected.filter(pc.field('b') <= 1.5)
        assert select(t['b'] >= 1.5) == expected.filter(pc.field('b') >= 1.5)

        assert select(t['s'] > 'bb') == expected.filter(pc.field('s') > 'bb')
        assert select(t['s'] < 'bb') == expected.filter(pc.field('s') < 'bb')
        assert select(t['s'] == 'bb') == expected.filter(pc.field('s') == 'bb')
        assert select(t['s'] != 'bb') == expected.filter(pc.field('s') != 'bb')
        assert select(t['s'] <= 'bb') == expected.filter(pc.field('s') <= 'bb')
        assert select(t['s'] >= 'bb') == expected.filter(pc.field('s') >= 'bb')


def test_duckdb(rpc, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int32()),
        ('b', pa.float64()),
    ])
    data = pa.table(schema=columns, data=[
        [111, 222, 333],
        [0.5, 1.5, 2.5],
    ])
    with prepare_data(rpc, clean_bucket_name, 's', 't', data) as t:
        conn = duckdb.connect()
        batches = t.select(columns=['a'], predicate=(t['b'] < 2))  # noqa: F841
        actual = conn.execute('SELECT max(a) as "a_max" FROM batches').arrow()
        expected = (data
            .filter(pc.field('b') < 2)
            .group_by([])
            .aggregate([('a', 'max')]))
        assert actual == expected


def test_parquet_export(rpc, clean_bucket_name):
    with rpc.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')
        columns = pa.schema([
            ('a', pa.int16()),
            ('b', pa.float32()),
            ('s', pa.utf8()),
        ])
        assert s.tables() == []
        t = s.create_table('t1', columns)
        assert s.tables() == [t]

        rb = pa.record_batch(schema=columns, data=[
            [111, 222],
            [0.5, 1.5],
            ['a', 'b'],
        ])
        expected = pa.Table.from_batches([rb])
        rb = t.insert(rb)
        assert rb.to_pydict() == {
            INTERNAL_ROW_ID: [0, 1]
        }

        actual = pa.Table.from_batches(t.select())
        assert actual == expected

        table_batches = t.select()

        with NamedTemporaryFile() as parquet_file:
            log.info("Writing table into parquet file: '%s'", parquet_file.name)
            with closing(pq.ParquetWriter(parquet_file.name, table_batches.schema)) as parquet_writer:
                for batch in table_batches:
                    parquet_writer.write_batch(batch)

            assert expected == pq.read_table(parquet_file.name)

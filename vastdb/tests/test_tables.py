import pyarrow as pa
import pyarrow.compute as pc
import logging

import pyarrow.parquet as pq
from tempfile import NamedTemporaryFile
from contextlib import contextmanager, closing

log = logging.getLogger(__name__)

@contextmanager
def prepare_data(rpc, clean_bucket_name, schema_name, table_name, arrow_table):
    with rpc.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(table_name, arrow_table.schema)
        t.insert(arrow_table)
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
            '$row_id': [0, 1, 2]
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
        t.insert(rb)

        actual = pa.Table.from_batches(t.select(columns=['a', 'b', 's']))
        assert actual == expected

        table_batches = t.select(columns=['a', 'b', 's'])

        with NamedTemporaryFile() as parquet_file:
            log.info("Writing table into parquet file: '%s'", parquet_file.name)
            with closing(pq.ParquetWriter(parquet_file.name, table_batches.schema)) as parquet_writer:
                for batch in table_batches:
                    parquet_writer.write_batch(batch)

            assert expected == pq.read_table(parquet_file.name)

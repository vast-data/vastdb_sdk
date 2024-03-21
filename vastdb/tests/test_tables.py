import pyarrow as pa
import pyarrow.compute as pc

from contextlib import contextmanager


@contextmanager
def prepare_data(rpc, clean_bucket_name, schema_name, table_name, arrow_table):
    with rpc.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(table_name, arrow_table.schema)
        t.insert(arrow_table)
        yield t
        t.drop()
        s.drop()


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

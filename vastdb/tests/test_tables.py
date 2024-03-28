import duckdb
import pytest
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from tempfile import NamedTemporaryFile
from contextlib import contextmanager, closing

from requests.exceptions import HTTPError
import logging

from vastdb.table import INTERNAL_ROW_ID, QueryConfig
from vastdb.errors import NotFoundError,Conflict


log = logging.getLogger(__name__)


@contextmanager
def prepare_data(session, clean_bucket_name, schema_name, table_name, arrow_table):
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(table_name, arrow_table.schema)
        rb = t.insert(arrow_table)
        row_ids = rb[INTERNAL_ROW_ID].to_pylist()
        log.debug("row_ids=%s" % row_ids)
        assert row_ids == list(range(arrow_table.num_rows))
        yield t
        t.drop()
        s.drop()

log = logging.getLogger(__name__)

def test_tables(session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int64()),
        ('b', pa.float32()),
        ('s', pa.utf8()),
    ])
    expected = pa.table(schema=columns, data=[
        [111, 222, 333],
        [0.5, 1.5, 2.5],
        ['a', 'bb', 'ccc'],
    ])
    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
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

        columns_to_delete = pa.schema([(INTERNAL_ROW_ID, pa.uint64())])
        rb = pa.record_batch(schema=columns_to_delete, data=[[0]])  # delete rows 0,1
        t.delete(rb)

        selected_rows = pa.Table.from_batches(t.select(columns=['b'], predicate=(t['a'] == 222), internal_row_id=True))
        t.delete(selected_rows)
        actual = pa.Table.from_batches(t.select(columns=['a', 'b', 's']))
        assert actual.to_pydict() == {
            'a': [333],
            'b': [2.5],
            's': ['ccc']
        }

def test_update_table(session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int64()),
        ('b', pa.float32()),
        ('s', pa.utf8()),
    ])
    expected = pa.table(schema=columns, data=[
        [111, 222, 333],
        [0.5, 1.5, 2.5],
        ['a', 'bb', 'ccc'],
    ])
    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
        columns_to_update = pa.schema([
            (INTERNAL_ROW_ID, pa.uint64()),
            ('a', pa.int64())
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

        actual = pa.Table.from_batches(t.select(columns=['a', 'b'], predicate=(t['a'] < 1000), internal_row_id=True))
        column_index = actual.column_names.index('a')
        column_field = actual.field(column_index)
        new_data = pc.add(actual.column('a'), 2000)
        update_table = actual.set_column(column_index, column_field, new_data)

        t.update(update_table, columns=['a'])
        actual = pa.Table.from_batches(t.select(columns=['a', 'b']))
        assert actual.to_pydict() == {
            'a': [1110, 2222, 3330],
            'b': [0.5, 1.5, 2.5]
        }

        actual = pa.Table.from_batches(t.select(columns=['a', 'b'], predicate=(t['a'] != 2222), internal_row_id=True))
        column_index = actual.column_names.index('a')
        column_field = actual.field(column_index)
        new_data = pc.divide(actual.column('a'), 10)
        update_table = actual.set_column(column_index, column_field, new_data)

        t.update(update_table.to_batches()[0], columns=['a'])
        actual = pa.Table.from_batches(t.select(columns=['a', 'b']))
        assert actual.to_pydict() == {
            'a': [111, 2222, 333],
            'b': [0.5, 1.5, 2.5]
        }

def test_select_with_multisplits(session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int32())
    ])

    data = [ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    data = data * 1000
    expected = pa.table(schema=columns, data=[data])

    config = QueryConfig()
    config.rows_per_split = 1000

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
        actual = pa.Table.from_batches(t.select(columns=['a'], config=config))
        assert actual == expected

def test_filters(session, clean_bucket_name):
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
    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
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

        assert select((t['a'] > 111) & (t['b'] > 0) & (t['s'] < 'ccc')) == expected.filter((pc.field('a') > 111) & (pc.field('b') > 0) & (pc.field('s') < 'ccc'))
        assert select((t['a'] > 111) & (t['b'] < 2.5)) == expected.filter((pc.field('a') > 111) & (pc.field('b') < 2.5))
        assert select((t['a'] > 111) & (t['a'] < 333)) == expected.filter((pc.field('a') > 111) & (pc.field('a') < 333))


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


def test_parquet_export(session, clean_bucket_name):
    with session.transaction() as tx:
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

def test_errors(session, clean_bucket_name):
    with pytest.raises(NotFoundError):
        with session.transaction() as tx:
            tx.bucket(clean_bucket_name).schema('s1')

    with pytest.raises(NotFoundError):
        with session.transaction() as tx:
            tx.bucket("bla")

    with pytest.raises(Conflict):
        with session.transaction() as tx:
            b = tx.bucket(clean_bucket_name)
            s = b.create_schema('s1')
            columns = pa.schema([
                ('a', pa.int16()),
                ('b', pa.float32()),
                ('s', pa.utf8()),
            ])
            s.create_table('t1', columns)
            s.drop() # cannot drop schema without dropping its tables first

def test_rename_schema(session, clean_bucket_name):

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s')

    with session.transaction() as tx, session.transaction() as tx2:
        b = tx.bucket(clean_bucket_name)
        # assert that there is only one schema in this bucket - pre rename
        assert [s.name for s in b.schemas()] == ['s']

        s = b.schema('s')
        s.rename('ss')

        # assert the table was renamed in the transaction context
        # where it was renamed
        assert s.name == 'ss'
        with pytest.raises(NotFoundError):
            tx.bucket(clean_bucket_name).schema('s')

        # assert that other transactions are isolated
        tx2.bucket(clean_bucket_name).schema('s')
        with pytest.raises(NotFoundError):
            tx2.bucket(clean_bucket_name).schema('ss')

    # assert that new transactions see the updated schema name
    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        with pytest.raises(NotFoundError):
            b.schema('s')
        s = b.schema('ss')
        # assert that we still have only one schema and it is the one that was renamed
        assert [s.name for s in b.schemas()] == ['ss']
        s.drop()


def test_rename_table(session, clean_bucket_name):
    columns = pa.schema([
            ('a', pa.int16()),
            ('b', pa.float32()),
            ('s', pa.utf8()),
        ])
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s')
        t = s.create_table('t', columns)

    with session.transaction() as tx, session.transaction() as tx2:
        s = tx.bucket(clean_bucket_name).schema('s')
        t = s.table('t')
        t.rename('t2')
        # assert that the new table name is seen in the context
        # in which it was renamed
        assert t.name == 't2'
        with pytest.raises(NotFoundError):
            s.table('t')
        t = s.table('t2')

        #assert that other transactions are isolated
        with pytest.raises(NotFoundError):
            tx2.bucket(clean_bucket_name).schema('s').table('t2')
        tx2.bucket(clean_bucket_name).schema('s').table('t')

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema('s')
        #assert that new transactions see the change
        with pytest.raises(NotFoundError):
            s.table('t')
        t = s.table('t2')
        t.drop()
        s.drop()

def test_add_column(session, clean_bucket_name):
    columns = pa.schema([
            ('a', pa.int16()),
            ('b', pa.float32()),
            ('s', pa.utf8()),
        ])
    new_column = pa.field('aa', pa.int16())
    new_schema = columns.append(new_column)

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s')
        s.create_table('t', columns)

    with session.transaction() as tx, session.transaction() as tx2:
        t = tx.bucket(clean_bucket_name).schema('s').table('t')
        assert t.arrow_schema == columns

        t.add_column(pa.schema([new_column]))
        # assert that the column is seen in the context
        # in which it was added
        assert t.arrow_schema == new_schema

        #assert that other transactions are isolated
        assert tx2.bucket(clean_bucket_name).schema('s').table('t').arrow_schema == columns


    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema('s')
        t = s.table('t')
        #assert that new transactions see the change
        assert t.arrow_schema == new_schema
        t.drop()
        s.drop()

def test_drop_column(session, clean_bucket_name):
    columns = pa.schema([
            ('a', pa.int16()),
            ('b', pa.float32()),
            ('s', pa.utf8()),
        ])
    field_idx = columns.get_field_index('a')
    new_schema = columns.remove(field_idx)
    column_to_drop = columns.field(field_idx)

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s')
        s.create_table('t', columns)

    with session.transaction() as tx, session.transaction() as tx2:
        t = tx.bucket(clean_bucket_name).schema('s').table('t')
        assert t.arrow_schema == columns

        t.drop_column(pa.schema([column_to_drop]))
        # assert that the column is seen in the context
        # in which it was added
        assert t.arrow_schema == new_schema

        #assert that other transactions are isolated
        assert tx2.bucket(clean_bucket_name).schema('s').table('t').arrow_schema == columns


    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema('s')
        t = s.table('t')
        #assert that new transactions see the change
        assert t.arrow_schema == new_schema
        t.drop()
        s.drop()

def test_rename_column(session, clean_bucket_name):
    columns = pa.schema([
            ('a', pa.int16()),
            ('b', pa.float32()),
            ('s', pa.utf8()),
        ])
    def prepare_rename_column(schema : pa.Schema, old_name : str, new_name : str) -> pa.Schema:
        field_idx = schema.get_field_index(old_name)
        column_to_rename = schema.field(field_idx)
        renamed_column = column_to_rename.with_name(new_name)
        return schema.set(field_idx, renamed_column)

    new_schema = prepare_rename_column(columns,'a','aaa')

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s')
        s.create_table('t', columns)

    with session.transaction() as tx, session.transaction() as tx2:
        t = tx.bucket(clean_bucket_name).schema('s').table('t')
        assert t.arrow_schema == columns

        t.rename_column('a', 'aaa')
        # assert that the column is seen in the context
        # in which it was added
        assert t.arrow_schema == new_schema

        #assert that other transactions are isolated
        assert tx2.bucket(clean_bucket_name).schema('s').table('t').arrow_schema == columns

    #assert that new transactions see the change
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema('s')
        t = s.table('t')

        assert t.arrow_schema == new_schema

    # simultaneos renames of the same column
    new_schema_tx1 = prepare_rename_column(new_schema, 'b', 'bb')
    new_schema_tx2 = prepare_rename_column(new_schema, 'b', 'bbb')
    with pytest.raises(Conflict):
        with session.transaction() as tx1, session.transaction() as tx2:
            t1 = tx1.bucket(clean_bucket_name).schema('s').table('t')
            t2 = tx2.bucket(clean_bucket_name).schema('s').table('t')
            t1.rename_column('b', 'bb')
            with pytest.raises(HTTPError, match = '409 Client Error: Conflict'):
                t2.rename_column('b', 'bbb')

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema('s')
        t = s.table('t')
        # validate that the rename conflicted and rolled back
        assert (t.arrow_schema != new_schema_tx1) and \
                (t.arrow_schema != new_schema_tx2)

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema('s')
        t = s.table('t')
        t.drop()
        s.drop()

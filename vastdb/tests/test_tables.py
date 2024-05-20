import datetime as dt
import decimal
import logging
import random
import threading
from contextlib import closing
from tempfile import NamedTemporaryFile

import ibis
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from requests.exceptions import HTTPError

from .. import errors
from ..table import INTERNAL_ROW_ID, QueryConfig
from .util import prepare_data

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
        actual = t.select(columns=['a', 'b', 's']).read_all()
        assert actual == expected

        actual = t.select().read_all()
        assert actual == expected

        actual = t.select(columns=['a', 'b']).read_all()
        assert actual == expected.select(['a', 'b'])

        actual = t.select(columns=['b', 's', 'a']).read_all()
        assert actual == expected.select(['b', 's', 'a'])

        actual = t.select(columns=['s']).read_all()
        assert actual == expected.select(['s'])

        actual = t.select(columns=[]).read_all()
        assert actual == expected.select([])

        actual = t.select(columns=['s'], internal_row_id=True).read_all()
        log.debug("actual=%s", actual)
        assert actual.to_pydict() == {
            's': ['a', 'bb', 'ccc'],
            INTERNAL_ROW_ID: [0, 1, 2]
        }

        columns_to_delete = pa.schema([(INTERNAL_ROW_ID, pa.uint64())])
        rb = pa.record_batch(schema=columns_to_delete, data=[[0]])  # delete rows 0,1
        t.delete(rb)

        selected_rows = t.select(columns=['b'], predicate=(t['a'] == 222), internal_row_id=True).read_all()
        t.delete(selected_rows)
        actual = t.select(columns=['a', 'b', 's']).read_all()
        assert actual.to_pydict() == {
            'a': [333],
            'b': [2.5],
            's': ['ccc']
        }


def test_insert_wide_row(session, clean_bucket_name):
    columns = pa.schema([pa.field(f's{i}', pa.utf8()) for i in range(500)])
    data = [['a' * 10**4] for i in range(500)]
    expected = pa.table(schema=columns, data=data)

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
        actual = t.select().read_all()
        assert actual == expected


def test_exists(session, clean_bucket_name):
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')
        assert s.tables() == []

        t = s.create_table('t', pa.schema([('x', pa.int64())]))

        assert s.tables() == [t]
        with pytest.raises(errors.TableExists):
            s.create_table('t', pa.schema([('x', pa.int64())]))

        assert s.tables() == [t]
        assert s.create_table('t', pa.schema([('x', pa.int64())]), fail_if_exists=False) == t
        assert s.tables() == [t]
        assert s.create_table('t', pa.schema([('y', pa.int64())]), fail_if_exists=False) == t
        assert s.tables() == [t]
        assert s.create_table('t', pa.schema([('x', pa.int64())]), fail_if_exists=False) == t
        assert s.tables() == [t]


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
        actual = t.select(columns=['a', 'b']).read_all()
        assert actual.to_pydict() == {
            'a': [1110, 222, 3330],
            'b': [0.5, 1.5, 2.5]
        }

        actual = t.select(columns=['a', 'b'], predicate=(t['a'] < 1000), internal_row_id=True).read_all()
        column_index = actual.column_names.index('a')
        column_field = actual.field(column_index)
        new_data = pc.add(actual.column('a'), 2000)
        update_table = actual.set_column(column_index, column_field, new_data)

        t.update(update_table, columns=['a'])
        actual = t.select(columns=['a', 'b']).read_all()
        assert actual.to_pydict() == {
            'a': [1110, 2222, 3330],
            'b': [0.5, 1.5, 2.5]
        }

        actual = t.select(columns=['a', 'b'], predicate=(t['a'] != 2222), internal_row_id=True).read_all()
        column_index = actual.column_names.index('a')
        column_field = actual.field(column_index)
        new_data = pc.divide(actual.column('a'), 10)
        update_table = actual.set_column(column_index, column_field, new_data)

        t.update(update_table.to_batches()[0], columns=['a'])
        actual = t.select(columns=['a', 'b']).read_all()
        assert actual.to_pydict() == {
            'a': [111, 2222, 333],
            'b': [0.5, 1.5, 2.5]
        }


def test_select_with_multisplits(session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int32())
    ])

    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    data = data * 1000
    expected = pa.table(schema=columns, data=[data])

    config = QueryConfig()
    config.rows_per_split = 1000

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
        actual = t.select(columns=['a'], config=config).read_all()
        assert actual == expected


def test_types(session, clean_bucket_name):
    columns = pa.schema([
        ('tb', pa.bool_()),
        ('a1', pa.int8()),
        ('a2', pa.int16()),
        ('a4', pa.int64()),
        ('b', pa.float32()),
        ('s', pa.string()),
        ('d', pa.decimal128(7, 3)),
        ('bin', pa.binary()),
        ('date', pa.date32()),
        ('t0', pa.time32('s')),
        ('t3', pa.time32('ms')),
        ('t6', pa.time64('us')),
        ('t9', pa.time64('ns')),
        ('ts0', pa.timestamp('s')),
        ('ts3', pa.timestamp('ms')),
        ('ts6', pa.timestamp('us')),
        ('ts9', pa.timestamp('ns')),
    ])

    expected = pa.table(schema=columns, data=[
        [True, True, False],
        [1, 2, 4],
        [1999, 2000, 2001],
        [11122221, 222111122, 333333],
        [0.5, 1.5, 2.5],
        ["a", "v", "s"],
        [decimal.Decimal('110.52'), decimal.Decimal('231.15'), decimal.Decimal('3332.44')],
        [b"\x01\x02", b"\x01\x05", b"\x01\x07"],
        [dt.date(2024, 4, 10), dt.date(2024, 4, 11), dt.date(2024, 4, 12)],
        [dt.time(12, 34, 56), dt.time(12, 34, 57), dt.time(12, 34, 58)],
        [dt.time(12, 34, 56, 789000), dt.time(12, 34, 57, 789000), dt.time(12, 34, 58, 789000)],
        [dt.time(12, 34, 56, 789789), dt.time(12, 34, 57, 789789), dt.time(12, 34, 58, 789789)],
        [dt.time(12, 34, 56, 789789), dt.time(12, 34, 57, 789789), dt.time(12, 34, 58, 789789)],
        [dt.datetime(2024, 4, 10, 12, 34, 56), dt.datetime(2025, 4, 10, 12, 34, 56), dt.datetime(2026, 4, 10, 12, 34, 56)],
        [dt.datetime(2024, 4, 10, 12, 34, 56, 789000), dt.datetime(2025, 4, 10, 12, 34, 56, 789000), dt.datetime(2026, 4, 10, 12, 34, 56, 789000)],
        [dt.datetime(2024, 4, 10, 12, 34, 56, 789789), dt.datetime(2025, 4, 10, 12, 34, 56, 789789), dt.datetime(2026, 4, 10, 12, 34, 56, 789789)],
        [dt.datetime(2024, 4, 10, 12, 34, 56, 789789), dt.datetime(2025, 4, 10, 12, 34, 56, 789789), dt.datetime(2026, 4, 10, 12, 34, 56, 789789)],
    ])

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as table:
        def select(predicate):
            return table.select(predicate=predicate).read_all()

        assert select(None) == expected
        for t in [table, ibis._]:
            assert select(t['tb'] == False) == expected.filter(pc.field('tb') == False)  # noqa: E712
            assert select(t['a1'] == 2) == expected.filter(pc.field('a1') == 2)
            assert select(t['a2'] == 2000) == expected.filter(pc.field('a2') == 2000)
            assert select(t['a4'] == 222111122) == expected.filter(pc.field('a4') == 222111122)
            assert select(t['b'] == 1.5) == expected.filter(pc.field('b') == 1.5)
            assert select(t['s'] == "v") == expected.filter(pc.field('s') == "v")
            assert select(t['d'] == 231.15) == expected.filter(pc.field('d') == 231.15)
            assert select(t['bin'] == b"\x01\x02") == expected.filter(pc.field('bin') == b"\x01\x02")

            date_literal = dt.date(2024, 4, 10)
            assert select(t['date'] == date_literal) == expected.filter(pc.field('date') == date_literal)

            time_literal = dt.time(12, 34, 56)
            assert select(t['t0'] == time_literal) == expected.filter(pc.field('t0') == time_literal)

            time_literal = dt.time(12, 34, 56, 789000)
            assert select(t['t3'] == time_literal) == expected.filter(pc.field('t3') == time_literal)

            time_literal = dt.time(12, 34, 56, 789789)
            assert select(t['t6'] == time_literal) == expected.filter(pc.field('t6') == time_literal)

            time_literal = dt.time(12, 34, 56, 789789)
            assert select(t['t9'] == time_literal) == expected.filter(pc.field('t9') == time_literal)

            ts_literal = dt.datetime(2024, 4, 10, 12, 34, 56)
            assert select(t['ts0'] == ts_literal) == expected.filter(pc.field('ts0') == ts_literal)

            ts_literal = dt.datetime(2024, 4, 10, 12, 34, 56, 789000)
            assert select(t['ts3'] == ts_literal) == expected.filter(pc.field('ts3') == ts_literal)

            ts_literal = dt.datetime(2024, 4, 10, 12, 34, 56, 789789)
            assert select(t['ts6'] == ts_literal) == expected.filter(pc.field('ts6') == ts_literal)

            ts_literal = dt.datetime(2024, 4, 10, 12, 34, 56, 789789)
            assert select(t['ts9'] == ts_literal) == expected.filter(pc.field('ts9') == ts_literal)


def test_filters(session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int32()),
        ('b', pa.float64()),
        ('s', pa.utf8()),
    ])

    expected = pa.table(schema=columns, data=[
        [111, 222, 333, 444, 555],
        [0.5, 1.5, 2.5, 3.5, 4.5],
        ['a', 'bb', 'ccc', None, 'xyz'],
    ])

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as table:
        def select(predicate):
            return table.select(predicate=predicate).read_all()

        assert select(None) == expected
        assert select(True) == expected
        assert select(False) == pa.Table.from_batches([], schema=columns)

        for t in [table, ibis._]:

            select(t['a'].isin(list(range(100))))
            select(t['a'].isin(list(range(1000))))
            select(t['a'].isin(list(range(10000))))
            with pytest.raises(errors.TooLargeRequest):
                select(t['a'].isin(list(range(100000))))

            assert select(t['a'].between(222, 444)) == expected.filter((pc.field('a') >= 222) & (pc.field('a') <= 444))
            assert select((t['a'].between(222, 444)) & (t['b'] > 2.5)) == expected.filter((pc.field('a') >= 222) & (pc.field('a') <= 444) & (pc.field('b') > 2.5))

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

            assert select((t['a'] > 111) | (t['a'] < 333)) == expected.filter((pc.field('a') > 111) | (pc.field('a') < 333))
            assert select(((t['a'] > 111) | (t['a'] < 333)) & (t['b'] < 2.5)) == expected.filter(((pc.field('a') > 111) | (pc.field('a') < 333)) & (pc.field('b') < 2.5))
            with pytest.raises(NotImplementedError):
                assert select((t['a'] > 111) | (t['b'] > 0) | (t['s'] < 'ccc')) == expected.filter((pc.field('a') > 111) | (pc.field('b') > 0) | (pc.field('s') < 'ccc'))
            assert select((t['a'] > 111) | (t['a'] < 333) | (t['a'] == 777)) == expected.filter((pc.field('a') > 111) | (pc.field('a') < 333) | (pc.field('a') == 777))

            assert select(t['s'].isnull()) == expected.filter(pc.field('s').is_null())
            assert select((t['s'].isnull()) | (t['s'] == 'bb'))  == expected.filter((pc.field('s').is_null()) | (pc.field('s') == 'bb'))
            assert select((t['s'].isnull()) & (t['b'] == 3.5))  == expected.filter((pc.field('s').is_null()) & (pc.field('b') == 3.5))

            assert select(~t['s'].isnull()) == expected.filter(~pc.field('s').is_null())
            assert select(t['s'].contains('b')) == expected.filter(pc.field('s') == 'bb')
            assert select(t['s'].contains('y')) == expected.filter(pc.field('s') == 'xyz')

            assert select(t['a'].isin([555])) == expected.filter(pc.field('a').isin([555]))
            assert select(t['a'].isin([111, 222, 999])) == expected.filter(pc.field('a').isin([111, 222, 999]))
            assert select((t['a'] == 111) | t['a'].isin([333, 444]) | (t['a'] > 600)) == expected.filter((pc.field('a') == 111) | pc.field('a').isin([333, 444]) | (pc.field('a') > 600))

            with pytest.raises(NotImplementedError):
                select(t['a'].isin([]))


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
        assert rb.to_pylist() == [0, 1]
        actual = t.select().read_all()
        assert actual == expected

        table_batches = t.select()

        with NamedTemporaryFile() as parquet_file:
            log.info("Writing table into parquet file: '%s'", parquet_file.name)
            with closing(pq.ParquetWriter(parquet_file.name, table_batches.schema)) as parquet_writer:
                for batch in table_batches:
                    parquet_writer.write_batch(batch)

            assert expected == pq.read_table(parquet_file.name)


def test_errors(session, clean_bucket_name):
    with pytest.raises(errors.MissingSchema):
        with session.transaction() as tx:
            tx.bucket(clean_bucket_name).schema('s1')

    with pytest.raises(errors.MissingBucket):
        with session.transaction() as tx:
            tx.bucket("bla")

    with pytest.raises(errors.Conflict):
        with session.transaction() as tx:
            b = tx.bucket(clean_bucket_name)
            s = b.create_schema('s1')
            columns = pa.schema([
                ('a', pa.int16()),
                ('b', pa.float32()),
                ('s', pa.utf8()),
            ])
            s.create_table('t1', columns)
            s.drop()  # cannot drop schema without dropping its tables first


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
        with pytest.raises(errors.MissingSchema):
            tx.bucket(clean_bucket_name).schema('s')

        # assert that other transactions are isolated
        tx2.bucket(clean_bucket_name).schema('s')
        with pytest.raises(errors.MissingSchema):
            tx2.bucket(clean_bucket_name).schema('ss')

    # assert that new transactions see the updated schema name
    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        with pytest.raises(errors.MissingSchema):
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
        with pytest.raises(errors.MissingTable):
            s.table('t')
        t = s.table('t2')

        # assert that other transactions are isolated
        with pytest.raises(errors.MissingTable):
            tx2.bucket(clean_bucket_name).schema('s').table('t2')
        tx2.bucket(clean_bucket_name).schema('s').table('t')

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema('s')
        # assert that new transactions see the change
        with pytest.raises(errors.MissingTable):
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

        # assert that other transactions are isolated
        assert tx2.bucket(clean_bucket_name).schema('s').table('t').arrow_schema == columns

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema('s')
        t = s.table('t')
        # assert that new transactions see the change
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

        # assert that other transactions are isolated
        assert tx2.bucket(clean_bucket_name).schema('s').table('t').arrow_schema == columns

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema('s')
        t = s.table('t')
        # assert that new transactions see the change
        assert t.arrow_schema == new_schema
        t.drop()
        s.drop()


def test_rename_column(session, clean_bucket_name):
    columns = pa.schema([
            ('a', pa.int16()),
            ('b', pa.float32()),
            ('s', pa.utf8()),
        ])

    def prepare_rename_column(schema: pa.Schema, old_name: str, new_name: str) -> pa.Schema:
        field_idx = schema.get_field_index(old_name)
        column_to_rename = schema.field(field_idx)
        renamed_column = column_to_rename.with_name(new_name)
        return schema.set(field_idx, renamed_column)

    new_schema = prepare_rename_column(columns, 'a', 'aaa')

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

        # assert that other transactions are isolated
        assert tx2.bucket(clean_bucket_name).schema('s').table('t').arrow_schema == columns

    # assert that new transactions see the change
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema('s')
        t = s.table('t')

        assert t.arrow_schema == new_schema

    # simultaneos renames of the same column
    new_schema_tx1 = prepare_rename_column(new_schema, 'b', 'bb')
    new_schema_tx2 = prepare_rename_column(new_schema, 'b', 'bbb')
    with pytest.raises(errors.Conflict):
        with session.transaction() as tx1, session.transaction() as tx2:
            t1 = tx1.bucket(clean_bucket_name).schema('s').table('t')
            t2 = tx2.bucket(clean_bucket_name).schema('s').table('t')
            t1.rename_column('b', 'bb')
            with pytest.raises(HTTPError, match='409 Client Error: Conflict'):
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


def test_select_stop(session, clean_bucket_name):
    columns = pa.schema([
            ('a', pa.uint8()),
        ])

    rb = pa.record_batch(schema=columns, data=[
            list(range(256)),
    ])

    num_rows = 0
    with session.transaction() as tx:
        b = tx.bucket(clean_bucket_name)
        s = b.create_schema('s')
        t = s.create_table('t', columns)
        t.insert(rb)

    num_rows = 2**8

    ROWS_PER_GROUP = 2**16
    qc = QueryConfig(num_sub_splits=2, num_splits=4, num_row_groups_per_sub_split=1)
    with session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema('s').table('t')
        qc.data_endpoints = list(t.get_stats().endpoints) * 2

    # Duplicate the table until it is large enough to generate enough batches
    while num_rows < (qc.num_sub_splits * qc.num_splits) * ROWS_PER_GROUP:
        # We need two separate transactions to prevent an infinite loop that may happen
        # while appending and reading the same table using a single transaction.
        with session.transaction() as tx_read, session.transaction() as tx_write:
            t_read = tx_read.bucket(clean_bucket_name).schema('s').table('t')
            t_write = tx_write.bucket(clean_bucket_name).schema('s').table('t')
            for batch in t_read.select(['a'], config=qc):
                t_write.insert(batch)
        num_rows = num_rows * 2
        log.info("Num rows: %d", num_rows)

    # Validate the number of batches and the number of rows
    read_rows = 0
    read_batches = 0
    with session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema('s').table('t')
        for batch in t.select(['a'], config=qc):
            read_batches += 1
            read_rows += len(batch)
    assert read_rows == num_rows
    # If this assert triggers it just means that the test assumptions about how
    # the tabular server splits the batches is not true anymore and we need to
    # rewrite the test.
    assert read_batches == qc.num_splits * qc.num_sub_splits
    qc.query_id = str(random.randint(0, 2**32))
    log.info("query id is: %s", qc.query_id)

    def active_threads():
        log.debug("%s", [t.getName() for t in threading.enumerate() if t.is_alive()])
        return sum([1 if t.is_alive() and qc.query_id in t.getName() else 0 for t in threading.enumerate()])

    assert active_threads() == 0

    with session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema('s').table('t')
        batches = iter(t.select(['a'], config=qc))
        next(batches)
        log.info("Active threads: %d", active_threads())
        try:
            assert active_threads() > 0
        finally:
            # If we dont delete the iterator, the threads will hang in a
            # zombie state.
            del batches

    # Check that all threads were killed
    log.info("Active threads: %d", active_threads())

    # validate that all query threads were killed.
    assert active_threads() == 0


def test_catalog_select(session, clean_bucket_name):
    with session.transaction() as tx:
        bc = tx.catalog()
        assert bc.columns()
        rows = bc.select(['name']).read_all()
        assert len(rows) > 0, rows


@pytest.mark.flaky(retries=30, delay=1, only_on=[AssertionError])
def test_audit_log_select(session, clean_bucket_name):
    with session.transaction() as tx:
        a = tx.audit_log()
        assert a.columns()
        rows = a.select().read_all()
        assert len(rows) > 0, rows


def test_catalog_snapshots_select(session, clean_bucket_name):
    with session.transaction() as tx:
        snaps = tx.catalog_snapshots()
        assert snaps
        latest = snaps[-1]
        t = tx.catalog(latest)
        assert t.columns()
        rows = t.select().read_all()
        assert len(rows) > 0, rows

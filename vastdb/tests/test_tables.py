import datetime as dt
import decimal
import logging
import random
import threading
import time
from contextlib import closing
from tempfile import NamedTemporaryFile

import ibis
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from requests.exceptions import HTTPError

from vastdb import errors
from vastdb.session import Session
from vastdb.table import INTERNAL_ROW_ID, QueryConfig

from .util import assert_row_ids_ascending_on_first_insertion_to_table, prepare_data

log = logging.getLogger(__name__)


@pytest.fixture
def elysium_session(session: Session):
    with session.transaction() as tx:
        try:
            tx._rpc.features.check_elysium()
            return session
        except errors.NotSupportedVersion:
            pytest.skip("Skipped because this test requires version 5.3.5 with Elysium")


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
        rb = pa.record_batch(schema=columns_to_delete, data=[[0]])  # delete row 0
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


def test_multi_batch_table(session, clean_bucket_name):
    columns = pa.schema([pa.field('s', pa.utf8())])
    expected = pa.Table.from_batches([
        pa.record_batch(schema=columns, data=[['a']]),
        pa.record_batch(schema=columns, data=[['b']]),
        pa.record_batch(schema=columns, data=[['c']]),
    ])

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
        actual = t.select().read_all()
        assert actual == expected


def test_insert_empty(session, clean_bucket_name):
    columns = pa.schema([('a', pa.int8()), ('b', pa.float32())])
    data = [[None] * 5, [None] * 5]
    all_nulls = pa.table(schema=columns, data=data)
    no_columns = all_nulls.select([])
    no_rows = pa.table(schema=columns, data=[[] for _ in columns])

    with session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).create_schema('s').create_table('t', columns)
        t.insert(all_nulls)

        with pytest.raises(errors.NotImplemented):
            t.insert(no_columns)

        row_ids = t.insert(no_rows).to_pylist()
        assert row_ids == []


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


@pytest.mark.parametrize("num_tables,page_size", [(10, 3)])
def test_list_tables(session, clean_bucket_name, num_tables, page_size):
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s1')
        assert s.tables() == []
        assert s.tablenames() == []

        tables = [
            s.create_table(f't{i}', pa.schema([(f'x{i}', pa.int64())]))
            for i in range(num_tables)
        ]
        assert tables == s.tables()
        tablenames = [t.name for t in tables]
        assert s.tablenames() == tablenames

        assert s.tablenames(page_size=page_size) == tablenames


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

        # test update for not sorted rows:
        rb = pa.record_batch(schema=columns_to_update, data=[
            [2, 0],  # update rows 0,2
            [231, 235]
        ])
        t.update(rb)
        actual = t.select(columns=['a', 'b']).read_all()
        assert actual.to_pydict() == {
            'a': [235, 2222, 231],
            'b': [0.5, 1.5, 2.5]
        }

        # test delete for not sorted rows:
        rb = pa.record_batch(schema=pa.schema([(INTERNAL_ROW_ID, pa.uint64())]), data=[[2, 0]])
        t.delete(rb)
        actual = t.select(columns=['a', 'b']).read_all()
        assert actual.to_pydict() == {
            'a': [2222],
            'b': [1.5]
        }


def test_update_from_select(session, clean_bucket_name):
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
        for b in t.select(internal_row_id=True):
            t.update(b)
            t.update(pa.Table.from_batches([b]))

        actual = t.select().read_all()
        assert actual == expected


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


def test_select_with_limit(session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int32())
    ])

    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    data = data * 1000
    expected = pa.table(schema=columns, data=[data])
    limit_rows = 10

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
        start = time.time()
        actual = t.select(predicate=(t['a'] < 3), limit_rows=limit_rows).read_all()
        end = time.time()
        log.info(f"actual: {actual} elapsed time: {end - start}")
        assert len(actual) == limit_rows


def test_select_with_priority(session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int32())
    ])
    expected = pa.table(schema=columns, data=[range(100)])
    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
        config = QueryConfig()

        config.queue_priority = 0
        assert t.select(config=config).read_all() == expected

        config.queue_priority = 12345
        assert t.select(config=config).read_all() == expected

        config.queue_priority = -1
        with pytest.raises(errors.BadRequest):
            t.select(config=config).read_all()


def test_timezones(session, clean_bucket_name):
    columns_with_tz = pa.schema([
        ('ts0', pa.timestamp('s', tz='+00:00')),
        ('ts3', pa.timestamp('ms', tz='UTC')),
        ('ts6', pa.timestamp('us', tz='GMT')),
        ('ts9', pa.timestamp('ns', tz='Universal')),
    ])

    # currently timezone information is not stored
    columns_without_tz = pa.schema([
        ('ts0', pa.timestamp('s')),
        ('ts3', pa.timestamp('ms')),
        ('ts6', pa.timestamp('us')),
        ('ts9', pa.timestamp('ns')),
    ])

    data = [
        [dt.datetime(2024, 4, 10, 12, 34, 56), dt.datetime(2025, 4, 10, 12, 34, 56), dt.datetime(2026, 4, 10, 12, 34, 56)],
        [dt.datetime(2024, 4, 10, 12, 34, 56, 789000), dt.datetime(2025, 4, 10, 12, 34, 56, 789000), dt.datetime(2026, 4, 10, 12, 34, 56, 789000)],
        [dt.datetime(2024, 4, 10, 12, 34, 56, 789789), dt.datetime(2025, 4, 10, 12, 34, 56, 789789), dt.datetime(2026, 4, 10, 12, 34, 56, 789789)],
        [dt.datetime(2024, 4, 10, 12, 34, 56, 789789), dt.datetime(2025, 4, 10, 12, 34, 56, 789789), dt.datetime(2026, 4, 10, 12, 34, 56, 789789)],
    ]

    inserted = pa.table(schema=columns_with_tz, data=data)
    with prepare_data(session, clean_bucket_name, 's', 't', inserted) as table:
        try:
            table.tx._rpc.features.check_timezone()
            assert table.arrow_schema == columns_with_tz
            assert table.select().read_all() == pa.table(schema=columns_with_tz, data=data)
        except errors.NotSupportedVersion:
            assert table.arrow_schema == columns_without_tz
            assert table.select().read_all() == pa.table(schema=columns_without_tz, data=data)


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
            assert select(t['tb'] == True) == expected.filter(pc.field('tb') == True)  # noqa: E712
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


@pytest.mark.parametrize("arrow_type,internal_support", [
    # Types not supported by Vast.
    (pa.null(), False),
    (pa.dictionary(pa.int64(), pa.int64()), False),
    (pa.dense_union([pa.field('1', pa.int32()), pa.field('2', pa.int64())]), False),
    # Arrow.FixedSizeBinaryType is not supported by Ibis, but Vast supports it internally.
    (pa.binary(1), True)
])
def test_unsupported_types(session, clean_bucket_name, arrow_type, internal_support):
    """ Test that unsupported types cannot be used in table creation or modification."""
    unsupported_field = pa.field('u', arrow_type)
    schema_name = 's'
    table_name = 't'

    # Create the schema
    with session.transaction() as tx:
        tx.bucket(clean_bucket_name).create_schema(schema_name)

    # Creation of a table with unsupported types should fail
    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema(schema_name)
        with pytest.raises(errors.NotSupportedSchema):
            s.create_table(table_name, pa.schema([unsupported_field]))

    with session.transaction() as tx:
        tx.bucket(clean_bucket_name).schema(schema_name).create_table(table_name,
                                                                      pa.schema([pa.field('a', pa.int32())]))

    # Adding unsupported types to an existing table should fail
    with session.transaction() as tx:
        t = tx.bucket(clean_bucket_name).schema(schema_name).table(table_name)
        with pytest.raises(errors.NotSupportedSchema):
            t.add_column(pa.schema([unsupported_field]))

    if internal_support:
        # Using internal API to add unsupported types
        with session.transaction() as tx:
            tx._rpc.api.add_columns(clean_bucket_name, schema_name, table_name, pa.schema([unsupported_field]),
                                    txid=tx.txid)

        # Attempt to open a table with unsupported types should fail
        with session.transaction() as tx:
            s = tx.bucket(clean_bucket_name).schema(schema_name)
            with pytest.raises(errors.NotSupportedSchema):
                s.table(table_name)

        # Even though the table is with unsupported types, it should still be listed
        with session.transaction() as tx:
            s = tx.bucket(clean_bucket_name).schema(schema_name)
            assert [table_name] == s.tablenames()


def test_unsigned_filters(session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.uint8()),
        ('b', pa.uint16()),
        ('c', pa.uint32()),
        ('d', pa.uint64()),
    ])

    expected = pa.table(schema=columns, data=[
        [1, 2, 3],
        [11, 22, 33],
        [111, 222, 333],
        [1111, 2222, 3333],
    ])

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as table:
        def select(predicate):
            return table.select(predicate=predicate).read_all()

        assert select(True) == expected
        for t in [table, ibis._]:
            assert select(t['a'] > 2) == expected.filter(pc.field('a') > 2)
            assert select(t['b'] > 22) == expected.filter(pc.field('b') > 22)
            assert select(t['c'] > 222) == expected.filter(pc.field('c') > 222)
            assert select(t['d'] > 2222) == expected.filter(pc.field('d') > 2222)


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
            assert select((t['s'].isnull()) | (t['s'] == 'bb')) == expected.filter((pc.field('s').is_null()) | (pc.field('s') == 'bb'))
            assert select((t['s'].isnull()) & (t['b'] == 3.5)) == expected.filter((pc.field('s').is_null()) & (pc.field('b') == 3.5))

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


class NotReady(Exception):
    pass


@pytest.mark.flaky(retries=30, delay=1, only_on=[NotReady])
def test_audit_log_select(session, clean_bucket_name):
    with session.transaction() as tx:
        a = tx.audit_log()
        assert a.columns()
        rows = a.select().read_all()
        if len(rows) == 0:
            raise NotReady


@pytest.mark.flaky(retries=30, delay=1, only_on=[NotReady])
def test_catalog_snapshots_select(session, clean_bucket_name):
    with session.transaction() as tx:
        snaps = tx.catalog_snapshots()
        filtered_snaps = []
        for snap in snaps:
            log.info("Snapshot: %s", snap)
            if snap.name.startswith("vast-big-catalog-bucket/.snapshot/bc_table"):
                filtered_snaps.append(snap)
        if not filtered_snaps:
            raise NotReady
        latest = filtered_snaps[-1]
        log.info("Latest snapshot: %s", latest)
        t = tx.catalog(latest)
        assert t.columns()
        rows = t.select(limit_rows=10).read_all()
        if not rows:
            raise NotReady


def test_starts_with(session, clean_bucket_name):
    columns = pa.schema([
        ('s', pa.utf8()),
        ('i', pa.int16()),
    ])

    expected = pa.table(schema=columns, data=[
        ['a', 'ab', 'abc', None, 'abd', 'α', '', 'b'],
        [0, 1, 2, 3, 4, 5, 6, 7],
    ])

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as table:
        def select(prefix):
            res = table.select(predicate=table['s'].startswith(prefix)).read_all()
            return res.to_pydict()

        assert select('')['s'] == ['a', 'ab', 'abc', 'abd', 'α', '', 'b']
        assert select('a')['s'] == ['a', 'ab', 'abc', 'abd']
        assert select('b')['s'] == ['b']
        assert select('ab')['s'] == ['ab', 'abc', 'abd']
        assert select('abc')['s'] == ['abc']
        assert select('α')['s'] == ['α']

        res = table.select(predicate=(table['s'].startswith('ab') | (table['s'].isnull()))).read_all()
        assert res.to_pydict()['s'] == ['ab', 'abc', None, 'abd']

        res = table.select(predicate=(table['s'].startswith('ab') | (table['s'] == 'b'))).read_all()
        assert res.to_pydict()['s'] == ['ab', 'abc', 'abd', 'b']

        res = table.select(predicate=((table['s'] == 'b') | table['s'].startswith('ab'))).read_all()
        assert res.to_pydict()['s'] == ['ab', 'abc', 'abd', 'b']

        res = table.select(predicate=(table['s'].startswith('ab') & (table['s'] != 'abc'))).read_all()
        assert res.to_pydict()['s'] == ['ab', 'abd']

        res = table.select(predicate=((table['s'] != 'abc') & table['s'].startswith('ab'))).read_all()
        assert res.to_pydict()['s'] == ['ab', 'abd']

        res = table.select(predicate=((table['i'] > 3) & table['s'].startswith('ab'))).read_all()
        assert res.to_pydict() == {'i': [4], 's': ['abd']}

        res = table.select(predicate=(table['s'].startswith('ab')) & (table['i'] > 3)).read_all()
        assert res.to_pydict() == {'i': [4], 's': ['abd']}


def test_external_row_id(session, clean_bucket_name):
    columns = [
        ('vastdb_rowid', pa.int64()),
        ('x', pa.float32()),
        ('y', pa.utf8()),
    ]

    with session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema('s')

        t = s.create_table('t1', pa.schema(columns))
        assert not t.stats.is_external_rowid_alloc
        t.insert(pa.record_batch(schema=pa.schema(columns), data=[[0], [1.5], ['ABC']]))
        assert t.stats.is_external_rowid_alloc

        t = s.create_table('t2', pa.schema(columns))
        assert not t.stats.is_external_rowid_alloc
        t.insert(pa.record_batch(schema=pa.schema(columns[1:]), data=[[1.5], ['ABC']]))
        assert not t.stats.is_external_rowid_alloc


def test_multiple_contains_clauses(session, clean_bucket_name):
    columns = pa.schema([
        ('theint', pa.int32()),
        ('thestring', pa.string()),
        ('theotherstring', pa.string()),
    ])

    expected = pa.table(schema=columns, data=[
        [111, 222, 333, 444, 555],
        ['abc', 'efg', 'hij', 'klm', 'nop'],
        ['abcd', 'bcde', 'cdef', 'defg', 'efgh'],
    ])
    with (prepare_data(session, clean_bucket_name, 's', 't', expected) as t):
        failed_preds = [
            lambda t: (t["thestring"].contains("b") | t["theotherstring"].contains("a")),
            lambda t: (~t["thestring"].contains("a")),
        ]

        assert t.select(predicate=t["thestring"].contains("b")).read_all() == expected.filter(pc.match_substring(expected["thestring"], "b"))
        assert (t.select(predicate=(t["thestring"].contains("b")) & (t["theotherstring"].startswith("a"))).read_all() ==
                expected.filter(pc.and_(
                    pc.match_substring(expected["thestring"], "b"),
                    pc.starts_with(expected["thestring"], "a")
                    )))
        assert (t.select(predicate=(t["thestring"].contains("b") & (t["thestring"].contains("y")))).read_all() ==
        t.select(predicate=t["thestring"].contains("y") & (t["thestring"].contains("b"))).read_all())

        assert (t.select(predicate=(t["thestring"].contains("o") & (t["theint"] > 500))).read_all() ==
        pc.filter(expected, pc.and_(
            pc.match_substring(expected["thestring"], "o"),
            pc.greater(expected["theint"], 500)
        )))
        assert (t.select(predicate=((t["thestring"].contains("bc")) | (t["thestring"].contains("kl")) |
                                   (t["thestring"] == "hi") | (t["thestring"].startswith("e")))).read_all() ==
                expected.filter(
                    pc.or_(pc.or_(pc.match_substring(expected["thestring"], "bc"),
                                  pc.match_substring(expected["thestring"], "kl")),
                            pc.or_(pc.equal(expected["thestring"], "hi"),
                                   pc.starts_with(expected["thestring"], "e"))
                )))
        assert (t.select(predicate=((t["thestring"].contains("abc")) | (t["thestring"].contains("xyz")) |
                                    (t["thestring"].startswith("z")) | (t["thestring"].isnull()))).read_all() ==
                pc.filter(expected,
                    pc.or_(pc.or_(pc.match_substring(expected["thestring"], "abc"),
                                  pc.match_substring(expected["thestring"], "xyz")),
                           pc.or_(pc.starts_with(expected["thestring"], "z"),
                                  pc.is_null(expected["thestring"]))
                    )))
        assert (t.select(predicate=((t["thestring"].contains("k")) & (t["theotherstring"].contains("h")) &
                                    (t["theint"] > 500))).read_all() ==
                         pc.filter(expected, pc.and_(
                             pc.and_(pc.match_substring(expected["thestring"], "k"),
                             pc.match_substring(expected["theotherstring"], "h")),
                             pc.greater(expected["theint"], 500)
                         )))
        for pred in failed_preds:
            with pytest.raises(NotImplementedError):
                t.select(predicate=pred(t)).read_all()


def test_tables_elysium(elysium_session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int8()),
        ('b', pa.int32()),
        ('c', pa.int16()),
    ])
    expected = pa.table(schema=columns, data=[
        [1, 2, 3],
        [111111, 222222, 333333],
        [111, 222, 333],
    ])
    sorting = [2, 1]
    with prepare_data(elysium_session, clean_bucket_name, 's', 't', expected, sorting_key=sorting) as t:
        sorted_columns = t.sorted_columns()
        assert sorted_columns[0].name == 'c'
        assert sorted_columns[1].name == 'b'


# Fails because of a known issue: ORION-240102
# def test_enable_elysium(session, clean_bucket_name):
#     columns = pa.schema([
#         ('a', pa.int8()),
#         ('b', pa.int32()),
#         ('c', pa.int16()),
#     ])
#     expected = pa.table(schema=columns, data=[
#         [1,2,3],
#         [111111,222222,333333],
#         [111, 222, 333],
#     ])
#     sorting = [2, 1]
#     with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
#         sorted_columns = t.sorted_columns()
#         assert len(sorted_columns) == 0
#         t.add_sorting_key(sorting)
#         time.sleep(10)
#         sorted_columns = t.sorted_columns()
#         assert len(sorted_columns) == 2
#         assert sorted_columns[0].name == 'c'
#         assert sorted_columns[1].name == 'b'


def test_elysium_tx(elysium_session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int8()),
        ('b', pa.int32()),
        ('c', pa.int16()),
    ])
    arrow_table = pa.table(schema=columns, data=[
        [1, 2, 3],
        [111111, 222222, 333333],
        [111, 222, 333],
    ])
    sorting = [2, 1]
    schema_name = 's'
    table_name = 't'
    with elysium_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(table_name, arrow_table.schema)
        row_ids_array = t.insert(arrow_table)
        row_ids = row_ids_array.to_pylist()
        assert_row_ids_ascending_on_first_insertion_to_table(row_ids, arrow_table.num_rows, t.sorted_table)
        sorted_columns = t.sorted_columns()
        assert len(sorted_columns) == 0
        t.add_sorting_key(sorting)

    with elysium_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema(schema_name)
        t = s.table(table_name)
        sorted_columns = t.sorted_columns()
        assert len(sorted_columns) == 2
        assert sorted_columns[0].name == 'c'
        assert sorted_columns[1].name == 'b'
        t.drop()
        s.drop()


def test_elysium_double_enable(elysium_session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int8()),
        ('b', pa.int32()),
        ('c', pa.int16()),
    ])
    expected = pa.table(schema=columns, data=[
        [1, 2, 3],
        [111111, 222222, 333333],
        [111, 222, 333],
    ])
    sorting = [2, 1]
    with pytest.raises(errors.BadRequest):
        with prepare_data(elysium_session, clean_bucket_name, 's', 't', expected, sorting_key=sorting) as t:
            sorted_columns = t.sorted_columns()
            assert sorted_columns[0].name == 'c'
            assert sorted_columns[1].name == 'b'
            t.add_sorting_key(sorting)


def test_elysium_update_table_tx(elysium_session: Session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int64()),
        ('b', pa.float32()),
        ('s', pa.utf8()),
    ])
    arrow_table = pa.table(schema=columns, data=[
        [111, 222, 333],
        [0.5, 1.5, 2.5],
        ['a', 'bb', 'ccc'],
    ])
    sorting = [2, 1]
    schema_name = 's'
    table_name = 't'
    with elysium_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(table_name, arrow_table.schema, sorting_key=sorting)
        row_ids_array = t.insert(arrow_table)
        row_ids = row_ids_array.to_pylist()
        assert_row_ids_ascending_on_first_insertion_to_table(row_ids, arrow_table.num_rows, t.sorted_table)
        sorted_columns = t.sorted_columns()
        assert sorted_columns[0].name == 's'
        assert sorted_columns[1].name == 'b'

    with elysium_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema(schema_name)
        t = s.table(table_name)
        sorted_columns = t.sorted_columns()
        assert sorted_columns[0].name == 's'
        assert sorted_columns[1].name == 'b'

        actual = t.select(columns=['a', 'b'], predicate=(t['a'] == 222), internal_row_id=True).read_all()
        column_index = actual.column_names.index('a')
        column_field = actual.field(column_index)
        new_data = pc.add(actual.column('a'), 2000)
        update_table = actual.set_column(column_index, column_field, new_data)

        t.update(update_table, columns=['a'])
        actual = t.select(columns=['a', 'b']).read_all()
        assert actual.to_pydict() == {
            'a': [111, 2222, 333],
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
            'a': [11, 2222, 33],
            'b': [0.5, 1.5, 2.5]
        }

        actual = t.select(columns=['a', 'b'], predicate=(t['a'] < 222), internal_row_id=True).read_all()
        column_index = actual.column_names.index('a')
        column_field = actual.field(column_index)
        new_data = pc.divide(actual.column('a'), 10)
        delete_rows = actual.set_column(column_index, column_field, new_data)

        t.delete(delete_rows)
        actual = t.select(columns=['a', 'b']).read_all()
        assert actual.to_pydict() == {
            'a': [2222],
            'b': [1.5]
        }


def test_elysium_splits(elysium_session, clean_bucket_name):
    columns = pa.schema([
        ('a', pa.int32())
    ])

    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    data = data * 10000
    arrow_table = pa.table(schema=columns, data=[data])

    config = QueryConfig()
    config.rows_per_split = 1000

    sorting = [0]
    schema_name = 's'
    table_name = 't'

    with elysium_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).create_schema(schema_name)
        t = s.create_table(table_name, arrow_table.schema, sorting_key=sorting)
        row_ids_array = t.insert(arrow_table)
        row_ids = row_ids_array.to_pylist()
        assert_row_ids_ascending_on_first_insertion_to_table(row_ids, arrow_table.num_rows, t.sorted_table)
        sorted_columns = t.sorted_columns()
        assert sorted_columns[0].name == 'a'

    time.sleep(300)
    with elysium_session.transaction() as tx:
        s = tx.bucket(clean_bucket_name).schema(schema_name)
        t = s.table(table_name)
        sorted_columns = t.sorted_columns()
        assert sorted_columns[0].name == 'a'

        actual = t.select(columns=['a'], predicate=(t['a'] == 1), config=config).read_all()
        assert len(actual) == 10000


def to_df(table: pa.Table) -> pd.DataFrame:
    return table.to_pandas().sort_values(by='a').reset_index(drop=True)


def test_select_splits_sanity(session, clean_bucket_name, check):
    columns = pa.schema([
        ('a', pa.int64()),
        ('b', pa.float32()),
        ('c', pa.utf8()),
    ])

    length = 1000000

    expected = pa.table(schema=columns, data=[
        list(range(length)),
        [i * 0.001 for i in range(length)],
        [f'a{i}' for i in range(length)],
    ])

    query_config = QueryConfig(
        num_sub_splits=1,
        num_splits=4,
        limit_rows_per_sub_split=2500,
        num_row_groups_per_sub_split=1,
    )

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
        splits_readers = t.select_splits(
            columns=['a', 'b', 'c'], config=query_config)
        splits_reader_tables = [splits_reader.read_all().combine_chunks()
                            for splits_reader in splits_readers]

        for splits_reader_table in splits_reader_tables:
            check.greater(splits_reader_table.num_rows, 0, "if splits readers are empty test is not interesting")

        actual = pa.concat_tables(splits_reader_tables).combine_chunks()

        check.is_true(to_df(actual).equals(to_df(expected)))

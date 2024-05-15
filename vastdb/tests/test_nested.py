import functools
import itertools
import operator

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from .util import prepare_data


def test_nested_select(session, clean_bucket_name):
    columns = pa.schema([
        ('l', pa.list_(pa.int8())),
        ('m', pa.map_(pa.utf8(), pa.float64())),
        ('s', pa.struct([('x', pa.int16()), ('y', pa.int32())])),
    ])
    expected = pa.table(schema=columns, data=[
        [[1], [], [2, 3], None],
        [None, {'a': 2.5}, {'b': 0.25, 'c': 0.025}, {}],
        [{'x': 1, 'y': None}, None, {'x': 2, 'y': 3}, {'x': None, 'y': 4}],
    ])

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
        actual = pa.Table.from_batches(t.select())
        assert actual == expected

        names = [f.name for f in columns]
        for n in range(len(names) + 1):
            for cols in itertools.permutations(names, n):
                actual = pa.Table.from_batches(t.select(columns=cols))
                assert actual == expected.select(cols)


def test_nested_filter(session, clean_bucket_name):
    columns = pa.schema([
        ('x', pa.int64()),
        ('l', pa.list_(pa.int8())),
        ('y', pa.int64()),
        ('m', pa.map_(pa.utf8(), pa.float64())),
        ('z', pa.int64()),
        ('s', pa.struct([('x', pa.int16()), ('y', pa.int32())])),
        ('w', pa.int64()),
    ])
    expected = pa.table(schema=columns, data=[
        [1, 2, 3, None],
        [[1], [], [2, 3], None],
        [1, 2, None, 3],
        [None, {'a': 2.5}, {'b': 0.25, 'c': 0.025}, {}],
        [1, None, 2, 3],
        [{'x': 1, 'y': None}, None, {'x': 2, 'y': 3}, {'x': None, 'y': 4}],
        [None, 1, 2, 3],
    ])

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
        actual = pa.Table.from_batches(t.select())
        assert actual == expected

        names = list('xyzw')
        for n in range(1, len(names) + 1):
            for cols in itertools.permutations(names, n):
                ibis_predicate = functools.reduce(
                    operator.and_,
                    (t[col] > 2 for col in cols))
                actual = pa.Table.from_batches(t.select(predicate=ibis_predicate), t.arrow_schema)

                arrow_predicate = functools.reduce(
                    operator.and_,
                    (pc.field(col) > 2 for col in cols))
                assert actual == expected.filter(arrow_predicate)


def test_nested_unsupported_filter(session, clean_bucket_name):
    columns = pa.schema([
        ('x', pa.int64()),
        ('l', pa.list_(pa.int8())),
        ('y', pa.int64()),
        ('m', pa.map_(pa.utf8(), pa.float64())),
        ('z', pa.int64()),
        ('s', pa.struct([('x', pa.int16()), ('y', pa.int32())])),
        ('w', pa.int64()),
    ])
    expected = pa.table(schema=columns, data=[
        [1, 2, 3, None],
        [[1], [], [2, 3], None],
        [1, 2, None, 3],
        [None, {'a': 2.5}, {'b': 0.25, 'c': 0.025}, {}],
        [1, None, 2, 3],
        [{'x': 1, 'y': None}, None, {'x': 2, 'y': 3}, {'x': None, 'y': 4}],
        [None, 1, 2, 3],
    ])

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:

        with pytest.raises(NotImplementedError):
            list(t.select(predicate=(t['l'].isnull())))

        with pytest.raises(NotImplementedError):
            list(t.select(predicate=(t['m'].isnull())))

        with pytest.raises(NotImplementedError):
            list(t.select(predicate=(t['s'].isnull())))

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
        ('fl', pa.list_(pa.field(name='item', type=pa.int64(), nullable=False), 2)),
        ('lfl', pa.list_(pa.list_(pa.field(name='item', type=pa.int64(), nullable=False), 2))),
        ('m', pa.map_(pa.utf8(), pa.float64())),
        ('s', pa.struct([('x', pa.int16()), ('y', pa.int32())])),
    ])
    expected = pa.table(schema=columns, data=[
        [[1], [], [2, 3], None],
        [[1, 2], None, [3, 4], None],
        [[[1, 2], [3, 4], [4, 5]], None, [[5, 6], [7, 8]], [None, None]],
        [None, {'a': 2.5}, {'b': 0.25, 'c': 0.025}, {}],
        [{'x': 1, 'y': None}, None, {'x': 2, 'y': 3}, {'x': None, 'y': 4}],
    ])

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
        actual = t.select().read_all()
        assert actual == expected

        names = [f.name for f in columns]
        for n in range(len(names) + 1):
            for cols in itertools.permutations(names, n):
                actual = t.select(columns=cols).read_all()
                assert actual == expected.select(cols)


def test_nested_filter(session, clean_bucket_name):
    columns = pa.schema([
        ('x', pa.int64()),
        ('l', pa.list_(pa.int8())),
        ('fl', pa.list_(pa.field(name='item', type=pa.int64(), nullable=False), 2)),
        ('y', pa.int64()),
        ('m', pa.map_(pa.utf8(), pa.float64())),
        ('z', pa.int64()),
        ('s', pa.struct([('x', pa.int16()), ('y', pa.int32())])),
        ('w', pa.int64()),
    ])
    expected = pa.table(schema=columns, data=[
        [1, 2, 3, None],
        [[1], [], [2, 3], None],
        [[1, 2], None, [3, 4], None],
        [1, 2, None, 3],
        [None, {'a': 2.5}, {'b': 0.25, 'c': 0.025}, {}],
        [1, None, 2, 3],
        [{'x': 1, 'y': None}, None, {'x': 2, 'y': 3}, {'x': None, 'y': 4}],
        [None, 1, 2, 3],
    ])

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:
        actual = t.select().read_all()
        assert actual == expected

        names = list('xyzw')
        for n in range(1, len(names) + 1):
            for cols in itertools.permutations(names, n):
                ibis_predicate = functools.reduce(
                    operator.and_,
                    (t[col] > 2 for col in cols))
                actual = t.select(predicate=ibis_predicate).read_all()

                arrow_predicate = functools.reduce(
                    operator.and_,
                    (pc.field(col) > 2 for col in cols))
                assert actual == expected.filter(arrow_predicate)


def test_nested_unsupported_filter(session, clean_bucket_name):
    columns = pa.schema([
        ('l', pa.list_(pa.int8())),
        ('fl', pa.list_(pa.field(name='item', type=pa.int64(), nullable=False), 2)),
        ('m', pa.map_(pa.utf8(), pa.float64())),
        ('s', pa.struct([('x', pa.int16()), ('y', pa.int32())])),
    ])
    expected = pa.table(schema=columns, data=[
        [[1], [], [2, 3], None],
        [[1, 2], None, [3, 4], None],
        [None, {'a': 2.5}, {'b': 0.25, 'c': 0.025}, {}],
        [{'x': 1, 'y': None}, None, {'x': 2, 'y': 3}, {'x': None, 'y': 4}],
    ])

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:

        with pytest.raises(NotImplementedError):
            list(t.select(predicate=(t['l'].isnull())))

        with pytest.raises(NotImplementedError):
            list(t.select(predicate=(t['fl'].isnull())))

        with pytest.raises(NotImplementedError):
            list(t.select(predicate=(t['m'].isnull())))

        with pytest.raises(NotImplementedError):
            list(t.select(predicate=(t['s'].isnull())))


def test_nested_subfields_predicate_pushdown(session, clean_bucket_name):
    columns = pa.schema([
        ('x', pa.int64()),
        ('l', pa.list_(pa.int8())),
        ('fl', pa.list_(pa.field(name='item', type=pa.int64(), nullable=False), 2)),
        ('y', pa.int64()),
        ('m', pa.map_(pa.utf8(), pa.float64())),
        ('z', pa.int64()),
        ('s', pa.struct([
            ('x', pa.int16()),
            ('y', pa.int32()),
            ('q', pa.struct([
                ('q1', pa.utf8()),
                ('q2', pa.float32())
            ]))
        ])),
        ('w', pa.int64()),
    ])
    expected = pa.table(schema=columns, data=[
        [1, 2, 3, None],
        [[1], [], [2, 3], None],
        [[1, 2], None, [3, 4], None],
        [1, 2, None, 3],
        [None, {'a': 2.5}, {'b': 0.25, 'c': 0.025}, {}],
        [1, None, 2, 3],
        [
            {'x': 1, 'y': None, 'q': {'q1': 'AAA', 'q2': 1.0}},
            None,
            {'x': 2, 'y': 3, 'q': {'q1': 'B', 'q2': 2.0}},
            {'x': None, 'y': 4, 'q': {'q1': 'CC', 'q2': 2.0}}],
        [None, 1, 2, 3],
    ])

    with prepare_data(session, clean_bucket_name, 's', 't', expected) as t:

        assert t.select(predicate=(t['s']['x'] == 1)).read_all() == expected.take([0])
        assert t.select(predicate=(t['s']['y'].isnull())).read_all() == expected.take([0, 1])
        assert t.select(predicate=(t['s']['q']['q1'] == 'AAA')).read_all() == expected.take([0])
        assert t.select(predicate=(t['s']['q']['q1'] < 'B')).read_all() == expected.take([0])
        assert t.select(predicate=(t['s']['q']['q1'] <= 'B')).read_all() == expected.take([0, 2])
        assert t.select(predicate=(t['s']['q']['q2'] == 1.0)).read_all() == expected.take([0])

        assert t.select(predicate=(t['s']['q']['q1'].isnull())).read_all() == expected.take([1])
        assert t.select(predicate=(t['s']['q']['q2'].isnull())).read_all() == expected.take([1])

        assert t.select(predicate=(t['s']['x'] == 2)).read_all() == expected.take([2])
        assert t.select(predicate=(t['s']['y'] == 3)).read_all() == expected.take([2])
        assert t.select(predicate=(t['s']['q']['q1'] == 'B')).read_all() == expected.take([2])
        assert t.select(predicate=(t['s']['q']['q2'] == 2.0)).read_all() == expected.take([2, 3])

        assert t.select(predicate=(t['s']['x'].isnull())).read_all() == expected.take([1, 3])
        assert t.select(predicate=(t['s']['y'] == 4)).read_all() == expected.take([3])
        assert t.select(predicate=(t['s']['q']['q1'] == 'CC')).read_all() == expected.take([3])
        assert t.select(predicate=(t['s']['q']['q1'] > 'B')).read_all() == expected.take([3])
        assert t.select(predicate=(t['s']['q']['q1'] >= 'B')).read_all() == expected.take([2, 3])

        assert t.select(predicate=(t['s']['x'] == 1) | (t['s']['x'] == 2)).read_all() == expected.take([0, 2])
        assert t.select(predicate=(t['s']['x'].isnull()) & (t['s']['y'].isnull())).read_all() == expected.take([1])

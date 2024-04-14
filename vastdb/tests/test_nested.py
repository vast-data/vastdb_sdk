import itertools

import pyarrow as pa

from .util import prepare_data


def test_nested(session, clean_bucket_name):
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
